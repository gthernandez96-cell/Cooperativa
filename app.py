from utils.db import init_db
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, Response, send_file, g
from flask_wtf.csrf import CSRFProtect
from dotenv import load_dotenv
import sqlite3
import os
import json
import csv
import math
import logging
import uuid
import hashlib
from io import StringIO, BytesIO
from datetime import datetime, date, timedelta, UTC
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

try:
    import psycopg  # type: ignore[import-not-found]
    from psycopg.rows import dict_row  # type: ignore[import-not-found]
except Exception:
    psycopg = None
    dict_row = None

# Cargar variables de entorno desde .env
load_dotenv()

from config import (
    DB, DB_BACKEND, DATABASE_URL,
    SOCIOS_UPLOAD_DIR, COOPERATIVA_UPLOAD_DIR, DEFAULT_COOPERATIVA_NOMBRE,
    CONFIG_LABELS, TRANSACCION_LABELS, TRANSACCIONES_POSITIVAS,
    REQUIRED_CONFIGURACIONES, SYSTEM_SETTINGS_DEFAULTS,
    AHORRO_SETTINGS_DEFAULTS, PRESTAMO_SETTINGS_DEFAULTS,
    DEFAULT_PRESTAMO_CATEGORIAS, ALLOWED_IMAGE_EXTENSIONS,
)
from utils.images import (
    allowed_image as allowed_socio_image,
    allowed_image as allowed_system_image,
    procesar_foto_socio,
    procesar_foto_cooperativa,
)
from utils.nombres import (
    descomponer_nombre,
    construir_nombre_completo,
    construir_apellido_completo,
    preparar_datos_socio,
    resumen_beneficiarios,
)
from utils.db import (
    get_db, db_execute, db_fetchone, db_fetchall,
    get_db_connection
)

app = Flask(__name__)

from blueprints.auth import bp as auth_bp
from blueprints.main import bp as main_bp
from blueprints.usuarios import bp as usuarios_bp
from blueprints.socios import bp as socios_bp
from blueprints.ahorro import bp as ahorro_bp
from blueprints.prestamos import bp as prestamos_bp
from blueprints.configuraciones import bp as configuraciones_bp

app.register_blueprint(auth_bp)
app.register_blueprint(main_bp)
app.register_blueprint(usuarios_bp)
app.register_blueprint(socios_bp)
app.register_blueprint(ahorro_bp)
app.register_blueprint(prestamos_bp)
app.register_blueprint(configuraciones_bp)

from utils.helpers import tipo_transaccion_label, es_transaccion_positiva
app.jinja_env.filters['tipo_transaccion'] = tipo_transaccion_label
app.jinja_env.filters['es_transaccion_positiva'] = es_transaccion_positiva

# SECRET_KEY debe estar en .env — si no hay valor se lanza excepción en producción
_secret = os.environ.get('SECRET_KEY')
if not _secret:
    raise RuntimeError(
        'SECRET_KEY no está definida. Crea un archivo .env con SECRET_KEY=<valor_seguro>'
    )
app.secret_key = _secret

# Protección CSRF global (Flask-WTF)
csrf = CSRFProtect(app)

# Logging estructurado para observabilidad básica.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger('cooperativa')




@app.before_request
def _set_request_context():
    g.request_id = request.headers.get('X-Request-ID') or str(uuid.uuid4())
    g.request_started_at = datetime.now(UTC)


@app.after_request
def _log_request(response):
    started = getattr(g, 'request_started_at', None)
    duration_ms = None
    if started:
        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
    logger.info(
        'event=request method=%s path=%s status=%s user=%s role=%s request_id=%s duration_ms=%s ip=%s',
        request.method,
        request.path,
        response.status_code,
        session.get('username', 'anon'),
        session.get('user_role', 'anon'),
        getattr(g, 'request_id', '-'),
        duration_ms,
        request.headers.get('X-Forwarded-For', request.remote_addr),
    )
    response.headers['X-Request-ID'] = getattr(g, 'request_id', '-')
    return response


def usuario_tiene_permiso(conn, user_id, user_role, permiso):
    if not user_id:
        return False
    if user_role == 'Administrador':
        return True

    role_defaults = ROLE_PERMISSION_DEFAULTS.get(user_role or '', set())
    if '*' in role_defaults or permiso in role_defaults:
        return True

    row = conn.execute(
        '''
        SELECT 1
        FROM usuarios u
        JOIN roles r ON r.id = u.rol_id
        JOIN rol_permisos rp ON rp.rol_id = r.id
        JOIN permisos p ON p.id = rp.permiso_id
        WHERE u.id = ? AND p.codigo = ?
        LIMIT 1
        ''',
        (user_id, permiso),
    ).fetchone()
    return row is not None


def permission_required(permiso):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if 'user_id' not in session:
                flash('Debe iniciar sesión', 'danger')
                return redirect(url_for('auth.login'))
            conn = get_db()
            try:
                ok = usuario_tiene_permiso(
                    conn,
                    session.get('user_id'),
                    session.get('user_role'),
                    permiso,
                )
            finally:
                conn.close()
            if not ok:
                flash('No tiene permisos para ejecutar esta acción.', 'danger')
                return redirect(url_for('main.index'))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def validate_idempotency(conn, scope):
    key = request.headers.get('X-Idempotency-Key') or (request.get_json(silent=True) or {}).get('idempotency_key')
    if not key:
        return True
    composed = f"{scope}:{session.get('user_id', 'anon')}:{key}"
    digest = hashlib.sha256(composed.encode('utf-8')).hexdigest()
    row = conn.execute('SELECT 1 FROM idempotency_keys WHERE key_hash=?', (digest,)).fetchone()
    if row:
        return False
    conn.execute(
        'INSERT INTO idempotency_keys (key_hash, scope, user_id, created_at) VALUES (?, ?, ?, ?)',
        (digest, scope, session.get('user_id'), datetime.now().isoformat()),
    )
    return True


def login_required(role=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if 'user_id' not in session:
                flash('Debe iniciar sesión', 'danger')
                return redirect(url_for('auth.login'))

            if role:
                user_role = session.get('user_role')
                allowed_roles = role
                if isinstance(role, str):
                    allowed_roles = [role]
                if user_role != 'Administrador' and user_role not in allowed_roles:
                    flash('Acceso denegado para su rol', 'danger')
                    return redirect(url_for('main.index'))

            return fn(*args, **kwargs)
        return wrapper
    return decorator

# ── DB helpers ────────────────────────────────────────────────────────────────














def db_insert_ignore(conn, table, columns, values, conflict_columns):
    placeholders = ', '.join(['?'] * len(columns))
    cols_sql = ', '.join(columns)
    conflict_sql = ', '.join(conflict_columns)
    db_execute(
        conn,
        f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) ON CONFLICT ({conflict_sql}) DO NOTHING",
        values,
    )





def get_config(tipo):
    """Obtiene el valor de una configuración por tipo"""
    conn = get_db()
    config = db_fetchone(conn, "SELECT tasa_interes FROM configuraciones WHERE tipo=?", [tipo])
    conn.close()
    return config['tasa_interes'] if config else 0


def ensure_system_settings(conn):
    hoy = date.today().isoformat()
    for clave, valor in SYSTEM_SETTINGS_DEFAULTS.items():
        db_insert_ignore(
            conn,
            'ajustes_sistema',
            ('clave', 'valor', 'fecha_actualizacion'),
            (clave, valor, hoy),
            ('clave',),
        )


def ensure_module_settings(conn):
    hoy = date.today().isoformat()
    for defaults in (AHORRO_SETTINGS_DEFAULTS, PRESTAMO_SETTINGS_DEFAULTS):
        for clave, valor in defaults.items():
            db_insert_ignore(
                conn,
                'ajustes_sistema',
                ('clave', 'valor', 'fecha_actualizacion'),
                (clave, valor, hoy),
                ('clave',),
            )


def ensure_default_prestamo_categories(conn):
    hoy = date.today().isoformat()
    for nombre, descripcion in DEFAULT_PRESTAMO_CATEGORIAS:
        db_insert_ignore(
            conn,
            'prestamo_categorias',
            ('nombre', 'descripcion', 'estado', 'fecha_actualizacion'),
            (nombre, descripcion, 'activo', hoy),
            ('nombre',),
        )


def ensure_permissions_catalog(conn):
    permisos_base = [
        ('socios.ver', 'Ver socios'),
        ('socios.editar', 'Editar socios'),
        ('socios.estado', 'Activar/Inactivar socios'),
        ('ahorro.ver', 'Ver modulo de ahorro'),
        ('ahorro.transaccion', 'Registrar transacciones de ahorro'),
        ('ahorro.masivo', 'Procesar operaciones masivas de ahorro'),
        ('prestamos.ver', 'Ver modulo de prestamos'),
        ('prestamos.aprobar', 'Aprobar prestamos'),
        ('prestamos.pagar', 'Registrar pagos de prestamos'),
        ('prestamos.masivo', 'Procesar pagos masivos de prestamos'),
        ('config.ahorro', 'Configurar modulo ahorro'),
        ('config.prestamos', 'Configurar modulo prestamos'),
        ('cobranza.gestion', 'Registrar acciones de cobranza'),
        ('cobranza.recordatorios', 'Enviar recordatorios de cobranza'),
        ('cobranza.legal', 'Marcar casos para revision legal'),
        ('reportes.ver', 'Ver reportes'),
    ]
    for codigo, nombre in permisos_base:
        db_insert_ignore(conn, 'permisos', ('codigo', 'nombre'), (codigo, nombre), ('codigo',))

    for rol, perms in ROLE_PERMISSION_DEFAULTS.items():
        rol_row = db_fetchone(conn, "SELECT id FROM roles WHERE nombre=?", (rol,))
        if not rol_row:
            continue
        if '*' in perms:
            continue
        for perm in perms:
            perm_row = db_fetchone(conn, "SELECT id FROM permisos WHERE codigo=?", (perm,))
            if not perm_row:
                continue
            db_insert_ignore(
                conn,
                'rol_permisos',
                ('rol_id', 'permiso_id'),
                (rol_row['id'], perm_row['id']),
                ('rol_id', 'permiso_id'),
            )


def get_system_setting(conn, clave, default=None):
    row = db_fetchone(conn, "SELECT valor FROM ajustes_sistema WHERE clave=?", (clave,))
    if row and row['valor'] is not None:
        return row['valor']
    if default is not None:
        return default
    return SYSTEM_SETTINGS_DEFAULTS.get(clave)


def set_system_setting(conn, clave, valor, usuario=None):
    db_execute(
        conn,
        """INSERT INTO ajustes_sistema (clave, valor, fecha_actualizacion, usuario_actualizacion)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(clave) DO UPDATE SET
               valor=excluded.valor,
               fecha_actualizacion=excluded.fecha_actualizacion,
               usuario_actualizacion=excluded.usuario_actualizacion""",
        (clave, valor, date.today().isoformat(), usuario)
    )


def obtener_marca_cooperativa():
    conn = get_db()
    try:
        nombre = get_system_setting(conn, 'cooperativa_nombre', DEFAULT_COOPERATIVA_NOMBRE)
        foto = get_system_setting(conn, 'cooperativa_foto', '')
    except Exception as e:
        print("EXCEPTION IN obtener_marca_cooperativa:", e)
        nombre = DEFAULT_COOPERATIVA_NOMBRE
        foto = ''
    finally:
        conn.close()

    return {
        'cooperativa_nombre': nombre or DEFAULT_COOPERATIVA_NOMBRE,
        'cooperativa_foto': foto or None,
    }


@app.context_processor
def inject_global_template_data():
    marca = obtener_marca_cooperativa()
    return {
        'now': datetime.now(),
        **marca,
    }


def get_config_label(tipo):
    """Retorna una etiqueta amigable para mostrar configuraciones al usuario."""
    return CONFIG_LABELS.get(tipo, 'configuracion seleccionada')


def obtener_tipo_cuenta_desde_planilla(nombre_planilla):
    """Extrae y normaliza el tipo de cuenta guardado en el nombre de la planilla."""
    nombre = (nombre_planilla or '').strip()
    if '[' in nombre and ']' in nombre:
        tipo = nombre.split('[', 1)[1].split(']', 1)[0].strip().lower()
        mapa = {
            'aportacion': 'Aportacion',
            'ahorro corriente': 'Ahorro corriente',
            'plazo fijo': 'Plazo fijo',
        }
        return mapa.get(tipo, tipo.title() if tipo else 'Ahorro corriente')
    return 'Ahorro corriente'

@app.template_filter('tipo_transaccion')
def tipo_transaccion_label(tipo):
    """Convierte identificadores tecnicos de transaccion en etiquetas amigables."""
    if not tipo:
        return 'Movimiento'
    return TRANSACCION_LABELS.get(tipo, tipo.replace('_', ' ').title())

@app.template_filter('es_transaccion_positiva')
def es_transaccion_positiva(tipo):
    """Indica si una transaccion debe mostrarse como positiva."""
    if not tipo:
        return False
    return tipo in TRANSACCIONES_POSITIVAS

def calcular_proximo_pago(fecha_ultimo_pago, frecuencia):
    """Calcula la fecha del próximo pago basado en la frecuencia"""
    from datetime import datetime, timedelta
    
    if isinstance(fecha_ultimo_pago, str):
        fecha_ultimo_pago = datetime.fromisoformat(fecha_ultimo_pago)
    
    if frecuencia == 'Catorcenal':
        return fecha_ultimo_pago + timedelta(days=14)
    elif frecuencia == 'Quincenal':
        return fecha_ultimo_pago + timedelta(days=15)
    else:
        # Default a quincenal
        return fecha_ultimo_pago + timedelta(days=15)


def obtener_dias_frecuencia(frecuencia):
    return 14 if (frecuencia or '').strip().lower() == 'catorcenal' else 15


def calcular_total_cuotas_prestamo(plazo_meses, frecuencia):
    plazo_meses = int(plazo_meses or 0)
    if plazo_meses <= 0:
        return 0
    return max(1, math.ceil((plazo_meses * 30) / obtener_dias_frecuencia(frecuencia)))


def calcular_resumen_prestamo(monto, tasa_anual, plazo_meses, frecuencia):
    monto = float(monto or 0)
    tasa_anual = float(tasa_anual or 0)
    frecuencia = frecuencia or 'Quincenal'
    dias_frecuencia = obtener_dias_frecuencia(frecuencia)
    total_cuotas = calcular_total_cuotas_prestamo(plazo_meses, frecuencia)
    tasa_periodica = (tasa_anual / 100) * (dias_frecuencia / 365)

    if monto <= 0 or total_cuotas <= 0:
        return {
            'frecuencia': frecuencia,
            'dias_frecuencia': dias_frecuencia,
            'total_cuotas': total_cuotas,
            'cuota': 0.0,
            'total': 0.0,
            'intereses': 0.0,
            'tasa_periodica': tasa_periodica,
        }

    if tasa_periodica > 0:
        cuota = monto * tasa_periodica / (1 - (1 + tasa_periodica) ** (-total_cuotas))
    else:
        cuota = monto / total_cuotas

    total = cuota * total_cuotas
    return {
        'frecuencia': frecuencia,
        'dias_frecuencia': dias_frecuencia,
        'total_cuotas': total_cuotas,
        'cuota': round(cuota, 2),
        'total': round(total, 2),
        'intereses': round(total - monto, 2),
        'tasa_periodica': tasa_periodica,
    }


def generar_calendario_prestamo(fecha_primer_pago, total_cuotas, monto_cuota, frecuencia):
    fecha_base = normalizar_fecha_referencia(fecha_primer_pago)
    dias = obtener_dias_frecuencia(frecuencia)
    calendario = []

    for numero in range(1, int(total_cuotas or 0) + 1):
        fecha_cuota = fecha_base + timedelta(days=(numero - 1) * dias)
        calendario.append({
            'numero_cuota': numero,
            'fecha_programada': fecha_cuota.isoformat(),
            'monto_programado': round(float(monto_cuota or 0), 2),
        })

    return calendario





def obtener_beneficiarios_socio(conn, socio_id):
    return [
        dict(row) for row in db_fetchall(
            conn,
            '''
            SELECT id, nombre, parentesco, porcentaje
            FROM socio_beneficiarios
            WHERE socio_id=?
            ORDER BY id
            ''',
            [socio_id]
        )
    ]


def parsear_beneficiarios_form(form):
    nombres = form.getlist('beneficiario_nombre[]')
    parentescos = form.getlist('beneficiario_parentesco[]')
    porcentajes = form.getlist('beneficiario_porcentaje[]')

    beneficiarios = []
    for nombre, parentesco, porcentaje in zip(nombres, parentescos, porcentajes):
        nombre = (nombre or '').strip()
        parentesco = (parentesco or '').strip()
        porcentaje = (porcentaje or '').strip()

        if not nombre and not parentesco and not porcentaje:
            continue

        if not nombre or not parentesco or not porcentaje:
            raise ValueError('Cada beneficiario debe incluir nombre, parentesco y porcentaje.')

        try:
            porcentaje_valor = round(float(porcentaje), 2)
        except ValueError:
            raise ValueError('El porcentaje de cada beneficiario debe ser numérico.')

        if porcentaje_valor <= 0:
            raise ValueError('El porcentaje de cada beneficiario debe ser mayor que cero.')

        beneficiarios.append({
            'nombre': nombre,
            'parentesco': parentesco,
            'porcentaje': porcentaje_valor,
        })

    if beneficiarios:
        total = round(sum(item['porcentaje'] for item in beneficiarios), 2)
        if abs(total - 100) > 0.01:
            raise ValueError('El porcentaje total de beneficiarios debe sumar 100%.')

    return beneficiarios

def normalizar_fecha_referencia(fecha_referencia=None):
    """Normaliza una fecha de referencia a date para validaciones de frecuencia."""
    if not fecha_referencia:
        return date.today()
    if isinstance(fecha_referencia, date) and not isinstance(fecha_referencia, datetime):
        return fecha_referencia
    if isinstance(fecha_referencia, datetime):
        return fecha_referencia.date()
    if isinstance(fecha_referencia, str):
        try:
            return datetime.fromisoformat(fecha_referencia).date()
        except ValueError:
            return date.fromisoformat(fecha_referencia[:10])
    return date.today()

def validar_pago_frecuencia(socio_id, tipo_pago, fecha_referencia=None):
    """
    Valida si un socio puede hacer un pago según su frecuencia configurada.
    Retorna True si puede pagar, False si no.
    """
    conn = get_db()

    socio = db_fetchone(conn, "SELECT frecuencia, cuota_ahorro FROM socios WHERE id=?", [socio_id])
    if not socio or not socio['frecuencia']:
        conn.close()
        return True

    hoy = normalizar_fecha_referencia(fecha_referencia)
    fecha_limite = hoy.isoformat()

    if tipo_pago == 'ahorro':
        ultimo_deposito = db_fetchone(
            conn,
            '''
            SELECT fecha FROM transacciones t
            JOIN cuentas c ON t.cuenta_id = c.id
            WHERE c.socio_id = ?
              AND t.tipo = 'deposito'
              AND t.monto = ?
              AND date(t.fecha) <= date(?)
            ORDER BY t.fecha DESC LIMIT 1
            ''',
            [socio_id, socio['cuota_ahorro'], fecha_limite],
        )
        if ultimo_deposito:
            proximo_pago = calcular_proximo_pago(ultimo_deposito['fecha'], socio['frecuencia'])
            if hoy < proximo_pago.date():
                conn.close()
                return False

    elif tipo_pago == 'prestamo':
        ultimo_pago_prestamo = db_fetchone(
            conn,
            '''
            SELECT fecha FROM pagos_prestamo pp
            JOIN prestamos p ON pp.prestamo_id = p.id
            WHERE p.socio_id = ?
              AND date(pp.fecha) <= date(?)
            ORDER BY pp.fecha DESC LIMIT 1
            ''',
            [socio_id, fecha_limite],
        )
        if ultimo_pago_prestamo:
            proximo_pago = calcular_proximo_pago(ultimo_pago_prestamo['fecha'], socio['frecuencia'])
            if hoy < proximo_pago.date():
                conn.close()
                return False

    conn.close()
    return True

def obtener_mensaje_validacion_frecuencia(socio_id, tipo_pago, fecha_referencia=None):
    """
    Retorna un mensaje explicativo cuando un pago no puede hacerse por frecuencia.
    """
    conn = get_db()
    socio = db_fetchone(conn, "SELECT frecuencia FROM socios WHERE id=?", [socio_id])
    conn.close()
    
    if not socio or not socio['frecuencia']:
        return ""
    
    frecuencia_dias = 14 if socio['frecuencia'] == 'Catorcenal' else 15
    fecha_ref = normalizar_fecha_referencia(fecha_referencia)
    return f"Según la frecuencia {socio['frecuencia'].lower()} configurada para la fecha {fecha_ref.isoformat()}, debe esperar {frecuencia_dias} días entre pagos."


# ── Routes ────────────────────────────────────────────────────────────────────







# ── SOCIOS ────────────────────────────────────────────────────────────────────

def log_auditoria_socio(socio_id, user_id, accion, datos_previos=None, datos_nuevos=None):
    conn = get_db()
    db_execute(conn, '''
        INSERT INTO auditoria_socios (socio_id, user_id, accion, datos_previos, datos_nuevos, fecha)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (socio_id, user_id, accion, datos_previos, datos_nuevos, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def log_auditoria_evento(modulo, entidad, accion, entidad_id=None, descripcion='', datos=None):
    conn = get_db()
    db_execute(
        conn,
        '''
        INSERT INTO auditoria_eventos (modulo, entidad, entidad_id, accion, descripcion, datos, usuario, fecha)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            modulo,
            entidad,
            entidad_id,
            accion,
            descripcion,
            json.dumps(datos, ensure_ascii=False) if isinstance(datos, (dict, list)) else (datos or ''),
            session.get('username', 'sistema'),
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def periodo_cerrado(modulo, fecha_evento=None):
    fecha_eval = normalizar_fecha_referencia(fecha_evento).isoformat()
    conn = get_db()
    cierre = db_fetchone(
        conn,
        '''
        SELECT id FROM cierres_periodo
        WHERE modulo = ?
          AND estado = 'cerrado'
          AND date(?) BETWEEN date(fecha_inicio) AND date(fecha_fin)
        LIMIT 1
        ''',
        (modulo, fecha_eval),
    )
    conn.close()
    return cierre is not None


def generar_numero_comprobante(conn):
    ultimo = db_fetchone(conn, 'SELECT MAX(id) FROM pagos_prestamo')[0] or 0
    return f'REC-{ultimo + 1:06d}'















# ── CONFIGURACIONES ───────────────────────────────────────────────────────────


# ── CUENTAS ───────────────────────────────────────────────────────────────────





# ── PRÉSTAMOS ─────────────────────────────────────────────────────────────────

















# ==================== TRANSACCIONES MASIVAS ====================


















# ==================== OPERACIONES DE AHORRO ====================


# ==================== HELPERS DE CARGA MASIVA ====================

def _leer_archivo_masivo(uploaded_file, expected_fields):
    import csv
    data = []
    filename = uploaded_file.filename.lower()

    if filename.endswith('.xlsx'):
        raise RuntimeError('Soporta CSV solamente en esta versión. Instale openpyxl o use archivo CSV.')

    if filename.endswith('.csv'):
        text = uploaded_file.read().decode('utf-8-sig')
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            data.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k,v in row.items()})
        return data

    raise RuntimeError('Formato de archivo no soportado. Use .csv o .xlsx')


def _parse_planilla_metadata(descripcion):
    metadata = {
        'nombre_planilla': '',
        'boleta_deposito': '',
        'frecuencia': ''
    }
    if not descripcion:
        return metadata

    for part in [p.strip() for p in descripcion.split('|')]:
        if ':' not in part:
            continue
        key, value = part.split(':', 1)
        key = key.strip().lower()
        value = value.strip()

        if key == 'planilla':
            metadata['nombre_planilla'] = value
        elif key == 'boleta':
            metadata['boleta_deposito'] = value
        elif key == 'frecuencia':
            metadata['frecuencia'] = value

    return metadata


















# ==================== OPERACIONES DE PRÉSTAMOS ====================







def _generar_datos_reporte_prestamos(tipo_reporte, fecha_inicio=None, fecha_fin=None):
    cartera = _obtener_cartera_con_alertas(fecha_inicio, fecha_fin)
    cartera_activa = [p for p in cartera if p['estado'] == 'aprobado' and float(p['saldo_pendiente'] or 0) > 0]
    vencidos = [p for p in cartera_activa if p['dias_atraso'] > 0]

    total_prestamos = len(cartera_activa)
    cartera_total = sum(float(p['saldo_pendiente'] or 0) for p in cartera_activa)
    promedio = (cartera_total / total_prestamos) if total_prestamos else 0.0
    tasa_morosidad = (len(vencidos) * 100.0 / total_prestamos) if total_prestamos else 0.0

    conn = get_db()
    params_rango = []
    where_rango = ''
    if fecha_inicio:
        where_rango += ' AND date(pp.fecha) >= date(?)'
        params_rango.append(fecha_inicio)
    if fecha_fin:
        where_rango += ' AND date(pp.fecha) <= date(?)'
        params_rango.append(fecha_fin)

    intereses = db_fetchone(
        conn,
        f"SELECT COALESCE(SUM(pp.interes),0) FROM pagos_prestamo pp WHERE 1=1 {where_rango}",
        params_rango
    )[0]
    conn.close()

    rendimiento_cartera = (float(intereses) * 100.0 / cartera_total) if cartera_total else 0.0

    estadisticas = {
        'total_prestamos': total_prestamos,
        'cartera_total': float(cartera_total),
        'promedio_prestamo': float(promedio),
        'prestamos_vencidos': len(vencidos),
        'tasa_morosidad': float(tasa_morosidad),
        'rendimiento_cartera': float(rendimiento_cartera),
    }

    morosidad = {
        'al_dia': len([p for p in cartera_activa if p['dias_atraso'] == 0]),
        'atraso_1_30': len([p for p in cartera_activa if 1 <= p['dias_atraso'] <= 30]),
        'atraso_31_mas': len([p for p in cartera_activa if p['dias_atraso'] > 30]),
    }

    if tipo_reporte == 'cartera_activa':
        resultados = [{
            'numero_prestamo': p['numero'],
            'nombre_socio': p['nombre_socio'],
            'monto_original': float(p['monto_aprobado'] or p['monto_solicitado'] or 0),
            'saldo_actual': float(p['saldo_pendiente'] or 0),
            'cuotas_pendientes': int(p['cuotas_pendientes']),
            'proximo_pago': p['proximo_pago'] or 'N/A',
            'estado': 'activo' if p['dias_atraso'] == 0 else 'en seguimiento',
        } for p in cartera_activa]
    elif tipo_reporte == 'morosidad':
        resultados = [{
            'numero_prestamo': p['numero'],
            'nombre_socio': p['nombre_socio'],
            'dias_atraso': int(p['dias_atraso']),
            'monto_vencido': float(p['monto_vencido']),
            'ultimo_pago': p['ultimo_pago'] or 'Sin pagos',
        } for p in cartera_activa]
    elif tipo_reporte == 'pagos_vencidos':
        resultados = [{
            'numero_prestamo': p['numero'],
            'nombre_socio': p['nombre_socio'],
            'fecha_vencimiento': p['proximo_pago'] or 'N/A',
            'monto_vencido': float(p['monto_vencido']),
            'dias_atraso': int(p['dias_atraso']),
        } for p in vencidos]
    elif tipo_reporte == 'rendimiento':
        conn = get_db()
        if _is_postgres_connection(conn):
            rows = db_fetchall(
                conn,
                '''
                SELECT to_char(fecha::date, 'YYYY-MM') AS mes,
                       COALESCE(SUM(interes),0) AS intereses_cobrados,
                       COALESCE(AVG(saldo_restante),0) AS cartera_promedio
                FROM pagos_prestamo
                GROUP BY to_char(fecha::date, 'YYYY-MM')
                ORDER BY mes DESC
                LIMIT 12
                '''
            )
        else:
            rows = db_fetchall(
                conn,
                '''
                SELECT substr(fecha,1,7) AS mes,
                       COALESCE(SUM(interes),0) AS intereses_cobrados,
                       COALESCE(AVG(saldo_restante),0) AS cartera_promedio
                FROM pagos_prestamo
                GROUP BY substr(fecha,1,7)
                ORDER BY mes DESC
                LIMIT 12
                '''
            )
        conn.close()
        resultados = []
        for r in rows:
            cartera_promedio = float(r['cartera_promedio'] or 0)
            interes_mes = float(r['intereses_cobrados'] or 0)
            resultados.append({
                'mes': r['mes'],
                'intereses_cobrados': interes_mes,
                'morosidad': float(tasa_morosidad),
                'cartera_promedio': cartera_promedio,
                'rendimiento': (interes_mes * 100.0 / cartera_promedio) if cartera_promedio else 0.0,
            })
    elif tipo_reporte == 'comparativo':
        conn = get_db()
        if _is_postgres_connection(conn):
            rows = db_fetchall(
                conn,
                '''
                SELECT to_char(fecha_solicitud::date, 'YYYY-MM') AS mes,
                       COUNT(*) AS nuevos_prestamos,
                       COALESCE(SUM(monto_solicitado),0) AS cartera_actual
                FROM prestamos
                GROUP BY to_char(fecha_solicitud::date, 'YYYY-MM')
                ORDER BY mes DESC
                LIMIT 12
                '''
            )
        else:
            rows = db_fetchall(
                conn,
                '''
                SELECT substr(fecha_solicitud,1,7) AS mes,
                       COUNT(*) AS nuevos_prestamos,
                       COALESCE(SUM(monto_solicitado),0) AS cartera_actual
                FROM prestamos
                GROUP BY substr(fecha_solicitud,1,7)
                ORDER BY mes DESC
                LIMIT 12
                '''
            )
        conn.close()
        resultados = []
        cartera_anterior = 0.0
        for r in reversed(rows):
            actual = float(r['cartera_actual'] or 0)
            resultados.append({
                'mes': r['mes'],
                'nuevos_prestamos': int(r['nuevos_prestamos']),
                'cartera_anterior': cartera_anterior,
                'cartera_actual': actual,
            })
            cartera_anterior = actual
        resultados.reverse()
    elif tipo_reporte == 'riesgo':
        resultados = []
        for p in cartera_activa:
            score = 100
            score -= min(p['dias_atraso'], 120) * 0.4
            score -= min((float(p['saldo_pendiente'] or 0) * 100.0 / max(float(p['monto_aprobado'] or p['monto_solicitado'] or 1), 1)), 100) * 0.2
            score = max(0, int(score))
            if p['dias_atraso'] == 0:
                historial = 'excelente'
            elif p['dias_atraso'] <= 15:
                historial = 'bueno'
            else:
                historial = 'regular'
            capacidad = 'alta' if score >= 75 else ('media' if score >= 50 else 'baja')
            nivel = 'bajo' if score >= 75 else ('medio' if score >= 50 else 'alto')
            resultados.append({
                'numero_prestamo': p['numero'],
                'nombre_socio': p['nombre_socio'],
                'score_riesgo': score,
                'historial_pagos': historial,
                'capacidad_pago': capacidad,
                'nivel_riesgo': nivel,
            })
    else:
        raise ValueError('Tipo de reporte desconocido')

    return resultados, estadisticas, morosidad
















































def _obtener_historial_planillas(tipo='todos', nombre='', boleta='', frecuencia='', fecha_desde='', fecha_hasta=''):

    conn = get_db_connection()
    ahorro_rows = db_fetchall(conn, '''
        SELECT t.fecha, t.monto, t.descripcion, s.frecuencia
        FROM transacciones t
        JOIN cuentas c ON t.cuenta_id = c.id
        JOIN socios s ON c.socio_id = s.id
        WHERE t.tipo = 'deposito'
          AND t.descripcion LIKE 'Planilla:%'
    ''')

    prestamo_rows = db_fetchall(conn, '''
        SELECT pp.fecha, pp.monto, pp.descripcion, pp.boleta_deposito, s.frecuencia
        FROM pagos_prestamo pp
        JOIN prestamos p ON pp.prestamo_id = p.id
        JOIN socios s ON p.socio_id = s.id
        WHERE pp.descripcion LIKE 'Planilla:%'
           OR COALESCE(pp.boleta_deposito, '') <> ''
    ''')
    conn.close()

    movimientos = []

    for row in ahorro_rows:
        meta = _parse_planilla_metadata(row['descripcion'])
        movimientos.append({
            'tipo': 'ahorro',
            'fecha': (row['fecha'] or '')[:10],
            'monto': float(row['monto'] or 0),
            'nombre_planilla': meta['nombre_planilla'] or 'Sin nombre',
            'boleta_deposito': meta['boleta_deposito'],
            'frecuencia': meta['frecuencia'] or row['frecuencia'] or 'N/A'
        })

    for row in prestamo_rows:
        meta = _parse_planilla_metadata(row['descripcion'])
        movimientos.append({
            'tipo': 'prestamo',
            'fecha': (row['fecha'] or '')[:10],
            'monto': float(row['monto'] or 0),
            'nombre_planilla': meta['nombre_planilla'] or 'Sin nombre',
            'boleta_deposito': meta['boleta_deposito'] or (row['boleta_deposito'] or ''),
            'frecuencia': meta['frecuencia'] or row['frecuencia'] or 'N/A'
        })

    filtrados = []
    for item in movimientos:
        if tipo in ('ahorro', 'prestamo') and item['tipo'] != tipo:
            continue
        if nombre and nombre not in item['nombre_planilla'].lower():
            continue
        if boleta and boleta not in item['boleta_deposito'].lower():
            continue
        if frecuencia and item['frecuencia'] != frecuencia:
            continue
        if fecha_desde and item['fecha'] and item['fecha'] < fecha_desde:
            continue
        if fecha_hasta and item['fecha'] and item['fecha'] > fecha_hasta:
            continue
        filtrados.append(item)

    resumen = {}
    for item in filtrados:
        key = (
            item['tipo'],
            item['nombre_planilla'],
            item['fecha'],
            item['boleta_deposito'],
            item['frecuencia']
        )
        if key not in resumen:
            resumen[key] = {
                'tipo': item['tipo'],
                'nombre_planilla': item['nombre_planilla'],
                'fecha': item['fecha'],
                'boleta_deposito': item['boleta_deposito'],
                'frecuencia': item['frecuencia'],
                'registros': 0,
                'total': 0.0
            }
        resumen[key]['registros'] += 1
        resumen[key]['total'] += item['monto']

    planillas = sorted(
        resumen.values(),
        key=lambda x: (x['fecha'], x['nombre_planilla']),
        reverse=True
    )

    total_general = sum(p['total'] for p in planillas)
    total_registros = sum(p['registros'] for p in planillas)

    return planillas, total_general, total_registros


def _exportar_historial_csv(planillas):
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Tipo', 'Nombre Planilla', 'Fecha Pago', 'No. Boleta', 'Frecuencia', 'Registros', 'Total'])

    for p in planillas:
        writer.writerow([
            'Ahorro' if p['tipo'] == 'ahorro' else 'Prestamos',
            p['nombre_planilla'],
            p['fecha'],
            p['boleta_deposito'] or '',
            p['frecuencia'],
            p['registros'],
            f"{p['total']:.2f}"
        ])

    filename = f"historial_planillas_{date.today().isoformat()}.csv"
    return Response(
        output.getvalue(),
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


def _exportar_historial_excel(planillas):
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = 'Historial Planillas'

    headers = ['Tipo', 'Nombre Planilla', 'Fecha Pago', 'No. Boleta', 'Frecuencia', 'Registros', 'Total']
    ws.append(headers)

    for p in planillas:
        ws.append([
            'Ahorro' if p['tipo'] == 'ahorro' else 'Prestamos',
            p['nombre_planilla'],
            p['fecha'],
            p['boleta_deposito'] or '',
            p['frecuencia'],
            p['registros'],
            float(p['total'])
        ])

    file_data = BytesIO()
    wb.save(file_data)
    file_data.seek(0)

    filename = f"historial_planillas_{date.today().isoformat()}.xlsx"
    return send_file(
        file_data,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

if __name__ == '__main__':
    init_db()
    print("\n🏦 Cooperativa de Ahorro y Crédito")
    print("=" * 40)
    print("▶  Abre tu navegador en: http://localhost:8001")
    print("   Presiona Ctrl+C para detener\n")
    app.run(debug=True, port=8001)
