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

# Cargar variables de entorno desde .env
load_dotenv()

from config import (
    DB, SOCIOS_UPLOAD_DIR, COOPERATIVA_UPLOAD_DIR, DEFAULT_COOPERATIVA_NOMBRE,
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

app = Flask(__name__)

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

ROLE_PERMISSION_DEFAULTS = {
    'Administrador': {'*'},
    'Operador': {
        'socios.ver', 'socios.editar', 'socios.estado',
        'ahorro.ver', 'ahorro.transaccion', 'ahorro.masivo',
        'prestamos.ver', 'prestamos.pagar',
        'reportes.ver',
    },
    'Asociado': {
        'socios.ver',
    },
}


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
                return redirect(url_for('login'))
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
                return redirect(url_for('index'))
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
                return redirect(url_for('login'))

            if role:
                user_role = session.get('user_role')
                allowed_roles = role
                if isinstance(role, str):
                    allowed_roles = [role]
                if user_role != 'Administrador' and user_role not in allowed_roles:
                    flash('Acceso denegado para su rol', 'danger')
                    return redirect(url_for('index'))

            return fn(*args, **kwargs)
        return wrapper
    return decorator

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout = 30000')
    conn.execute('PRAGMA journal_mode = WAL')
    return conn


def get_db_connection():
    # Alias de compatibilidad para rutas legacy.
    return get_db()

def get_config(tipo):
    """Obtiene el valor de una configuración por tipo"""
    conn = get_db()
    config = conn.execute("SELECT tasa_interes FROM configuraciones WHERE tipo=?", [tipo]).fetchone()
    conn.close()
    return config['tasa_interes'] if config else 0


def ensure_system_settings(conn):
    hoy = date.today().isoformat()
    for clave, valor in SYSTEM_SETTINGS_DEFAULTS.items():
        conn.execute(
            """INSERT OR IGNORE INTO ajustes_sistema
               (clave, valor, fecha_actualizacion)
               VALUES (?, ?, ?)""",
            (clave, valor, hoy)
        )


def ensure_module_settings(conn):
    hoy = date.today().isoformat()
    for defaults in (AHORRO_SETTINGS_DEFAULTS, PRESTAMO_SETTINGS_DEFAULTS):
        for clave, valor in defaults.items():
            conn.execute(
                """INSERT OR IGNORE INTO ajustes_sistema
                   (clave, valor, fecha_actualizacion)
                   VALUES (?, ?, ?)""",
                (clave, valor, hoy)
            )


def ensure_default_prestamo_categories(conn):
    hoy = date.today().isoformat()
    for nombre, descripcion in DEFAULT_PRESTAMO_CATEGORIAS:
        conn.execute(
            """INSERT OR IGNORE INTO prestamo_categorias
               (nombre, descripcion, estado, fecha_actualizacion)
               VALUES (?, ?, 'activo', ?)""",
            (nombre, descripcion, hoy)
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
        conn.execute(
            "INSERT OR IGNORE INTO permisos (codigo, nombre) VALUES (?, ?)",
            (codigo, nombre),
        )

    for rol, perms in ROLE_PERMISSION_DEFAULTS.items():
        rol_row = conn.execute("SELECT id FROM roles WHERE nombre=?", (rol,)).fetchone()
        if not rol_row:
            continue
        if '*' in perms:
            continue
        for perm in perms:
            perm_row = conn.execute("SELECT id FROM permisos WHERE codigo=?", (perm,)).fetchone()
            if not perm_row:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO rol_permisos (rol_id, permiso_id) VALUES (?, ?)",
                (rol_row['id'], perm_row['id']),
            )


def get_system_setting(conn, clave, default=None):
    row = conn.execute("SELECT valor FROM ajustes_sistema WHERE clave=?", (clave,)).fetchone()
    if row and row['valor'] is not None:
        return row['valor']
    if default is not None:
        return default
    return SYSTEM_SETTINGS_DEFAULTS.get(clave)


def set_system_setting(conn, clave, valor, usuario=None):
    conn.execute(
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
    except sqlite3.OperationalError:
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

def ensure_required_configurations(conn):
    """Asegura que existan las configuraciones base editables en el panel."""
    hoy = date.today().isoformat()
    for tipo, tasa, descripcion in REQUIRED_CONFIGURACIONES:
        conn.execute(
            """INSERT OR IGNORE INTO configuraciones
               (tipo, tasa_interes, descripcion, fecha_actualizacion)
               VALUES (?, ?, ?, ?)""",
            (tipo, tasa, descripcion, hoy)
        )
        conn.execute(
            "UPDATE configuraciones SET descripcion=? WHERE tipo=?",
            (descripcion, tipo)
        )

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


def renderizar_finiquito_prestamo(prestamo, plantilla):
    calendario = prestamo.get('calendario') or []
    fecha_primer_pago = calendario[0]['fecha_programada'] if calendario else '—'
    fecha_ultima_cuota = calendario[-1]['fecha_programada'] if calendario else '—'
    contexto = {
        'cooperativa_nombre': prestamo.get('cooperativa_nombre') or DEFAULT_COOPERATIVA_NOMBRE,
        'prestamo_numero': prestamo.get('numero') or '',
        'socio_nombre': prestamo.get('nombre_socio') or '',
        'socio_codigo': prestamo.get('socio_codigo') or '',
        'categoria_nombre': prestamo.get('categoria_nombre') or 'General',
        'monto_aprobado': f"{float(prestamo.get('monto_aprobado') or prestamo.get('monto_solicitado') or 0):,.2f}",
        'cuota': f"{float(prestamo.get('cuota_mensual') or 0):,.2f}",
        'frecuencia': prestamo.get('frecuencia') or 'Quincenal',
        'fecha_aprobacion': prestamo.get('fecha_aprobacion') or date.today().isoformat(),
        'fecha_primer_pago': fecha_primer_pago,
        'fecha_ultima_cuota': fecha_ultima_cuota,
        'total_cuotas': prestamo.get('total_cuotas') or len(calendario),
        'estado': prestamo.get('estado') or '',
        'desembolso_tipo': prestamo.get('desembolso_tipo') or 'No definido',
        'desembolso_referencia': prestamo.get('desembolso_referencia') or 'Sin referencia',
    }

    try:
        return (plantilla or SYSTEM_SETTINGS_DEFAULTS['prestamo_finiquito_texto']).format(**contexto)
    except KeyError:
        return SYSTEM_SETTINGS_DEFAULTS['prestamo_finiquito_texto'].format(**contexto)



def obtener_beneficiarios_socio(conn, socio_id):
    return [
        dict(row) for row in conn.execute(
            '''
            SELECT id, nombre, parentesco, porcentaje
            FROM socio_beneficiarios
            WHERE socio_id=?
            ORDER BY id
            ''',
            [socio_id]
        ).fetchall()
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
    
    # Obtener información del socio
    socio = conn.execute("SELECT frecuencia, cuota_ahorro FROM socios WHERE id=?", [socio_id]).fetchone()
    if not socio or not socio['frecuencia']:
        conn.close()
        return True  # Si no tiene frecuencia configurada, permitir el pago
    
    hoy = normalizar_fecha_referencia(fecha_referencia)
    
    fecha_limite = hoy.isoformat()

    if tipo_pago == 'ahorro':
        # Verificar último depósito de ahorro
        ultimo_deposito = conn.execute('''
            SELECT fecha FROM transacciones t
            JOIN cuentas c ON t.cuenta_id = c.id
            WHERE c.socio_id = ?
              AND t.tipo = 'deposito'
              AND t.monto = ?
              AND date(t.fecha) <= date(?)
            ORDER BY t.fecha DESC LIMIT 1
        ''', [socio_id, socio['cuota_ahorro'], fecha_limite]).fetchone()
        
        if ultimo_deposito:
            proximo_pago = calcular_proximo_pago(ultimo_deposito['fecha'], socio['frecuencia'])
            if hoy < proximo_pago.date():
                conn.close()
                return False  # No puede pagar aún
    
    elif tipo_pago == 'prestamo':
        # Verificar último pago de préstamo
        ultimo_pago_prestamo = conn.execute('''
            SELECT fecha FROM pagos_prestamo pp
            JOIN prestamos p ON pp.prestamo_id = p.id
            WHERE p.socio_id = ?
              AND date(pp.fecha) <= date(?)
            ORDER BY pp.fecha DESC LIMIT 1
        ''', [socio_id, fecha_limite]).fetchone()
        
        if ultimo_pago_prestamo:
            proximo_pago = calcular_proximo_pago(ultimo_pago_prestamo['fecha'], socio['frecuencia'])
            if hoy < proximo_pago.date():
                conn.close()
                return False  # No puede pagar aún
    
    conn.close()
    return True  # Puede pagar

def obtener_mensaje_validacion_frecuencia(socio_id, tipo_pago, fecha_referencia=None):
    """
    Retorna un mensaje explicativo cuando un pago no puede hacerse por frecuencia.
    """
    conn = get_db()
    socio = conn.execute("SELECT frecuencia FROM socios WHERE id=?", [socio_id]).fetchone()
    conn.close()
    
    if not socio or not socio['frecuencia']:
        return ""
    
    frecuencia_dias = 14 if socio['frecuencia'] == 'Catorcenal' else 15
    fecha_ref = normalizar_fecha_referencia(fecha_referencia)
    return f"Según la frecuencia {socio['frecuencia'].lower()} configurada para la fecha {fecha_ref.isoformat()}, debe esperar {frecuencia_dias} días entre pagos."

def init_db():
    conn = get_db()
    c = conn.cursor()

    # 1. Crear todas las tablas primero (si no existen)
    c.execute('''CREATE TABLE IF NOT EXISTS socios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        nombre TEXT NOT NULL,
        primer_nombre TEXT,
        segundo_nombre TEXT,
        tercer_nombre TEXT,
        apellido TEXT NOT NULL,
        primer_apellido TEXT,
        segundo_apellido TEXT,
        estado_civil TEXT DEFAULT 'Soltero',
        apellido_casada TEXT,
        dpi TEXT UNIQUE NOT NULL,
        telefono TEXT,
        email TEXT,
        direccion TEXT,
        rol TEXT DEFAULT 'Asociado',
        fecha_ingreso TEXT NOT NULL,
        estado TEXT DEFAULT "activo",
        frecuencia TEXT DEFAULT 'Quincenal',
        cuota_ahorro REAL DEFAULT 0,
        tipo_ahorro TEXT DEFAULT 'ahorro corriente',
        nit TEXT,
        beneficiario TEXT,
        finca TEXT,
        banco_nombre TEXT,
        banco_tipo_cuenta TEXT,
        banco_numero_cuenta TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS roles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT UNIQUE NOT NULL,
        descripcion TEXT,
        estado TEXT DEFAULT "activo"
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        rol_id INTEGER,
        activo TEXT DEFAULT "si",
        fecha_creacion TEXT NOT NULL,
        FOREIGN KEY (rol_id) REFERENCES roles(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS permisos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        nombre TEXT NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS rol_permisos (
        rol_id INTEGER NOT NULL,
        permiso_id INTEGER NOT NULL,
        PRIMARY KEY (rol_id, permiso_id),
        FOREIGN KEY (rol_id) REFERENCES roles(id),
        FOREIGN KEY (permiso_id) REFERENCES permisos(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS idempotency_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key_hash TEXT UNIQUE NOT NULL,
        scope TEXT NOT NULL,
        user_id INTEGER,
        created_at TEXT NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS configuraciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo TEXT UNIQUE NOT NULL,
        tasa_interes REAL NOT NULL,
        descripcion TEXT,
        fecha_actualizacion TEXT NOT NULL,
        usuario_actualizacion TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS prestamo_categorias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT UNIQUE NOT NULL,
        descripcion TEXT,
        estado TEXT DEFAULT 'activo',
        fecha_actualizacion TEXT NOT NULL,
        usuario_actualizacion TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS ajustes_sistema (
        clave TEXT PRIMARY KEY,
        valor TEXT,
        fecha_actualizacion TEXT NOT NULL,
        usuario_actualizacion TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS cuentas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT UNIQUE NOT NULL,
        socio_id INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        saldo REAL DEFAULT 0,
        tasa_interes REAL DEFAULT 0,
        fecha_apertura TEXT NOT NULL,
        estado TEXT DEFAULT "activa",
        FOREIGN KEY (socio_id) REFERENCES socios(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS transacciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cuenta_id INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        monto REAL NOT NULL,
        saldo_despues REAL NOT NULL,
        descripcion TEXT,
        fecha TEXT NOT NULL,
        FOREIGN KEY (cuenta_id) REFERENCES cuentas(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS solicitudes_retiro (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT UNIQUE NOT NULL,
        cuenta_id INTEGER NOT NULL,
        socio_id INTEGER NOT NULL,
        monto REAL NOT NULL,
        descripcion TEXT,
        metodo_retiro TEXT DEFAULT 'cheque',
        banco_tipo_cuenta TEXT,
        banco_numero_cuenta TEXT,
        fecha_solicitud TEXT NOT NULL,
        estado TEXT DEFAULT 'pendiente',
        fecha_aprobacion TEXT,
        aprobado_por TEXT,
        FOREIGN KEY (cuenta_id) REFERENCES cuentas(id),
        FOREIGN KEY (socio_id) REFERENCES socios(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS auditoria_socios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        socio_id INTEGER NOT NULL,
        user_id INTEGER,
        accion TEXT NOT NULL,
        datos_previos TEXT,
        datos_nuevos TEXT,
        fecha TEXT NOT NULL,
        FOREIGN KEY (socio_id) REFERENCES socios(id),
        FOREIGN KEY (user_id) REFERENCES usuarios(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS socio_beneficiarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        socio_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        parentesco TEXT NOT NULL,
        porcentaje REAL NOT NULL,
        FOREIGN KEY (socio_id) REFERENCES socios(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS planillas_masivas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo TEXT NOT NULL,
        nombre TEXT NOT NULL,
        fecha_pago TEXT NOT NULL,
        frecuencia TEXT,
        estado TEXT DEFAULT 'pendiente',
        boleta_deposito TEXT,
        total_monto REAL DEFAULT 0,
        total_registros INTEGER DEFAULT 0,
        fecha_creacion TEXT NOT NULL,
        fecha_aplicacion TEXT,
        usuario_creacion TEXT,
        usuario_aplicacion TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS planilla_masiva_detalles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        planilla_id INTEGER NOT NULL,
        referencia_tipo TEXT NOT NULL,
        referencia_id INTEGER NOT NULL,
        numero_referencia TEXT,
        socio_codigo TEXT,
        socio_nombre TEXT,
        monto REAL NOT NULL,
        estado TEXT DEFAULT 'pendiente',
        FOREIGN KEY (planilla_id) REFERENCES planillas_masivas(id)
    )''')

    # Índices para acelerar consultas frecuentes.
    c.execute("CREATE INDEX IF NOT EXISTS idx_socios_codigo ON socios(codigo)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_socios_dpi ON socios(dpi)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_socios_estado ON socios(estado)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cuentas_socio_id ON cuentas(socio_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_transacciones_cuenta_fecha ON transacciones(cuenta_id, fecha)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_solicitudes_retiro_estado_fecha ON solicitudes_retiro(estado, fecha_solicitud)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_solicitudes_retiro_cuenta_id ON solicitudes_retiro(cuenta_id)")

    # 2. Manejo de Migraciones (columnas nuevas)

    # Asegurar columna rol en socios para versiones previas de BD
    c.execute("PRAGMA table_info(socios)")
    cols = [row[1] for row in c.fetchall()]
    if 'rol' not in cols:
        try:
            c.execute("ALTER TABLE socios ADD COLUMN rol TEXT DEFAULT 'Asociado'")
        except Exception:
            pass

    # Migrar columnas nuevas para asociados (siempre verificar)
    columnas_nuevas = {
        'frecuencia': "TEXT DEFAULT 'Quincenal'",
        'cuota_ahorro': "REAL DEFAULT 0",
        'tipo_ahorro': "TEXT DEFAULT 'ahorro corriente'",
        'nit': "TEXT",
        'beneficiario': "TEXT",
        'finca': "TEXT",
        'foto': "TEXT",
        'primer_nombre': "TEXT",
        'segundo_nombre': "TEXT",
        'tercer_nombre': "TEXT",
        'primer_apellido': "TEXT",
        'segundo_apellido': "TEXT",
        'estado_civil': "TEXT DEFAULT 'Soltero'",
        'apellido_casada': "TEXT",
        'banco_nombre': "TEXT",
        'banco_tipo_cuenta': "TEXT",
        'banco_numero_cuenta': "TEXT"
    }

    for columna, definicion in columnas_nuevas.items():
        if columna not in cols:
            try:
                c.execute(f"ALTER TABLE socios ADD COLUMN {columna} {definicion}")
            except Exception:
                pass

        c.execute('''CREATE TABLE IF NOT EXISTS prestamos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero TEXT UNIQUE NOT NULL,
            socio_id INTEGER NOT NULL,
            monto_solicitado REAL NOT NULL,
            monto_aprobado REAL,
            tasa_interes REAL NOT NULL,
            plazo_meses INTEGER NOT NULL,
            cuota_mensual REAL,
            saldo_pendiente REAL,
            fecha_solicitud TEXT NOT NULL,
            fecha_aprobacion TEXT,
            estado TEXT DEFAULT "pendiente"
        )''')

        c.execute('''CREATE TABLE IF NOT EXISTS pagos_prestamo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prestamo_id INTEGER NOT NULL,
            monto REAL NOT NULL,
            capital REAL NOT NULL,
            interes REAL NOT NULL,
            saldo_restante REAL NOT NULL,
            descripcion TEXT,
            boleta_deposito TEXT,
            fecha TEXT NOT NULL,
            FOREIGN KEY (prestamo_id) REFERENCES prestamos(id)
        )''')

    c.execute('''CREATE TABLE IF NOT EXISTS auditoria_eventos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        modulo TEXT NOT NULL,
        entidad TEXT NOT NULL,
        entidad_id INTEGER,
        accion TEXT NOT NULL,
        descripcion TEXT,
        datos TEXT,
        usuario TEXT,
        fecha TEXT NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS cobranza_acciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prestamo_id INTEGER NOT NULL,
        tipo_accion TEXT NOT NULL,
        resultado TEXT NOT NULL,
        notas TEXT,
        monto_comprometido REAL DEFAULT 0,
        fecha_compromiso TEXT,
        fecha_accion TEXT NOT NULL,
        responsable TEXT,
        FOREIGN KEY (prestamo_id) REFERENCES prestamos(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS prestamo_calendario_pagos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prestamo_id INTEGER NOT NULL,
        numero_cuota INTEGER NOT NULL,
        fecha_programada TEXT NOT NULL,
        monto_programado REAL NOT NULL,
        estado TEXT DEFAULT 'pendiente',
        FOREIGN KEY (prestamo_id) REFERENCES prestamos(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS cierres_periodo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        modulo TEXT NOT NULL,
        fecha_inicio TEXT NOT NULL,
        fecha_fin TEXT NOT NULL,
        estado TEXT DEFAULT 'cerrado',
        observaciones TEXT,
        usuario TEXT,
        fecha_creacion TEXT NOT NULL
    )''')

    c.execute("CREATE INDEX IF NOT EXISTS idx_prestamos_socio_id ON prestamos(socio_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prestamos_estado_fecha ON prestamos(estado, fecha_solicitud)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pagos_prestamo_fecha ON pagos_prestamo(prestamo_id, fecha)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_auditoria_eventos_fecha ON auditoria_eventos(fecha)")

    c.execute("PRAGMA table_info(pagos_prestamo)")
    pagos_cols = [row[1] for row in c.fetchall()]
    if 'descripcion' not in pagos_cols:
        try:
            c.execute("ALTER TABLE pagos_prestamo ADD COLUMN descripcion TEXT")
        except Exception:
            pass
    if 'boleta_deposito' not in pagos_cols:
        try:
            c.execute("ALTER TABLE pagos_prestamo ADD COLUMN boleta_deposito TEXT")
        except Exception:
            pass
    if 'numero_comprobante' not in pagos_cols:
        try:
            c.execute("ALTER TABLE pagos_prestamo ADD COLUMN numero_comprobante TEXT")
        except Exception:
            pass

    c.execute("PRAGMA table_info(prestamos)")
    prestamos_cols = [row[1] for row in c.fetchall()]
    if 'etapa_cobranza' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN etapa_cobranza TEXT DEFAULT 'activo'")
        except Exception:
            pass
    if 'categoria_id' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN categoria_id INTEGER")
        except Exception:
            pass
    if 'desembolso_tipo' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN desembolso_tipo TEXT")
        except Exception:
            pass
    if 'desembolso_referencia' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN desembolso_referencia TEXT")
        except Exception:
            pass
    if 'refinanciado_de' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN refinanciado_de INTEGER")
        except Exception:
            pass
    if 'monto_amortizado' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN monto_amortizado REAL DEFAULT 0")
        except Exception:
            pass
    if 'monto_desembolso' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN monto_desembolso REAL")
        except Exception:
            pass
    if 'banco_tipo_cuenta' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN banco_tipo_cuenta TEXT")
        except Exception:
            pass
    if 'banco_numero_cuenta' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN banco_numero_cuenta TEXT")
        except Exception:
            pass

    c.execute("PRAGMA table_info(cuentas)")
    cuentas_cols = [row[1] for row in c.fetchall()]
    if 'producto_ahorro' not in cuentas_cols:
        try:
            c.execute("ALTER TABLE cuentas ADD COLUMN producto_ahorro TEXT")
        except Exception:
            pass
    try:
        c.execute(
            """UPDATE cuentas
               SET producto_ahorro='ahorro_corriente'
               WHERE tipo='ahorro' AND (producto_ahorro IS NULL OR trim(producto_ahorro)='')"""
        )
    except Exception:
        pass

    c.execute("PRAGMA table_info(solicitudes_retiro)")
    solicitudes_retiro_cols = [row[1] for row in c.fetchall()]
    if 'metodo_retiro' not in solicitudes_retiro_cols:
        try:
            c.execute("ALTER TABLE solicitudes_retiro ADD COLUMN metodo_retiro TEXT DEFAULT 'cheque'")
        except Exception:
            pass
    if 'banco_tipo_cuenta' not in solicitudes_retiro_cols:
        try:
            c.execute("ALTER TABLE solicitudes_retiro ADD COLUMN banco_tipo_cuenta TEXT")
        except Exception:
            pass
    if 'banco_numero_cuenta' not in solicitudes_retiro_cols:
        try:
            c.execute("ALTER TABLE solicitudes_retiro ADD COLUMN banco_numero_cuenta TEXT")
        except Exception:
            pass
    if 'destino' not in solicitudes_retiro_cols:
        try:
            c.execute("ALTER TABLE solicitudes_retiro ADD COLUMN destino TEXT DEFAULT 'retiro'")
        except Exception:
            pass
    if 'prestamo_id' not in solicitudes_retiro_cols:
        try:
            c.execute("ALTER TABLE solicitudes_retiro ADD COLUMN prestamo_id INTEGER")
        except Exception:
            pass

    c.execute("PRAGMA table_info(planillas_masivas)")
    planillas_cols = [row[1] for row in c.fetchall()]
    planillas_nuevas = {
        'frecuencia': "TEXT",
        'estado': "TEXT DEFAULT 'pendiente'",
        'boleta_deposito': "TEXT",
        'total_monto': "REAL DEFAULT 0",
        'total_registros': "INTEGER DEFAULT 0",
        'fecha_creacion': "TEXT",
        'fecha_aplicacion': "TEXT",
        'usuario_creacion': "TEXT",
        'usuario_aplicacion': "TEXT"
    }
    for columna, definicion in planillas_nuevas.items():
        if columna not in planillas_cols:
            try:
                c.execute(f"ALTER TABLE planillas_masivas ADD COLUMN {columna} {definicion}")
            except Exception:
                pass

    ensure_required_configurations(conn)
    ensure_system_settings(conn)
    ensure_default_prestamo_categories(conn)
    ensure_permissions_catalog(conn)

    try:
        socios_beneficiario_legacy = c.execute(
            '''
            SELECT id, beneficiario
            FROM socios
            WHERE beneficiario IS NOT NULL AND trim(beneficiario) <> ''
            '''
        ).fetchall()
        for socio_legacy in socios_beneficiario_legacy:
            existe = c.execute('SELECT 1 FROM socio_beneficiarios WHERE socio_id=? LIMIT 1', (socio_legacy['id'],)).fetchone()
            if not existe:
                c.execute(
                    'INSERT INTO socio_beneficiarios (socio_id, nombre, parentesco, porcentaje) VALUES (?, ?, ?, ?)',
                    (socio_legacy['id'], socio_legacy['beneficiario'].strip(), 'No especificado', 100)
                )
    except Exception:
        pass

    # Migrar contraseñas existentes a hash si no están hasheadas
    try:
        usuarios = c.execute("SELECT id, password FROM usuarios").fetchall()
        for usuario in usuarios:
            if not (usuario['password'].startswith('pbkdf2:sha256:') or usuario['password'].startswith('scrypt:')):
                hashed = generate_password_hash(usuario['password'])
                c.execute("UPDATE usuarios SET password=? WHERE id=?", (hashed, usuario['id']))
    except Exception:
        pass  # Si hay error, continuar

    # Demo data if empty
    c.execute("SELECT COUNT(*) FROM socios")
    if c.fetchone()[0] == 0:
        socios_demo = [
            ('SOC-001','María','García','1234567890101','5555-1001','maria@email.com','Zona 1, Guatemala','2022-01-15'),
            ('SOC-002','Carlos','Pérez','2345678901202','5555-1002','carlos@email.com','Zona 5, Guatemala','2022-03-10'),
            ('SOC-003','Ana','López','3456789012303','5555-1003','ana@email.com','Xela, Quetzaltenango','2023-06-20'),
            ('SOC-004','Luis','Martínez','4567890123404','5555-1004','luis@email.com','Cobán, A. Verapaz','2023-09-05'),
        ]
        for s in socios_demo:
            c.execute("INSERT INTO socios (codigo,nombre,apellido,dpi,telefono,email,direccion,rol,fecha_ingreso) VALUES (?,?,?,?,?,?,?,?,?)", (s[0], s[1], s[2], s[3], s[4], s[5], s[6], 'Asociado', s[7]))
        
        c.execute("INSERT OR IGNORE INTO roles (nombre,descripcion) VALUES (?,?)", ('Administrador','Acceso completo al sistema'))
        c.execute("INSERT OR IGNORE INTO roles (nombre,descripcion) VALUES (?,?)", ('Operador','Permite gestionar socios y cuentas'))
        
        # Configuraciones iniciales de tasas de interés
        configuraciones_demo = [
            ('ahorro_corriente', 2.5, 'Tasa de interés para cuentas de ahorro corriente', date.today().isoformat()),
            ('ahorro_plazo_fijo', 4.0, 'Tasa de interés para cuentas de ahorro a plazo fijo', date.today().isoformat()),
            ('ahorro_aportacion', 3.0, 'Tasa de interés para cuentas de aportación', date.today().isoformat()),
            ('prestamo_personal', 18.0, 'Tasa de interés para préstamos personales', date.today().isoformat()),
            ('prestamo_vivienda', 12.0, 'Tasa de interés para préstamos de vivienda', date.today().isoformat()),
            ('prestamo_negocio', 15.0, 'Tasa de interés para préstamos de negocio', date.today().isoformat()),
        ]
        for conf in configuraciones_demo:
            c.execute("INSERT OR IGNORE INTO configuraciones (tipo,tasa_interes,descripcion,fecha_actualizacion) VALUES (?,?,?,?)", conf)
        
        c.execute("INSERT OR IGNORE INTO usuarios (username,password,rol_id,fecha_creacion) VALUES (?,?,?,?)", ('admin',generate_password_hash('admin123'),1,date.today().isoformat()))

        cuentas_demo = [
            ('AHO-0001',1,'ahorro',15000,3.5,'2022-01-16'),
            ('AHO-0002',2,'ahorro',8500,3.5,'2022-03-11'),
            ('COR-0001',1,'corriente',3200,0,'2022-01-16'),
            ('AHO-0003',3,'ahorro',22000,3.5,'2023-06-21'),
            ('AHO-0004',4,'ahorro',5000,3.5,'2023-09-06'),
        ]
        for cu in cuentas_demo:
            c.execute("INSERT INTO cuentas (numero,socio_id,tipo,saldo,tasa_interes,fecha_apertura) VALUES (?,?,?,?,?,?)", cu)

        prestamos_demo = [
            ('PRE-0001',1,25000,25000,18,24,1041.67,12500,'2023-01-10','2023-01-15','aprobado'),
            ('PRE-0002',2,10000,10000,18,12,916.67,6000,'2023-06-01','2023-06-05','aprobado'),
            ('PRE-0003',3,50000,None,18,36,None,None,'2024-01-20',None,'pendiente'),
        ]
        for p in prestamos_demo:
            c.execute("INSERT INTO prestamos (numero,socio_id,monto_solicitado,monto_aprobado,tasa_interes,plazo_meses,cuota_mensual,saldo_pendiente,fecha_solicitud,fecha_aprobacion,estado) VALUES (?,?,?,?,?,?,?,?,?,?,?)", p)

        # Some transactions
        txns = [
            (1,'deposito',5000,15000,'Depósito inicial','2022-01-16'),
            (1,'deposito',3000,18000,'Depósito','2023-03-01'),
            (1,'retiro',3000,15000,'Retiro','2023-11-15'),
            (2,'deposito',8500,8500,'Apertura de cuenta','2022-03-11'),
            (4,'deposito',22000,22000,'Depósito','2023-06-21'),
        ]
        for t in txns:
            c.execute("INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)", t)

    # Se ejecuta al final para cubrir tanto BD nueva como existente.
    ensure_permissions_catalog(conn)

    conn.commit()
    conn.close()

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/login_test', methods=['GET','POST'])
def login_test():
    if request.method == 'POST':
        user = request.form['username'].strip()
        pwd = request.form['password'].strip()
        return f"POST received: user={user}, pwd={pwd[:10]}..."
    return render_template('login.html')

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        user = request.form['username'].strip()
        pwd = request.form['password'].strip()
        conn = get_db()
        row = conn.execute("SELECT u.*, r.nombre as rol_nombre FROM usuarios u LEFT JOIN roles r ON u.rol_id=r.id WHERE u.username=?", (user,)).fetchone()
        conn.close()
        if not row or not check_password_hash(row['password'], pwd):
            flash('Usuario o contraseña incorrectos', 'danger')
            return render_template('login.html')
        if row['activo'] != 'si':
            flash('Cuenta inactiva', 'danger')
            return render_template('login.html')
        session['user_id'] = row['id']
        session['username'] = row['username']
        session['user_role'] = row['rol_nombre'] or 'Asociado'
        flash('Bienvenido ' + session['username'], 'success')
        return redirect(url_for('index'))
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    flash('Sesión cerrada', 'info')
    return redirect(url_for('login'))


@app.route('/')
@login_required()
def index():
    conn = get_db()
    stats = {
        'total_socios': conn.execute("SELECT COUNT(*) FROM socios WHERE estado='activo'").fetchone()[0],
        'total_cuentas': conn.execute("SELECT COUNT(*) FROM cuentas WHERE estado='activa'").fetchone()[0],
        'total_ahorros': conn.execute("SELECT COALESCE(SUM(saldo),0) FROM cuentas WHERE estado='activa'").fetchone()[0],
        'prestamos_activos': conn.execute("SELECT COUNT(*) FROM prestamos WHERE estado='aprobado'").fetchone()[0],
        'cartera_prestamos': conn.execute("SELECT COALESCE(SUM(saldo_pendiente),0) FROM prestamos WHERE estado='aprobado'").fetchone()[0],
        'prestamos_pendientes': conn.execute("SELECT COUNT(*) FROM prestamos WHERE estado='pendiente'").fetchone()[0],
    }
    
# Estadísticas simples de préstamos
    stats['pagos_prestamos_hoy'] = 0  # Por ahora 0, se puede calcular más tarde si es necesario
    stats['monto_pagos_prestamos_hoy'] = 0.0
    
    # Socios por frecuencia
    stats['socios_catorcenal'] = conn.execute("SELECT COUNT(*) FROM socios WHERE estado='activo' AND frecuencia='Catorcenal'").fetchone()[0]
    stats['socios_quincenal'] = conn.execute("SELECT COUNT(*) FROM socios WHERE estado='activo' AND frecuencia='Quincenal'").fetchone()[0]
    
    ultimas_txn = conn.execute('''
        SELECT t.*, c.numero as cuenta_num, s.nombre||' '||s.apellido as socio
        FROM transacciones t
        JOIN cuentas c ON t.cuenta_id=c.id
        JOIN socios s ON c.socio_id=s.id
        ORDER BY t.id DESC LIMIT 5
    ''').fetchall()
    conn.close()
    return render_template('index.html', stats=stats, transacciones=ultimas_txn)

# ── SOCIOS ────────────────────────────────────────────────────────────────────

def log_auditoria_socio(socio_id, user_id, accion, datos_previos=None, datos_nuevos=None):
    conn = get_db()
    conn.execute('''
        INSERT INTO auditoria_socios (socio_id, user_id, accion, datos_previos, datos_nuevos, fecha)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (socio_id, user_id, accion, datos_previos, datos_nuevos, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def log_auditoria_evento(modulo, entidad, accion, entidad_id=None, descripcion='', datos=None):
    conn = get_db()
    conn.execute(
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
    cierre = conn.execute(
        '''
        SELECT id FROM cierres_periodo
        WHERE modulo = ?
          AND estado = 'cerrado'
          AND date(?) BETWEEN date(fecha_inicio) AND date(fecha_fin)
        LIMIT 1
        ''',
        (modulo, fecha_eval),
    ).fetchone()
    conn.close()
    return cierre is not None


def generar_numero_comprobante(conn):
    ultimo = conn.execute('SELECT MAX(id) FROM pagos_prestamo').fetchone()[0] or 0
    return f'REC-{ultimo + 1:06d}'


def _calcular_alerta_prestamo(prestamo):
    estado = (prestamo['estado'] or '').lower()
    if estado != 'aprobado' or float(prestamo['saldo_pendiente'] or 0) <= 0:
        return {
            'semaforo': 'al_dia',
            'estado_alerta': 'Al dia',
            'dias_atraso': 0,
            'monto_vencido': 0.0,
            'proximo_pago': None,
        }

    frecuencia = prestamo['frecuencia'] or 'Quincenal'
    total_cuotas = calcular_total_cuotas_prestamo(prestamo.get('plazo_meses'), frecuencia)
    referencia = prestamo['ultimo_pago'] or prestamo['fecha_aprobacion'] or prestamo['fecha_solicitud']
    proximo_pago = calcular_proximo_pago(referencia, frecuencia).date()
    dias_atraso = (date.today() - proximo_pago).days

    if dias_atraso > 0:
        semaforo = 'vencido'
        estado_alerta = 'Vencido'
        monto_vencido = min(float(prestamo['cuota_mensual'] or 0), float(prestamo['saldo_pendiente'] or 0))
    elif dias_atraso >= -3:
        semaforo = 'por_vencer'
        estado_alerta = 'Por vencer'
        monto_vencido = 0.0
    else:
        semaforo = 'al_dia'
        estado_alerta = 'Al dia'
        monto_vencido = 0.0

    return {
        'semaforo': semaforo,
        'estado_alerta': estado_alerta,
        'dias_atraso': max(dias_atraso, 0),
        'monto_vencido': monto_vencido,
        'proximo_pago': proximo_pago.isoformat(),
        'total_cuotas': total_cuotas,
    }


def _obtener_cartera_con_alertas(fecha_inicio=None, fecha_fin=None):
    conn = get_db()
    filtros = ''
    params = []
    if fecha_inicio:
        filtros += ' AND date(p.fecha_solicitud) >= date(?)'
        params.append(fecha_inicio)
    if fecha_fin:
        filtros += ' AND date(p.fecha_solicitud) <= date(?)'
        params.append(fecha_fin)

    rows = conn.execute(
        f'''
        SELECT p.*, s.id AS socio_id,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS nombre_socio,
               s.frecuencia,
               pc.nombre AS categoria_nombre,
               COALESCE(p.etapa_cobranza, 'activo') AS etapa_cobranza,
               EXISTS(
                   SELECT 1
                   FROM prestamos px
                   WHERE px.refinanciado_de = p.id
               ) AS fue_amortizado,
               (
                   SELECT MAX(pp.fecha)
                   FROM pagos_prestamo pp
                   WHERE pp.prestamo_id = p.id
               ) AS ultimo_pago,
               (
                   SELECT COUNT(*)
                   FROM pagos_prestamo pp
                   WHERE pp.prestamo_id = p.id
               ) AS cuotas_pagadas
        FROM prestamos p
        JOIN socios s ON s.id = p.socio_id
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        WHERE 1=1 {filtros}
        ORDER BY p.id DESC
        ''',
        params,
    ).fetchall()
    conn.close()

    cartera = []
    for row in rows:
        item = dict(row)
        if (item.get('estado') or '').lower() == 'amortizado':
            item['estado'] = 'pagado'
        alerta = _calcular_alerta_prestamo(item)
        item.update(alerta)
        cuota = float(item.get('cuota_mensual') or 0)
        saldo = float(item.get('saldo_pendiente') or 0)
        item['cuotas_pendientes'] = math.ceil(saldo / cuota) if cuota > 0 and saldo > 0 else 0
        item['total_cuotas'] = item.get('total_cuotas') or calcular_total_cuotas_prestamo(item.get('plazo_meses'), item.get('frecuencia'))
        cartera.append(item)

    return cartera

@app.route('/socios')
@login_required()
def socios():
    q = request.args.get('q', '')
    page = max(1, int(request.args.get('page', 1) or 1))
    per_page = min(100, max(10, int(request.args.get('per_page', 25) or 25)))
    offset = (page - 1) * per_page
    conn = get_db()
    if q:
        like = f'%{q}%'
        total = conn.execute(
            """SELECT COUNT(*) FROM socios
               WHERE nombre LIKE ? OR apellido LIKE ? OR codigo LIKE ? OR dpi LIKE ?
                  OR primer_nombre LIKE ? OR segundo_nombre LIKE ? OR tercer_nombre LIKE ?
                  OR primer_apellido LIKE ? OR segundo_apellido LIKE ?""",
            [like] * 9
        ).fetchone()[0]
        rows = conn.execute(
            """SELECT * FROM socios
               WHERE nombre LIKE ? OR apellido LIKE ? OR codigo LIKE ? OR dpi LIKE ?
                  OR primer_nombre LIKE ? OR segundo_nombre LIKE ? OR tercer_nombre LIKE ?
                  OR primer_apellido LIKE ? OR segundo_apellido LIKE ?
               ORDER BY id DESC
               LIMIT ? OFFSET ?""",
            [like] * 9 + [per_page, offset]
        ).fetchall()
    else:
        total = conn.execute("SELECT COUNT(*) FROM socios").fetchone()[0]
        rows = conn.execute("SELECT * FROM socios ORDER BY id DESC LIMIT ? OFFSET ?", [per_page, offset]).fetchall()
    conn.close()
    socios_lista = [preparar_datos_socio(row) for row in rows]
    total_pages = max(1, math.ceil(total / per_page))
    return render_template(
        'socios.html',
        socios=socios_lista,
        q=q,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
    )

@app.route('/socios/nuevo', methods=['GET','POST'])
def nuevo_socio():
    conn = get_db()
    codigo_sugerido = f"SOC-{conn.execute('SELECT COUNT(*) FROM socios').fetchone()[0] + 1:03d}"
    conn.close()
    if request.method == 'POST':
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) FROM socios").fetchone()[0]
        codigo = request.form.get('codigo', '').strip().upper() or f'SOC-{count+1:03d}'

        existente_codigo = conn.execute("SELECT id FROM socios WHERE codigo=?", (codigo,)).fetchone()
        if existente_codigo:
            flash('Ya existe un socio con ese código.', 'danger')
            conn.close()
            return render_template('nuevo_socio.html', codigo_sugerido=codigo_sugerido, beneficiarios=request.form)

        try:
            beneficiarios = parsear_beneficiarios_form(request.form)
            primer_nombre = request.form.get('primer_nombre', '').strip()
            segundo_nombre = request.form.get('segundo_nombre', '').strip()
            tercer_nombre = request.form.get('tercer_nombre', '').strip()
            primer_apellido = request.form.get('primer_apellido', '').strip()
            segundo_apellido = request.form.get('segundo_apellido', '').strip()
            estado_civil = request.form.get('estado_civil', 'Soltero').strip() or 'Soltero'
            apellido_casada = request.form.get('apellido_casada', '').strip() if estado_civil == 'Casado' else ''
            nombre = construir_nombre_completo(primer_nombre, segundo_nombre, tercer_nombre)
            apellido = construir_apellido_completo(primer_apellido, segundo_apellido)

            if not primer_nombre or not primer_apellido or not request.form.get('dpi', '').strip():
                raise ValueError('Código, primer nombre, primer apellido y DPI son obligatorios.')

            conn.execute(
                '''INSERT INTO socios (
                       codigo,nombre,primer_nombre,segundo_nombre,tercer_nombre,
                       apellido,primer_apellido,segundo_apellido,estado_civil,apellido_casada,
                       dpi,telefono,email,direccion,rol,fecha_ingreso,nit,beneficiario,
                       banco_nombre,banco_tipo_cuenta,banco_numero_cuenta,
                       frecuencia,cuota_ahorro,tipo_ahorro,finca
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    codigo, nombre, primer_nombre, segundo_nombre, tercer_nombre,
                    apellido, primer_apellido, segundo_apellido, estado_civil, apellido_casada,
                    request.form.get('dpi', '').strip(), request.form.get('telefono', '').strip(),
                    request.form.get('email', '').strip(), request.form.get('direccion', '').strip(),
                    'Asociado', date.today().isoformat(),
                    request.form.get('nit', '').strip(), resumen_beneficiarios(beneficiarios),
                    request.form.get('banco_nombre', '').strip(), request.form.get('banco_tipo_cuenta', '').strip(),
                    request.form.get('banco_numero_cuenta', '').strip(),
                    request.form.get('frecuencia', 'Quincenal').strip() or 'Quincenal',
                    float(request.form.get('cuota_ahorro', 0) or 0),
                    request.form.get('tipo_ahorro', 'ahorro corriente').strip() or 'ahorro corriente',
                    request.form.get('finca', '').strip(),
                )
            )
            socio_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            if beneficiarios:
                conn.executemany(
                    'INSERT INTO socio_beneficiarios (socio_id, nombre, parentesco, porcentaje) VALUES (?, ?, ?, ?)',
                    [(socio_id, item['nombre'], item['parentesco'], item['porcentaje']) for item in beneficiarios]
                )
            conn.commit()
            flash('Socio registrado exitosamente.', 'success')
            return redirect(url_for('socios'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()
    return render_template('nuevo_socio.html', codigo_sugerido=codigo_sugerido, beneficiarios=None)

@app.route('/roles')
@login_required(role='Administrador')
def roles():
    conn = get_db()
    rows = conn.execute("SELECT * FROM roles ORDER BY id DESC").fetchall()
    conn.close()
    return render_template('roles.html', roles=rows)

@app.route('/roles/nuevo', methods=['GET','POST'])
def nuevo_rol():
    if request.method == 'POST':
        conn = get_db()
        try:
            conn.execute("INSERT INTO roles (nombre,descripcion) VALUES (?,?)",
                (request.form['nombre'], request.form['descripcion']))
            conn.commit()
            flash('Rol creado exitosamente.', 'success')
            return redirect(url_for('roles'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()
    return render_template('nuevo_rol.html')

@app.route('/usuarios')
@login_required(role='Administrador')
def usuarios():
    conn = get_db()
    rows = conn.execute('''SELECT u.*, r.nombre as rol_nombre
                           FROM usuarios u LEFT JOIN roles r ON u.rol_id=r.id
                           ORDER BY u.id DESC''').fetchall()
    conn.close()
    return render_template('usuarios.html', usuarios=rows)

@app.route('/usuarios/nuevo', methods=['GET','POST'])
def nuevo_usuario():
    conn = get_db()
    roles = conn.execute("SELECT id,nombre FROM roles WHERE estado='activo'").fetchall()
    if request.method == 'POST':
        try:
            conn.execute("INSERT INTO usuarios (username,password,rol_id,fecha_creacion) VALUES (?,?,?,?)",
                (request.form['username'], generate_password_hash(request.form['password']), request.form.get('rol_id'), date.today().isoformat()))
            conn.commit()
            flash('Usuario creado exitosamente.', 'success')
            return redirect(url_for('usuarios'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()
    conn.close()
    return render_template('nuevo_usuario.html', roles=roles)

@app.route('/socios/<int:sid>')
@login_required()
def detalle_socio(sid):
    conn = get_db()
    socio = conn.execute("SELECT * FROM socios WHERE id=?", [sid]).fetchone()
    cuentas = conn.execute("SELECT * FROM cuentas WHERE socio_id=?", [sid]).fetchall()
    prestamos = conn.execute('''
        SELECT p.*,
               s.frecuencia,
               pc.nombre AS categoria_nombre,
               COALESCE(pp.pagos_realizados, 0) AS pagos_realizados,
               COALESCE(pp.monto_pagado, 0) AS monto_pagado,
               CASE
                   WHEN p.estado = 'pendiente' THEN 'Pendiente de aprobacion'
                   WHEN p.estado = 'pagado' OR COALESCE(p.saldo_pendiente, 0) <= 0 THEN 'Cancelado'
                   WHEN p.estado = 'aprobado' AND COALESCE(pp.pagos_realizados, 0) = 0 THEN 'Activo sin pagos'
                   WHEN p.estado = 'aprobado' THEN 'Activo con pagos'
                   ELSE 'En revision'
               END AS estado_cuenta
        FROM prestamos p
        JOIN socios s ON s.id = p.socio_id
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        LEFT JOIN (
            SELECT prestamo_id,
                   COUNT(*) AS pagos_realizados,
                   SUM(monto) AS monto_pagado
            FROM pagos_prestamo
            GROUP BY prestamo_id
        ) pp ON p.id = pp.prestamo_id
        WHERE p.socio_id=?
        ORDER BY p.id DESC
    ''', [sid]).fetchall()

    pagos_prestamos = conn.execute('''
        SELECT pp.*, p.numero AS numero_prestamo
        FROM pagos_prestamo pp
        JOIN prestamos p ON pp.prestamo_id = p.id
        WHERE p.socio_id=?
        ORDER BY date(pp.fecha) DESC, pp.id DESC
    ''', [sid]).fetchall()

    beneficiarios = obtener_beneficiarios_socio(conn, sid)

    conn.close()

    socio = preparar_datos_socio(socio)

    prestamos_normalizados = []
    for prestamo in prestamos:
        item = dict(prestamo)
        item['total_cuotas'] = calcular_total_cuotas_prestamo(item.get('plazo_meses'), item.get('frecuencia'))
        prestamos_normalizados.append(item)

    return render_template(
        'detalle_socio.html',
        socio=socio,
        beneficiarios=beneficiarios,
        cuentas=cuentas,
        prestamos=prestamos_normalizados,
        pagos_prestamos=pagos_prestamos
    )

@app.route('/socios/<int:sid>/editar', methods=['GET', 'POST'])
@login_required(role=('Administrador', 'Operador'))
def editar_socio(sid):
    conn = get_db()
    socio = conn.execute("SELECT * FROM socios WHERE id=?", [sid]).fetchone()
    if not socio:
        conn.close()
        flash('Socio no encontrado.', 'danger')
        return redirect(url_for('socios'))

    # Convertir a diccionario para acceso seguro
    socio_dict = preparar_datos_socio(socio)
    beneficiarios_existentes = obtener_beneficiarios_socio(conn, sid)

    if request.method == 'POST':
        codigo = request.form.get('codigo', '').strip().upper()
        primer_nombre = request.form.get('primer_nombre', '').strip()
        segundo_nombre = request.form.get('segundo_nombre', '').strip()
        tercer_nombre = request.form.get('tercer_nombre', '').strip()
        primer_apellido = request.form.get('primer_apellido', '').strip()
        segundo_apellido = request.form.get('segundo_apellido', '').strip()
        nombre = construir_nombre_completo(primer_nombre, segundo_nombre, tercer_nombre)
        apellido = construir_apellido_completo(primer_apellido, segundo_apellido)
        dpi = request.form.get('dpi', '').strip()
        telefono = request.form.get('telefono', '').strip()
        email = request.form.get('email', '').strip()
        direccion = request.form.get('direccion', '').strip()
        estado_civil = request.form.get('estado_civil', socio_dict.get('estado_civil') or 'Soltero').strip() or 'Soltero'
        apellido_casada = request.form.get('apellido_casada', '').strip() if estado_civil == 'Casado' else ''
        frecuencia = request.form.get('frecuencia', socio_dict.get('frecuencia') or 'Quincenal')
        cuota_ahorro = float(request.form.get('cuota_ahorro', socio_dict.get('cuota_ahorro') or 0) or 0)
        tipo_ahorro = request.form.get('tipo_ahorro', socio_dict.get('tipo_ahorro') or 'ahorro corriente')
        nit = request.form.get('nit', '').strip()
        finca = request.form.get('finca', '').strip()
        banco_nombre = request.form.get('banco_nombre', '').strip()
        banco_tipo_cuenta = request.form.get('banco_tipo_cuenta', '').strip()
        banco_numero_cuenta = request.form.get('banco_numero_cuenta', '').strip()
        foto = request.files.get('foto')

        if not codigo or not primer_nombre or not primer_apellido or not dpi:
            flash('Código, primer nombre, primer apellido y DPI son obligatorios.', 'danger')
            conn.close()
            return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)

        existente_codigo = conn.execute("SELECT id FROM socios WHERE codigo=? AND id<>?", (codigo, sid)).fetchone()
        if existente_codigo:
            flash('Ya existe otro socio con ese código.', 'danger')
            conn.close()
            return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)

        existente = conn.execute("SELECT id FROM socios WHERE dpi=? AND id<>?", (dpi, sid)).fetchone()
        if existente:
            flash('Ya existe otro socio con ese DPI.', 'danger')
            conn.close()
            return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)

        try:
            beneficiarios = parsear_beneficiarios_form(request.form)
            datos_previos = dict(socio)
            ruta_foto = socio_dict.get('foto')

            if foto and foto.filename:
                if not allowed_socio_image(foto.filename):
                    conn.close()
                    flash('Formato de foto no permitido. Use PNG, JPG, JPEG o WEBP.', 'warning')
                    return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)
                ruta_foto = procesar_foto_socio(foto, sid)

            conn.execute('''
                UPDATE socios SET codigo=?, nombre=?, primer_nombre=?, segundo_nombre=?, tercer_nombre=?,
                                  apellido=?, primer_apellido=?, segundo_apellido=?, estado_civil=?, apellido_casada=?,
                                  dpi=?, telefono=?, email=?, direccion=?, rol=?, frecuencia=?, cuota_ahorro=?, tipo_ahorro=?,
                                  nit=?, beneficiario=?, finca=?, banco_nombre=?, banco_tipo_cuenta=?, banco_numero_cuenta=?, foto=?
                WHERE id=?
            ''', (
                  codigo, nombre, primer_nombre, segundo_nombre, tercer_nombre,
                  apellido, primer_apellido, segundo_apellido, estado_civil, apellido_casada,
                                    dpi, telefono, email, direccion, 'Asociado',
                frecuencia, cuota_ahorro, tipo_ahorro, nit, resumen_beneficiarios(beneficiarios), finca,
                  banco_nombre, banco_tipo_cuenta, banco_numero_cuenta, ruta_foto, sid))
            conn.execute('DELETE FROM socio_beneficiarios WHERE socio_id=?', [sid])
            if beneficiarios:
                conn.executemany(
                    'INSERT INTO socio_beneficiarios (socio_id, nombre, parentesco, porcentaje) VALUES (?, ?, ?, ?)',
                    [(sid, item['nombre'], item['parentesco'], item['porcentaje']) for item in beneficiarios]
                )
            conn.commit()

            if (foto and foto.filename and datos_previos.get('foto') and
                    datos_previos.get('foto').startswith('uploads/socios/') and
                    datos_previos.get('foto') != ruta_foto):
                try:
                    os.remove(os.path.join(os.path.dirname(__file__), 'static', datos_previos.get('foto')))
                except OSError:
                    pass

            datos_nuevos = {
                'codigo': codigo,
                'nombre': nombre,
                'apellido': apellido,
                'primer_nombre': primer_nombre,
                'segundo_nombre': segundo_nombre,
                'tercer_nombre': tercer_nombre,
                'primer_apellido': primer_apellido,
                'segundo_apellido': segundo_apellido,
                'estado_civil': estado_civil,
                'apellido_casada': apellido_casada,
                'dpi': dpi,
                'telefono': telefono,
                'email': email,
                'direccion': direccion,
                'rol': 'Asociado',
                'frecuencia': frecuencia,
                'cuota_ahorro': cuota_ahorro,
                'tipo_ahorro': tipo_ahorro,
                'nit': nit,
                'beneficiario': resumen_beneficiarios(beneficiarios),
                'finca': finca,
                'banco_nombre': banco_nombre,
                'banco_tipo_cuenta': banco_tipo_cuenta,
                'banco_numero_cuenta': banco_numero_cuenta,
                'beneficiarios': beneficiarios,
                'foto': ruta_foto
            }
            log_auditoria_socio(sid, session.get('user_id'), 'editar', 
                                json.dumps(datos_previos), 
                                json.dumps(datos_nuevos)
            )

            flash('Socio actualizado correctamente.', 'success')
            return redirect(url_for('detalle_socio', sid=sid))
        except Exception as e:
            flash(f'Error actualizando socio: {e}', 'danger')
        finally:
            conn.close()

    conn.close()
    return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)

@app.route('/socios/<int:sid>/activar', methods=['POST'])
@login_required(role=('Administrador','Operador'))
def activar_socio(sid):
    conn = get_db()
    conn.execute("UPDATE socios SET estado='activo' WHERE id=?", [sid])
    conn.commit()
    conn.close()
    log_auditoria_socio(sid, session.get('user_id'), 'activar', None, 'activo')
    flash('Socio activado.', 'success')
    return redirect(url_for('detalle_socio', sid=sid))

@app.route('/socios/<int:sid>/inactivar', methods=['POST'])
@login_required(role=('Administrador','Operador'))
def inactivar_socio(sid):
    conn = get_db()
    conn.execute("UPDATE socios SET estado='inactivo' WHERE id=?", [sid])
    conn.commit()
    conn.close()
    log_auditoria_socio(sid, session.get('user_id'), 'inactivar', None, 'inactivo')
    flash('Socio inactivado.', 'warning')
    return redirect(url_for('detalle_socio', sid=sid))

# ── CONFIGURACIONES ───────────────────────────────────────────────────────────
@app.route('/configuraciones')
@login_required(role=('Administrador',))
def configuraciones():
    conn = get_db()
    ensure_required_configurations(conn)
    ensure_system_settings(conn)
    ensure_default_prestamo_categories(conn)
    configs = conn.execute(
        """SELECT * FROM configuraciones
           WHERE tipo IN ('ahorro_corriente', 'ahorro_plazo_fijo', 'ahorro_aportacion', 'prestamo')
           ORDER BY CASE tipo
               WHEN 'ahorro_corriente' THEN 1
               WHEN 'ahorro_plazo_fijo' THEN 2
               WHEN 'ahorro_aportacion' THEN 3
               WHEN 'prestamo' THEN 4
               ELSE 99
           END"""
    ).fetchall()
    categorias_prestamo = conn.execute(
        """SELECT * FROM prestamo_categorias
           WHERE estado='activo'
           ORDER BY nombre"""
    ).fetchall()
    cooperativa_nombre = get_system_setting(conn, 'cooperativa_nombre', DEFAULT_COOPERATIVA_NOMBRE)
    cooperativa_foto = get_system_setting(conn, 'cooperativa_foto', '')
    prestamo_finiquito_texto = get_system_setting(
        conn,
        'prestamo_finiquito_texto',
        SYSTEM_SETTINGS_DEFAULTS['prestamo_finiquito_texto']
    )
    conn.close()
    return render_template(
        'configuraciones.html',
        configuraciones=configs,
        categorias_prestamo=categorias_prestamo,
        cooperativa_nombre=cooperativa_nombre,
        cooperativa_foto=cooperativa_foto,
        prestamo_finiquito_texto=prestamo_finiquito_texto,
    )

@app.route('/configuraciones/actualizar', methods=['POST'])
@login_required(role=('Administrador',))
def actualizar_configuraciones():
    conn = get_db()
    try:
        hoy = date.today().isoformat()
        ensure_default_prestamo_categories(conn)

        nombre_cooperativa = (request.form.get('cooperativa_nombre') or DEFAULT_COOPERATIVA_NOMBRE).strip()
        if not nombre_cooperativa:
            raise ValueError('El nombre de la cooperativa es obligatorio.')
        set_system_setting(conn, 'cooperativa_nombre', nombre_cooperativa, session.get('username'))
        set_system_setting(
            conn,
            'prestamo_finiquito_texto',
            (request.form.get('prestamo_finiquito_texto') or SYSTEM_SETTINGS_DEFAULTS['prestamo_finiquito_texto']).strip(),
            session.get('username')
        )

        foto_cooperativa = request.files.get('cooperativa_foto')
        if foto_cooperativa and foto_cooperativa.filename:
            if not allowed_system_image(foto_cooperativa.filename):
                raise ValueError('La foto de la cooperativa debe ser PNG, JPG, JPEG o WEBP.')

            foto_anterior = get_system_setting(conn, 'cooperativa_foto', '')
            nueva_foto = procesar_foto_cooperativa(foto_cooperativa)
            set_system_setting(conn, 'cooperativa_foto', nueva_foto, session.get('username'))

            if foto_anterior:
                ruta_anterior = os.path.join(app.static_folder, foto_anterior)
                if os.path.exists(ruta_anterior):
                    try:
                        os.remove(ruta_anterior)
                    except OSError:
                        pass

        for tipo, tasa_default, descripcion in REQUIRED_CONFIGURACIONES:
            tasa = float(request.form.get(tipo, tasa_default))
            if tasa < 0:
                raise ValueError(f'La tasa para {get_config_label(tipo)} no puede ser negativa.')

            conn.execute(
                """INSERT INTO configuraciones
                   (tipo, tasa_interes, descripcion, fecha_actualizacion, usuario_actualizacion)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(tipo) DO UPDATE SET
                       tasa_interes=excluded.tasa_interes,
                       descripcion=excluded.descripcion,
                       fecha_actualizacion=excluded.fecha_actualizacion,
                       usuario_actualizacion=excluded.usuario_actualizacion""",
                (tipo, tasa, descripcion, hoy, session.get('username'))
            )

        categoria_ids = request.form.getlist('categoria_id[]')
        categoria_nombres = request.form.getlist('categoria_nombre[]')
        categoria_descripciones = request.form.getlist('categoria_descripcion[]')

        for categoria_id, nombre, descripcion in zip(categoria_ids, categoria_nombres, categoria_descripciones):
            nombre = (nombre or '').strip()
            descripcion = (descripcion or '').strip()
            if not nombre:
                continue

            existente = conn.execute(
                "SELECT id FROM prestamo_categorias WHERE lower(nombre)=lower(?) AND id != COALESCE(?, 0)",
                (nombre, categoria_id or None)
            ).fetchone()
            if existente:
                raise ValueError(f'La categoria de prestamo "{nombre}" ya existe.')

            if categoria_id:
                conn.execute(
                    """UPDATE prestamo_categorias
                       SET nombre=?, descripcion=?, fecha_actualizacion=?, usuario_actualizacion=?
                       WHERE id=?""",
                    (nombre, descripcion, hoy, session.get('username'), categoria_id)
                )
            else:
                conn.execute(
                    """INSERT INTO prestamo_categorias
                       (nombre, descripcion, estado, fecha_actualizacion, usuario_actualizacion)
                       VALUES (?, ?, 'activo', ?, ?)""",
                    (nombre, descripcion, hoy, session.get('username'))
                )
        
        conn.commit()
        flash('Configuraciones actualizadas correctamente.', 'success')
    except Exception as e:
        flash(f'Error actualizando configuraciones: {e}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('configuraciones'))

# ── CUENTAS ───────────────────────────────────────────────────────────────────
@app.route('/cuentas')
def cuentas():
    conn = get_db()
    rows = conn.execute('''SELECT c.*, s.nombre||' '||s.apellido as socio
                           FROM cuentas c JOIN socios s ON c.socio_id=s.id
                           ORDER BY c.id DESC''').fetchall()
    conn.close()
    return render_template('cuentas.html', cuentas=rows)

@app.route('/cuentas/nueva', methods=['GET','POST'])
def nueva_cuenta():
    conn = get_db()
    socios = conn.execute(
        """
        SELECT id, codigo, nombre, apellido
        FROM socios
        WHERE estado='activo'
        ORDER BY codigo, nombre, apellido
        """
    ).fetchall()

    tipos_ahorro = conn.execute(
        """
        SELECT tipo, tasa_interes
        FROM configuraciones
        WHERE tipo IN ('ahorro_aportacion', 'ahorro_corriente', 'ahorro_plazo_fijo')
        ORDER BY CASE tipo
            WHEN 'ahorro_aportacion' THEN 1
            WHEN 'ahorro_corriente' THEN 2
            WHEN 'ahorro_plazo_fijo' THEN 3
            ELSE 99
        END
        """
    ).fetchall()

    tasas_por_tipo = {row['tipo']: float(row['tasa_interes'] or 0) for row in tipos_ahorro}
    selected_socio_id = request.form.get('socio_id', '').strip()
    selected_producto = request.form.get('producto_ahorro', 'ahorro_aportacion').strip() or 'ahorro_aportacion'

    if request.method == 'POST':
        try:
            if not selected_socio_id:
                raise ValueError('Debe seleccionar un asociado activo.')

            socio = conn.execute(
                "SELECT id, estado FROM socios WHERE id=?",
                [selected_socio_id]
            ).fetchone()
            if not socio or (socio['estado'] or '').lower() != 'activo':
                raise ValueError('Solo se permite abrir cuentas a asociados activos.')

            productos_validos = {'ahorro_aportacion', 'ahorro_corriente', 'ahorro_plazo_fijo'}
            if selected_producto not in productos_validos:
                raise ValueError('Debe seleccionar un tipo de cuenta válido.')

            cuenta_existente = conn.execute(
                """
                SELECT id, numero
                FROM cuentas
                WHERE socio_id=?
                  AND tipo='ahorro'
                  AND COALESCE(producto_ahorro, 'ahorro_corriente')=?
                LIMIT 1
                """,
                [selected_socio_id, selected_producto]
            ).fetchone()
            if cuenta_existente:
                raise ValueError('El asociado ya tiene una cuenta de ese tipo.')

            count = conn.execute("SELECT COUNT(*) FROM cuentas").fetchone()[0]
            prefijos = {
                'ahorro_aportacion': 'APR',
                'ahorro_corriente': 'COR',
                'ahorro_plazo_fijo': 'PLF',
            }
            numero = f"{prefijos[selected_producto]}-{count+1:04d}"
            tasa = tasas_por_tipo.get(selected_producto, 0)

            conn.execute(
                """
                INSERT INTO cuentas (numero, socio_id, tipo, producto_ahorro, saldo, tasa_interes, fecha_apertura)
                VALUES (?, ?, 'ahorro', ?, 0, ?, ?)
                """,
                (numero, selected_socio_id, selected_producto, tasa, date.today().isoformat())
            )
            conn.commit()
            flash('Cuenta creada exitosamente.', 'success')
            return redirect(url_for('cuentas'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')

    conn.close()
    return render_template(
        'nueva_cuenta.html',
        socios=socios,
        tipos_ahorro=tipos_ahorro,
        selected_socio_id=selected_socio_id,
        selected_producto=selected_producto,
    )

@app.route('/ahorro/aplicar-intereses', methods=['POST'])
@login_required(role='Administrador')
def aplicar_intereses():
    conn = get_db()
    cursor = conn.cursor()
    hoy = date.today()
    # Obtenemos cuentas de ahorro activas con saldo y tasa
    cuentas = cursor.execute("SELECT id, numero, saldo, tasa_interes FROM cuentas WHERE tipo='ahorro' AND estado='activa' AND saldo > 0 AND tasa_interes > 0").fetchall()
    
    procesados = 0
    total_pagado = 0
    
    for c in cuentas:
        # Cálculo: (Saldo * (Tasa/100)) / 12 meses
        monto_interes = round(c['saldo'] * (c['tasa_interes'] / 100 / 12), 2)
        if monto_interes > 0:
            nuevo_saldo = round(c['saldo'] + monto_interes, 2)
            cursor.execute("UPDATE cuentas SET saldo=? WHERE id=?", (nuevo_saldo, c['id']))
            cursor.execute("""INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha)
                           VALUES (?, 'interes', ?, ?, ?, ?)""", 
                           (c['id'], monto_interes, nuevo_saldo, f"Capitalización Interés - {hoy.strftime('%B %Y')}", datetime.now().isoformat()))
            procesados += 1
            total_pagado += monto_interes
            
    conn.commit()
    conn.close()
    flash(f'Proceso finalizado: Se aplicaron intereses a {procesados} cuentas por un total de Q{total_pagado:,.2f}', 'success')
    return redirect(url_for('configuracion_ahorro'))

@app.route('/cuentas/<int:cid>')
def detalle_cuenta(cid):
    conn = get_db()
    cuenta = conn.execute('''SELECT c.*, s.nombre||' '||s.apellido as socio
                             FROM cuentas c JOIN socios s ON c.socio_id=s.id
                             WHERE c.id=?''', [cid]).fetchone()
    if not cuenta:
        conn.close()
        flash('Cuenta no encontrada.', 'danger')
        return redirect(url_for('cuentas'))
    txns = conn.execute("SELECT * FROM transacciones WHERE cuenta_id=? ORDER BY id DESC", [cid]).fetchall()
    conn.close()
    return render_template('detalle_cuenta.html', cuenta=cuenta, transacciones=txns)

@app.route('/cuentas/<int:cid>/transaccion', methods=['POST'])
def hacer_transaccion(cid):
    conn = get_db()
    cuenta = conn.execute("SELECT * FROM cuentas WHERE id=?", [cid]).fetchone()
    socio = conn.execute("SELECT id FROM socios WHERE id=?", [cuenta['socio_id']]).fetchone()
    tipo = request.form['tipo']
    monto = float(request.form['monto'])
    desc = request.form.get('descripcion', tipo.capitalize())

    if periodo_cerrado('ahorro', datetime.now().isoformat()):
        conn.close()
        flash('El periodo de ahorro está cerrado. No se permiten movimientos en la fecha actual.', 'warning')
        return redirect(url_for('detalle_cuenta', cid=cid))
    
    # Validar frecuencia para depósitos de ahorro
    if tipo == 'deposito':
        if not validar_pago_frecuencia(socio['id'], 'ahorro'):
            mensaje = obtener_mensaje_validacion_frecuencia(socio['id'], 'ahorro')
            flash(f'No se puede realizar el depósito. {mensaje}', 'warning')
            conn.close()
            return redirect(url_for('detalle_cuenta', cid=cid))
    
    try:
        if tipo == 'retiro' and monto > cuenta['saldo']:
            flash('Saldo insuficiente.', 'danger')
        else:
            nuevo_saldo = cuenta['saldo'] + monto if tipo == 'deposito' else cuenta['saldo'] - monto
            conn.execute("UPDATE cuentas SET saldo=? WHERE id=?", [nuevo_saldo, cid])
            conn.execute("INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)",
                (cid, tipo, monto, nuevo_saldo, desc, datetime.now().isoformat()))
            conn.commit()
            log_auditoria_evento(
                modulo='ahorro',
                entidad='transaccion',
                entidad_id=cid,
                accion='crear',
                descripcion=f'Transaccion {tipo} registrada en cuenta {cuenta["numero"]}',
                datos={'monto': monto, 'saldo_despues': nuevo_saldo}
            )
            flash('Transacción realizada.', 'success')
    except Exception as e:
        flash(f'Error: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('detalle_cuenta', cid=cid))

# ── PRÉSTAMOS ─────────────────────────────────────────────────────────────────
@app.route('/prestamos')
def prestamos():
    rows = _obtener_cartera_con_alertas()
    q = request.args.get('q', '').strip()
    estado_filtro = request.args.get('estado', '').strip().lower()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()
    ordenar_por = request.args.get('ordenar_por', 'fecha_solicitud').strip().lower()
    direccion = request.args.get('direccion', 'desc').strip().lower()
    vista = request.args.get('vista', 'activos').strip().lower()
    vistas_validas = {'activos', 'pagados', 'pendientes'}
    if vista not in vistas_validas:
        vista = 'activos'
    if direccion not in ('asc', 'desc'):
        direccion = 'desc'

    if q:
        q_lower = q.lower()
        filtrados = []
        for item in rows:
            texto_busqueda = ' '.join([
                str(item.get('numero') or ''),
                str(item.get('nombre_socio') or ''),
                str(item.get('socio_codigo') or ''),
                str(item.get('categoria_nombre') or ''),
                str(item.get('estado') or ''),
                str(item.get('desembolso_referencia') or ''),
            ]).lower()
            if q_lower in texto_busqueda:
                filtrados.append(item)
        rows = filtrados

    if estado_filtro:
        rows = [item for item in rows if (item.get('estado') or '').lower() == estado_filtro]

    if fecha_desde:
        rows = [
            item for item in rows
            if (item.get('fecha_solicitud') or '')[:10] >= fecha_desde
        ]

    if fecha_hasta:
        rows = [
            item for item in rows
            if (item.get('fecha_solicitud') or '')[:10] <= fecha_hasta
        ]

    conteos = {
        'activos': sum(1 for item in rows if item.get('estado') == 'aprobado' and float(item.get('saldo_pendiente') or 0) > 0),
        'pagados': sum(1 for item in rows if item.get('estado') == 'pagado' or (item.get('estado') == 'aprobado' and float(item.get('saldo_pendiente') or 0) <= 0)),
        'pendientes': sum(1 for item in rows if item.get('estado') == 'pendiente'),
    }

    if vista == 'activos':
        rows = [item for item in rows if item.get('estado') == 'aprobado' and float(item.get('saldo_pendiente') or 0) > 0]
        subtitulo = 'Préstamos aprobados con saldo pendiente'
    elif vista == 'pagados':
        rows = [item for item in rows if item.get('estado') == 'pagado' or (item.get('estado') == 'aprobado' and float(item.get('saldo_pendiente') or 0) <= 0)]
        subtitulo = 'Préstamos cancelados o liquidados'
    else:
        rows = [item for item in rows if item.get('estado') == 'pendiente']
        subtitulo = 'Solicitudes pendientes de aprobación'

    reverse = direccion == 'desc'
    if ordenar_por == 'saldo_pendiente':
        rows = sorted(rows, key=lambda item: float(item.get('saldo_pendiente') or 0), reverse=reverse)
    elif ordenar_por == 'cuota_mensual':
        rows = sorted(rows, key=lambda item: float(item.get('cuota_mensual') or 0), reverse=reverse)
    elif ordenar_por == 'monto_solicitado':
        rows = sorted(rows, key=lambda item: float(item.get('monto_solicitado') or 0), reverse=reverse)
    elif ordenar_por == 'nombre_socio':
        rows = sorted(rows, key=lambda item: str(item.get('nombre_socio') or '').lower(), reverse=reverse)
    else:
        ordenar_por = 'fecha_solicitud'
        rows = sorted(rows, key=lambda item: str(item.get('fecha_solicitud') or ''), reverse=reverse)

    page = max(1, int(request.args.get('page', 1) or 1))
    per_page = min(100, max(10, int(request.args.get('per_page', 25) or 25)))
    total = len(rows)
    total_pages = max(1, math.ceil(total / per_page))
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    rows = rows[start_idx:end_idx]

    return render_template(
        'prestamos.html',
        prestamos=rows,
        vista=vista,
        conteos=conteos,
        subtitulo=subtitulo,
        q=q,
        estado_filtro=estado_filtro,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        ordenar_por=ordenar_por,
        direccion=direccion,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
    )


@app.route('/prestamos/<int:pid>')
@login_required()
def detalle_prestamo(pid):
    conn = get_db()
    prestamo = conn.execute(
        '''
        SELECT p.*, s.codigo AS socio_codigo, s.nombre || ' ' || s.apellido AS nombre_socio,
               s.frecuencia, pc.nombre AS categoria_nombre,
               COALESCE(pp.cuotas_pagadas, 0) AS cuotas_pagadas,
               COALESCE(pp.monto_pagado, 0) AS monto_pagado,
               pp.ultimo_pago
        FROM prestamos p
        JOIN socios s ON s.id = p.socio_id
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        LEFT JOIN (
            SELECT prestamo_id,
                   COUNT(*) AS cuotas_pagadas,
                   SUM(monto) AS monto_pagado,
                   MAX(fecha) AS ultimo_pago
            FROM pagos_prestamo
            GROUP BY prestamo_id
        ) pp ON p.id = pp.prestamo_id
        WHERE p.id=?
        ''',
        [pid]
    ).fetchone()

    if not prestamo:
        conn.close()
        flash('Prestamo no encontrado.', 'danger')
        return redirect(url_for('prestamos'))

    prestamo = dict(prestamo)
    if (prestamo.get('estado') or '').lower() == 'amortizado':
        prestamo['estado'] = 'pagado'

    pagos = conn.execute(
        '''
        SELECT *
        FROM pagos_prestamo
        WHERE prestamo_id=?
        ORDER BY date(fecha) DESC, id DESC
        ''',
        [pid]
    ).fetchall()
    pagos = [dict(row) for row in pagos]

    # Si este préstamo fue amortizado por uno o más préstamos nuevos,
    # se muestran como movimientos dentro del historial de cuotas pagadas.
    prestamos_pagadores = conn.execute(
        '''
        SELECT numero,
               fecha_aprobacion,
               fecha_solicitud,
               COALESCE(monto_amortizado, 0) AS monto_amortizado,
               COALESCE(monto_aprobado, monto_solicitado, 0) AS total_prestamo
        FROM prestamos
        WHERE refinanciado_de=?
          AND COALESCE(monto_amortizado, 0) > 0
        ORDER BY date(COALESCE(fecha_aprobacion, fecha_solicitud)) DESC, id DESC
        ''',
        [pid]
    ).fetchall()
    for prestamo_pagador in prestamos_pagadores:
        pagos.append({
            'id': None,
            'fecha': prestamo_pagador['fecha_aprobacion'] or prestamo_pagador['fecha_solicitud'],
            'monto': float(prestamo_pagador['monto_amortizado'] or 0),
            'capital': float(prestamo_pagador['monto_amortizado'] or 0),
            'interes': 0.0,
            'saldo_restante': float(prestamo.get('saldo_pendiente') or 0),
            'boleta_deposito': 'Amortización',
            'numero_comprobante': None,
            'es_amortizacion': True,
            'prestamo_origen_numero': prestamo_pagador['numero'] or '—',
            'total_prestamo_origen': float(prestamo_pagador['total_prestamo'] or 0),
        })

    if prestamo.get('refinanciado_de') and float(prestamo.get('monto_amortizado') or 0) > 0:
        prestamo_amortizado = conn.execute(
            '''
            SELECT numero,
                   COALESCE(monto_aprobado, monto_solicitado, 0) AS total_prestamo
            FROM prestamos
            WHERE id=?
            ''',
            [prestamo['refinanciado_de']]
        ).fetchone()

        pagos.insert(0, {
            'id': None,
            'fecha': prestamo.get('fecha_aprobacion') or prestamo.get('fecha_solicitud'),
            'monto': float(prestamo.get('monto_amortizado') or 0),
            'capital': float(prestamo.get('monto_amortizado') or 0),
            'interes': 0.0,
            'saldo_restante': float(prestamo.get('saldo_pendiente') or 0),
            'boleta_deposito': 'Amortización',
            'numero_comprobante': None,
            'es_amortizacion': True,
            'prestamo_amortizado_numero': prestamo_amortizado['numero'] if prestamo_amortizado else '—',
            'total_prestamo_amortizado': float(prestamo_amortizado['total_prestamo'] or 0) if prestamo_amortizado else 0.0,
        })

    pagos = sorted(
        pagos,
        key=lambda item: (str(item.get('fecha') or ''), int(item.get('id') or 0)),
        reverse=True,
    )

    calendario = conn.execute(
        '''
        SELECT numero_cuota, fecha_programada, monto_programado, estado
        FROM prestamo_calendario_pagos
        WHERE prestamo_id=?
        ORDER BY numero_cuota
        ''',
        [pid]
    ).fetchall()

    conn.close()

    prestamo_dict = dict(prestamo)
    prestamo_dict['total_cuotas'] = calcular_total_cuotas_prestamo(
        prestamo_dict.get('plazo_meses'),
        prestamo_dict.get('frecuencia')
    )
    if calendario:
        prestamo_dict['cuotas_pagadas_calendario'] = sum(1 for cuota in calendario if (cuota['estado'] or '').lower() == 'pagado')
    else:
        prestamo_dict['cuotas_pagadas_calendario'] = prestamo_dict.get('cuotas_pagadas') or 0

    return render_template(
        'detalle_prestamo.html',
        prestamo=prestamo_dict,
        pagos=pagos,
        calendario=calendario,
    )

def _cargar_contexto_nuevo_prestamo(conn, socio_id_seleccionado=''):
    ensure_required_configurations(conn)
    ensure_default_prestamo_categories(conn)

    socios = conn.execute(
        "SELECT id, codigo, nombre, apellido, dpi, frecuencia, banco_nombre, banco_tipo_cuenta, banco_numero_cuenta FROM socios WHERE estado='activo' ORDER BY codigo, nombre, apellido"
    ).fetchall()
    configs = conn.execute("SELECT * FROM configuraciones WHERE tipo='prestamo'").fetchall()
    categorias_prestamo = conn.execute(
        "SELECT id, nombre, descripcion FROM prestamo_categorias WHERE estado='activo' ORDER BY nombre"
    ).fetchall()
    prestamos_rows = conn.execute(
        '''
        SELECT p.id,
               p.socio_id,
               p.numero,
               p.estado,
               p.fecha_solicitud,
               COALESCE(p.saldo_pendiente, p.monto_aprobado, p.monto_solicitado, 0) AS saldo_vigente,
               p.monto_solicitado,
               pc.nombre AS categoria_nombre
        FROM prestamos p
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        WHERE p.estado IN ('pendiente', 'aprobado')
        ORDER BY p.socio_id, date(p.fecha_solicitud) DESC, p.id DESC
        '''
    ).fetchall()

    prestamos_vigentes_por_socio = {}
    for row in prestamos_rows:
        socio_key = str(row['socio_id'])
        prestamos_vigentes_por_socio.setdefault(socio_key, []).append({
            'id': row['id'],
            'numero': row['numero'],
            'estado': row['estado'],
            'fecha_solicitud': row['fecha_solicitud'],
            'monto_solicitado': float(row['monto_solicitado'] or 0),
            'saldo_vigente': float(row['saldo_vigente'] or 0),
            'categoria_nombre': row['categoria_nombre'] or 'Sin categoria',
        })

    return {
        'socios': socios,
        'configuraciones': configs,
        'categorias_prestamo': categorias_prestamo,
        'prestamos_vigentes_por_socio': prestamos_vigentes_por_socio,
        'socio_id_seleccionado': str(socio_id_seleccionado or ''),
    }


@app.route('/prestamos/nuevo', methods=['GET', 'POST'])
def nuevo_prestamo():
    conn = get_db()

    if request.method == 'POST':
        socio_id = request.form.get('socio_id', '').strip()
        categoria_id = request.form.get('categoria_id', '').strip()
        prestamo_a_amortizar_id = request.form.get('prestamo_a_amortizar_id', '').strip()
        forma_desembolso = request.form.get('forma_desembolso', 'cheque').strip() or 'cheque'

        if not socio_id:
            conn.close()
            flash('Debe seleccionar un asociado válido.', 'danger')
            return redirect(url_for('nuevo_prestamo'))

        socio = conn.execute(
            "SELECT id, frecuencia, banco_nombre, banco_tipo_cuenta, banco_numero_cuenta FROM socios WHERE id=?",
            [socio_id]
        ).fetchone()
        if not socio:
            conn.close()
            flash('Debe seleccionar un asociado válido.', 'danger')
            return redirect(url_for('nuevo_prestamo'))

        categoria = None
        if categoria_id:
            categoria = conn.execute(
                "SELECT id FROM prestamo_categorias WHERE id=? AND estado='activo'",
                [categoria_id]
            ).fetchone()
        if not categoria:
            conn.close()
            flash('Debe seleccionar una categoria de prestamo válida.', 'danger')
            return redirect(url_for('nuevo_prestamo', socio_id=socio_id))

        try:
            monto = float(request.form.get('monto', 0) or 0)
            tasa = float(request.form.get('tasa', 0) or 0)
            plazo = int(request.form.get('plazo', 0) or 0)
        except (TypeError, ValueError):
            conn.close()
            flash('Los datos del préstamo no son válidos.', 'danger')
            return redirect(url_for('nuevo_prestamo', socio_id=socio_id))

        if monto <= 0 or tasa <= 0 or plazo <= 0:
            conn.close()
            flash('Debe ingresar monto, tasa y plazo válidos.', 'danger')
            return redirect(url_for('nuevo_prestamo', socio_id=socio_id))

        banco_tipo = ''
        banco_numero = ''
        if forma_desembolso == 'deposito':
            banco_tipo = (socio['banco_tipo_cuenta'] or '').strip()
            banco_numero = (socio['banco_numero_cuenta'] or '').strip()
            banco_nombre = (socio['banco_nombre'] or '').strip()
            if not banco_nombre or not banco_tipo or not banco_numero:
                conn.close()
                flash('El asociado no tiene la información bancaria completa para desembolso por deposito.', 'danger')
                return redirect(url_for('nuevo_prestamo', socio_id=socio_id))

        prestamos_vigentes = conn.execute(
            '''
            SELECT id, numero, categoria_id, COALESCE(saldo_pendiente, monto_aprobado, monto_solicitado, 0) AS saldo_vigente
            FROM prestamos
            WHERE socio_id=? AND estado IN ('pendiente', 'aprobado')
            ORDER BY date(fecha_solicitud) DESC, id DESC
            ''',
            [socio_id]
        ).fetchall()
        tiene_prestamos_vigentes = len(prestamos_vigentes) > 0

        if tiene_prestamos_vigentes and not prestamo_a_amortizar_id:
            conn.close()
            flash('Este asociado ya tiene préstamos vigentes. Debe seleccionar uno para amortizar dentro del mismo formulario.', 'danger')
            return redirect(url_for('nuevo_prestamo', socio_id=socio_id))

        resumen = calcular_resumen_prestamo(monto, tasa, plazo, socio['frecuencia'])

        try:
            count = conn.execute("SELECT COUNT(*) FROM prestamos").fetchone()[0]
            numero = f'PRE-{count+1:04d}'

            if prestamo_a_amortizar_id:
                prestamo_viejo = conn.execute(
                    '''
                    SELECT id, numero, categoria_id,
                           COALESCE(saldo_pendiente, monto_aprobado, monto_solicitado, 0) AS saldo_vigente
                    FROM prestamos
                    WHERE id=? AND socio_id=? AND estado IN ('pendiente', 'aprobado')
                    ''',
                    [prestamo_a_amortizar_id, socio_id]
                ).fetchone()
                if not prestamo_viejo:
                    conn.close()
                    flash('El préstamo seleccionado para amortizar ya no está vigente.', 'danger')
                    return redirect(url_for('nuevo_prestamo', socio_id=socio_id))

                monto_amortizado = float(prestamo_viejo['saldo_vigente'] or 0)
                monto_desembolso = max(0, monto - monto_amortizado)
                conn.execute(
                    '''
                    INSERT INTO prestamos (
                        numero, socio_id, categoria_id, monto_solicitado, tasa_interes, plazo_meses,
                        cuota_mensual, fecha_solicitud, desembolso_tipo, banco_tipo_cuenta,
                        banco_numero_cuenta, refinanciado_de, monto_amortizado, monto_desembolso
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''',
                    (
                        numero,
                        socio_id,
                        categoria['id'],
                        monto,
                        tasa,
                        plazo,
                        resumen['cuota'],
                        date.today().isoformat(),
                        forma_desembolso,
                        banco_tipo,
                        banco_numero,
                        prestamo_viejo['id'],
                        monto_amortizado,
                        monto_desembolso,
                    )
                )
                mensaje = f'Solicitud de préstamo enviada. Si se aprueba, amortizará {prestamo_viejo["numero"]} y el desembolso estimado será de Q{monto_desembolso:,.2f}'
            else:
                conn.execute(
                    '''
                    INSERT INTO prestamos (
                        numero, socio_id, categoria_id, monto_solicitado, tasa_interes, plazo_meses,
                        cuota_mensual, fecha_solicitud, desembolso_tipo, banco_tipo_cuenta,
                        banco_numero_cuenta, monto_desembolso
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    ''',
                    (
                        numero,
                        socio_id,
                        categoria['id'],
                        monto,
                        tasa,
                        plazo,
                        resumen['cuota'],
                        date.today().isoformat(),
                        forma_desembolso,
                        banco_tipo,
                        banco_numero,
                        monto,
                    )
                )
                mensaje = 'Solicitud de préstamo enviada.'

            conn.commit()
            flash(mensaje, 'success')
            return redirect(url_for('prestamos'))
        except Exception as e:
            conn.rollback()
            flash(f'Error: {e}', 'danger')
            return redirect(url_for('nuevo_prestamo', socio_id=socio_id))
        finally:
            conn.close()

    socio_id_seleccionado = request.args.get('socio_id', '').strip()
    contexto = _cargar_contexto_nuevo_prestamo(conn, socio_id_seleccionado=socio_id_seleccionado)
    conn.close()
    return render_template('nuevo_prestamo.html', **contexto)

def obtener_detalle_prestamo_aprobacion(conn, pid):
    prestamo = conn.execute(
        '''
        SELECT p.*, s.codigo AS socio_codigo, s.nombre || ' ' || s.apellido AS nombre_socio,
               s.frecuencia, pc.nombre AS categoria_nombre
        FROM prestamos p
        JOIN socios s ON s.id = p.socio_id
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        WHERE p.id=?
        ''',
        [pid]
    ).fetchone()
    if not prestamo:
        return None

    calendario = conn.execute(
        '''
        SELECT numero_cuota, fecha_programada, monto_programado, estado
        FROM prestamo_calendario_pagos
        WHERE prestamo_id=?
        ORDER BY numero_cuota
        ''',
        [pid]
    ).fetchall()

    item = dict(prestamo)
    item['calendario'] = [dict(row) for row in calendario]
    item['total_cuotas'] = calcular_total_cuotas_prestamo(item.get('plazo_meses'), item.get('frecuencia'))
    return item


@app.route('/prestamos/<int:pid>/aprobar', methods=['GET', 'POST'])
@login_required(role=('Administrador', 'Operador'))
@permission_required('prestamos.aprobar')
def aprobar_prestamo(pid):
    conn = get_db()
    prestamo = obtener_detalle_prestamo_aprobacion(conn, pid)
    if not prestamo:
        conn.close()
        flash('Préstamo no encontrado.', 'danger')
        return redirect(url_for('prestamos'))

    fecha_aprobacion = request.form.get('fecha_aprobacion') if request.method == 'POST' else (prestamo.get('fecha_aprobacion') or date.today().isoformat())
    fecha_primer_pago_default = prestamo.get('calendario', [{}])[0].get('fecha_programada') if prestamo.get('calendario') else None
    if not fecha_primer_pago_default:
        fecha_primer_pago_default = (normalizar_fecha_referencia(fecha_aprobacion) + timedelta(days=obtener_dias_frecuencia(prestamo.get('frecuencia')))).isoformat()
    fecha_primer_pago = request.form.get('fecha_primer_pago') if request.method == 'POST' else fecha_primer_pago_default
    monto_aprobado = float(request.form.get('monto_aprobado', prestamo.get('monto_aprobado') or prestamo.get('monto_solicitado') or 0))
    desembolso_tipo = request.form.get('desembolso_tipo') if request.method == 'POST' else (prestamo.get('desembolso_tipo') or 'deposito')
    desembolso_referencia = request.form.get('desembolso_referencia') if request.method == 'POST' else (prestamo.get('desembolso_referencia') or '')
    resumen = calcular_resumen_prestamo(monto_aprobado, prestamo['tasa_interes'], prestamo['plazo_meses'], prestamo['frecuencia'])
    if request.method == 'GET' and prestamo.get('calendario'):
        calendario_preview = prestamo['calendario']
    else:
        calendario_preview = generar_calendario_prestamo(fecha_primer_pago, resumen['total_cuotas'], resumen['cuota'], prestamo['frecuencia'])

    if request.method == 'POST':
        if prestamo.get('estado') != 'pendiente':
            conn.close()
            flash('Solo se pueden aprobar solicitudes en estado pendiente.', 'warning')
            return redirect(url_for('detalle_prestamo', pid=pid))

        if normalizar_fecha_referencia(fecha_primer_pago) <= normalizar_fecha_referencia(fecha_aprobacion):
            conn.close()
            flash('La primera fecha de pago debe ser posterior a la fecha de aprobación.', 'warning')
            return render_template('aprobar_prestamo.html', prestamo=prestamo, resumen=resumen, calendario_preview=calendario_preview, fecha_aprobacion=fecha_aprobacion, fecha_primer_pago=fecha_primer_pago, desembolso_tipo=desembolso_tipo, desembolso_referencia=desembolso_referencia)

        if desembolso_tipo not in ('deposito', 'cheque'):
            conn.close()
            flash('Debe seleccionar una forma de desembolso válida.', 'warning')
            return render_template('aprobar_prestamo.html', prestamo=prestamo, resumen=resumen, calendario_preview=calendario_preview, fecha_aprobacion=fecha_aprobacion, fecha_primer_pago=fecha_primer_pago, desembolso_tipo=desembolso_tipo, desembolso_referencia=desembolso_referencia)

        if not (desembolso_referencia or '').strip():
            conn.close()
            flash('Debe ingresar la referencia del depósito o cheque.', 'warning')
            return render_template('aprobar_prestamo.html', prestamo=prestamo, resumen=resumen, calendario_preview=calendario_preview, fecha_aprobacion=fecha_aprobacion, fecha_primer_pago=fecha_primer_pago, desembolso_tipo=desembolso_tipo, desembolso_referencia=desembolso_referencia)

        conn.execute(
            "UPDATE prestamos SET estado='aprobado', monto_aprobado=?, cuota_mensual=?, saldo_pendiente=?, fecha_aprobacion=?, desembolso_tipo=?, desembolso_referencia=? WHERE id=?",
            [monto_aprobado, resumen['cuota'], monto_aprobado, fecha_aprobacion, desembolso_tipo, desembolso_referencia.strip(), pid]
        )
        mensaje_aprobacion = 'Préstamo aprobado y calendario generado correctamente.'
        if prestamo.get('refinanciado_de'):
            prestamo_anterior = conn.execute(
                '''
                SELECT id, numero, COALESCE(saldo_pendiente, monto_aprobado, monto_solicitado, 0) AS saldo_vigente
                FROM prestamos
                WHERE id=?
                ''',
                [prestamo['refinanciado_de']]
            ).fetchone()
            if prestamo_anterior and (prestamo_anterior['saldo_vigente'] or 0) > 0:
                monto_amortizado = float(prestamo_anterior['saldo_vigente'] or 0)
                monto_desembolso = max(0, float(monto_aprobado or 0) - monto_amortizado)
                conn.execute(
                    "UPDATE prestamos SET saldo_pendiente=0, estado='pagado' WHERE id=?",
                    [prestamo_anterior['id']]
                )
                conn.execute(
                    "UPDATE prestamos SET monto_amortizado=?, monto_desembolso=? WHERE id=?",
                    [monto_amortizado, monto_desembolso, pid]
                )
                conn.execute(
                    '''
                    INSERT INTO auditoria_eventos (modulo, entidad, entidad_id, accion, descripcion, usuario, fecha)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        'prestamos',
                        'prestamo',
                        prestamo_anterior['id'],
                        'amortizar',
                        f"Amortización de {prestamo_anterior['numero']} aplicada al aprobar el préstamo {prestamo['numero']}. Desembolso real Q{monto_desembolso:,.2f}",
                        session.get('username', 'sistema'),
                        date.today().isoformat(),
                    )
                )
                mensaje_aprobacion = f'Préstamo aprobado, calendario generado y amortización aplicada a {prestamo_anterior["numero"]}.'
        conn.execute('DELETE FROM prestamo_calendario_pagos WHERE prestamo_id=?', [pid])
        conn.executemany(
            '''
            INSERT INTO prestamo_calendario_pagos (prestamo_id, numero_cuota, fecha_programada, monto_programado, estado)
            VALUES (?, ?, ?, ?, 'pendiente')
            ''',
            [(pid, item['numero_cuota'], item['fecha_programada'], item['monto_programado']) for item in calendario_preview]
        )
        conn.commit()
        conn.close()
        flash(mensaje_aprobacion, 'success')
        return redirect(url_for('prestamos', vista='activos'))

    conn.close()
    return render_template('aprobar_prestamo.html', prestamo=prestamo, resumen=resumen, calendario_preview=calendario_preview, fecha_aprobacion=fecha_aprobacion, fecha_primer_pago=fecha_primer_pago, desembolso_tipo=desembolso_tipo, desembolso_referencia=desembolso_referencia)


@app.route('/prestamos/<int:pid>/no-procede', methods=['POST'])
@login_required()
def marcar_prestamo_no_procede(pid):
    conn = get_db()
    prestamo = conn.execute("SELECT id, numero, estado FROM prestamos WHERE id=?", [pid]).fetchone()
    if not prestamo:
        conn.close()
        flash('Prestamo no encontrado.', 'danger')
        return redirect(url_for('prestamos', vista='pendientes'))

    if (prestamo['estado'] or '').lower() != 'pendiente':
        conn.close()
        flash('Solo se pueden marcar como no procede las solicitudes pendientes.', 'warning')
        return redirect(url_for('prestamos', vista='pendientes'))

    conn.execute(
        "UPDATE prestamos SET estado='no_procede', saldo_pendiente=0 WHERE id=?",
        [pid]
    )
    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='prestamos',
        entidad='prestamo',
        entidad_id=pid,
        accion='actualizar',
        descripcion=f'Solicitud de prestamo {prestamo["numero"]} marcada como no procede',
        datos={'estado': 'no_procede'}
    )

    flash('La solicitud se marco como no procede.', 'success')
    return redirect(url_for('prestamos', vista='pendientes'))


@app.route('/prestamos/<int:pid>/calendario/pdf')
def calendario_prestamo_pdf(pid):
    conn = get_db()
    prestamo = obtener_detalle_prestamo_aprobacion(conn, pid)
    if not prestamo:
        conn.close()
        flash('Préstamo no encontrado.', 'danger')
        return redirect(url_for('prestamos'))

    if not prestamo.get('calendario'):
        conn.close()
        flash('Debe generar el calendario de pagos antes de exportarlo.', 'warning')
        return redirect(url_for('aprobar_prestamo', pid=pid))

    prestamo['cooperativa_nombre'] = get_system_setting(conn, 'cooperativa_nombre', DEFAULT_COOPERATIVA_NOMBRE)
    conn.close()

    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    y = height - 45

    pdf.setFont('Helvetica-Bold', 14)
    pdf.drawString(40, y, 'Calendario de Pagos de Prestamo')
    y -= 18
    pdf.setFont('Helvetica', 10)
    pdf.drawString(40, y, prestamo['cooperativa_nombre'])
    y -= 16
    pdf.drawString(40, y, f'Prestamo: {prestamo.get("numero") or ""} · Socio: {prestamo.get("socio_codigo") or ""} - {prestamo.get("nombre_socio") or ""}')
    y -= 16
    pdf.drawString(40, y, f'Categoria: {prestamo.get("categoria_nombre") or "General"} · Frecuencia: {prestamo.get("frecuencia") or "Quincenal"}')
    y -= 24

    pdf.setFont('Helvetica-Bold', 9)
    pdf.drawString(40, y, 'Cuota')
    pdf.drawString(110, y, 'Fecha programada')
    pdf.drawString(260, y, 'Monto')
    pdf.drawString(350, y, 'Estado')
    y -= 14
    pdf.line(40, y, width - 40, y)
    y -= 14

    pdf.setFont('Helvetica', 9)
    for cuota in prestamo['calendario']:
        pdf.drawString(40, y, str(cuota['numero_cuota']))
        pdf.drawString(110, y, cuota['fecha_programada'])
        pdf.drawString(260, y, f"Q{float(cuota['monto_programado']):,.2f}")
        pdf.drawString(350, y, cuota.get('estado', 'pendiente').capitalize())
        y -= 16
        if y < 60:
            pdf.showPage()
            y = height - 45
            pdf.setFont('Helvetica', 9)

    pdf.save()
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name=f'calendario_{prestamo.get("numero")}.pdf', mimetype='application/pdf')


@app.route('/prestamos/<int:pid>/finiquito')
def finiquito_prestamo(pid):
    conn = get_db()
    prestamo = obtener_detalle_prestamo_aprobacion(conn, pid)
    if not prestamo:
        conn.close()
        flash('Préstamo no encontrado.', 'danger')
        return redirect(url_for('prestamos'))

    if prestamo.get('estado') == 'pendiente':
        conn.close()
        flash('Debe aprobar el préstamo antes de generar el finiquito.', 'warning')
        return redirect(url_for('aprobar_prestamo', pid=pid))

    prestamo['cooperativa_nombre'] = get_system_setting(conn, 'cooperativa_nombre', DEFAULT_COOPERATIVA_NOMBRE)
    plantilla = get_system_setting(conn, 'prestamo_finiquito_texto', SYSTEM_SETTINGS_DEFAULTS['prestamo_finiquito_texto'])
    contenido = renderizar_finiquito_prestamo(prestamo, plantilla)
    formato = request.args.get('formato', 'html').strip().lower()
    conn.close()

    if formato == 'pdf':
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas

        buffer = BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        y = height - 50

        pdf.setFont('Helvetica-Bold', 14)
        pdf.drawString(40, y, 'Finiquito de Prestamo')
        y -= 22
        pdf.setFont('Helvetica', 10)
        pdf.drawString(40, y, prestamo['cooperativa_nombre'])
        y -= 18
        pdf.drawString(40, y, f'Prestamo: {prestamo.get("numero") or ""}')
        y -= 24

        pdf.setFont('Helvetica', 10)
        for bloque in contenido.split('\n'):
            linea_actual = ''
            for palabra in bloque.split(' '):
                prueba = (linea_actual + ' ' + palabra).strip()
                if pdf.stringWidth(prueba, 'Helvetica', 10) > (width - 80):
                    pdf.drawString(40, y, linea_actual)
                    y -= 16
                    linea_actual = palabra
                else:
                    linea_actual = prueba
            if linea_actual:
                pdf.drawString(40, y, linea_actual)
                y -= 16
            y -= 6
            if y < 70:
                pdf.showPage()
                y = height - 50
                pdf.setFont('Helvetica', 10)

        pdf.save()
        buffer.seek(0)
        return send_file(buffer, as_attachment=True, download_name=f'finiquito_{prestamo.get("numero")}.pdf', mimetype='application/pdf')

    return render_template('finiquito_prestamo.html', prestamo=prestamo, contenido_finiquito=contenido)

@app.route('/prestamos/<int:pid>/pago', methods=['POST'])
@login_required(role=('Administrador', 'Operador'))
@permission_required('prestamos.pagar')
def pagar_prestamo(pid):
    conn = get_db()
    prestamo = conn.execute("SELECT p.*, s.frecuencia FROM prestamos p JOIN socios s ON s.id = p.socio_id WHERE p.id=?", [pid]).fetchone()

    if periodo_cerrado('prestamos', date.today().isoformat()):
        conn.close()
        flash('El periodo de préstamos está cerrado para la fecha seleccionada.', 'warning')
        return redirect(url_for('prestamos'))
    
    # Validar frecuencia para pagos de préstamo
    if not validar_pago_frecuencia(prestamo['socio_id'], 'prestamo'):
        mensaje = obtener_mensaje_validacion_frecuencia(prestamo['socio_id'], 'prestamo')
        flash(f'No se puede realizar el pago. {mensaje}', 'warning')
        conn.close()
        return redirect(url_for('prestamos'))
    
    tasa_periodica = (prestamo['tasa_interes'] / 100) * (obtener_dias_frecuencia(prestamo['frecuencia']) / 365)
    interes = round(prestamo['saldo_pendiente'] * tasa_periodica, 2)
    capital = round(prestamo['cuota_mensual'] - interes, 2)
    if capital <= 0:
        capital = round(prestamo['cuota_mensual'], 2)
        interes = 0
    nuevo_saldo = round(max(0, prestamo['saldo_pendiente'] - capital), 2)
    numero_comprobante = generar_numero_comprobante(conn)
    cur = conn.execute(
        """
        INSERT INTO pagos_prestamo
        (prestamo_id,monto,capital,interes,saldo_restante,fecha,numero_comprobante)
        VALUES (?,?,?,?,?,?,?)
        """,
        [pid, prestamo['cuota_mensual'], capital, interes, nuevo_saldo, date.today().isoformat(), numero_comprobante]
    )
    estado = 'pagado' if nuevo_saldo == 0 else 'aprobado'
    conn.execute("UPDATE prestamos SET saldo_pendiente=?, estado=? WHERE id=?", [nuevo_saldo, estado, pid])
    cuota_programada = conn.execute(
        '''
        SELECT id FROM prestamo_calendario_pagos
        WHERE prestamo_id=? AND estado='pendiente'
        ORDER BY numero_cuota
        LIMIT 1
        ''',
        [pid]
    ).fetchone()
    if cuota_programada:
        conn.execute(
            "UPDATE prestamo_calendario_pagos SET estado='pagado' WHERE id=?",
            [cuota_programada['id']]
        )
    conn.commit()
    pago_id = cur.lastrowid
    conn.close()

    log_auditoria_evento(
        modulo='prestamos',
        entidad='pago_prestamo',
        entidad_id=pago_id,
        accion='crear',
        descripcion=f'Pago individual aplicado al prestamo {prestamo["numero"]}',
        datos={'prestamo_id': pid, 'monto': prestamo['cuota_mensual'], 'comprobante': numero_comprobante}
    )

    flash('Pago registrado exitosamente.', 'success')
    return redirect(url_for('prestamos'))

@app.route('/api/cuota')
def calcular_cuota():
    monto = float(request.args.get('monto', 0))
    tasa = float(request.args.get('tasa', 18))
    plazo = int(request.args.get('plazo', 12))
    tm = tasa / 100 / 12
    cuota = monto * tm / (1 - (1 + tm)**(-plazo)) if tm > 0 else monto / plazo
    return jsonify({'cuota': round(cuota, 2), 'total': round(cuota * plazo, 2), 'intereses': round(cuota * plazo - monto, 2)})

# ==================== TRANSACCIONES MASIVAS ====================

@app.route('/transacciones_masivas')
@login_required()
def transacciones_masivas():
    return render_template('transacciones_masivas.html')

@app.route('/menu_ahorro')
@login_required()
def menu_ahorro():
    return render_template('menu_ahorro.html')

@app.route('/menu_prestamos')
@login_required()
def menu_prestamos():
    return render_template('menu_prestamos.html')


@app.route('/gestiones')
@login_required(role=('Administrador', 'Operador'))
def gestiones():
    conn = get_db()
    tipo_filtro = (request.args.get('tipo') or 'todos').strip().lower()
    destino_filtro = (request.args.get('destino') or 'todos').strip().lower()
    categoria_id_filtro = (request.args.get('categoria_id') or '').strip()
    estados_validos = {'pendiente', 'aprobado', 'no_procede', 'pagado'}
    estado_filtro = (request.args.get('estado') or 'pendiente').strip().lower()
    if estado_filtro not in estados_validos:
        estado_filtro = 'pendiente'
    if destino_filtro not in {'todos', 'retiro', 'amortizacion'}:
        destino_filtro = 'todos'

    categorias_prestamo = conn.execute(
        "SELECT id, nombre FROM prestamo_categorias WHERE estado='activo' ORDER BY nombre"
    ).fetchall()

    solicitudes = []

    if tipo_filtro in ('todos', 'retiro'):
        filtros_retiro = []
        params_retiro = []
        if estado_filtro:
            filtros_retiro.append("sr.estado = ?")
            params_retiro.append(estado_filtro)
        if destino_filtro == 'amortizacion':
            filtros_retiro.append("COALESCE(sr.destino, 'retiro') = 'amortizacion_prestamo'")
        elif destino_filtro == 'retiro':
            filtros_retiro.append("COALESCE(sr.destino, 'retiro') <> 'amortizacion_prestamo'")
        where_retiro = f"WHERE {' AND '.join(filtros_retiro)}" if filtros_retiro else ''
        retiros = conn.execute(
            f'''
            SELECT sr.id,
                   sr.numero,
                   sr.fecha_solicitud,
                   sr.estado,
                   sr.monto,
                   sr.descripcion,
                                     sr.metodo_retiro,
                                     sr.banco_tipo_cuenta,
                                     sr.banco_numero_cuenta,
                                     COALESCE(sr.destino, 'retiro') AS destino,
                                     sr.prestamo_id,
                                     p.numero AS prestamo_numero,
                   s.codigo AS socio_codigo,
                   s.nombre || ' ' || s.apellido AS socio_nombre,
                   c.numero AS cuenta_numero
            FROM solicitudes_retiro sr
            JOIN socios s ON s.id = sr.socio_id
            JOIN cuentas c ON c.id = sr.cuenta_id
                        LEFT JOIN prestamos p ON p.id = sr.prestamo_id
            {where_retiro}
            ORDER BY sr.fecha_solicitud DESC, sr.id DESC
            ''',
            params_retiro,
        ).fetchall()
        for item in retiros:
            row = dict(item)
            row['tipo_solicitud'] = 'retiro'
            row['categoria_prestamo'] = ''
            solicitudes.append(row)

    if tipo_filtro in ('todos', 'prestamo'):
        filtros_prestamo = []
        params_prestamo = []
        if estado_filtro:
            filtros_prestamo.append("p.estado = ?")
            params_prestamo.append(estado_filtro)
        if categoria_id_filtro.isdigit():
            filtros_prestamo.append("p.categoria_id = ?")
            params_prestamo.append(int(categoria_id_filtro))
        where_prestamo = f"WHERE {' AND '.join(filtros_prestamo)}" if filtros_prestamo else ''
        prestamos = conn.execute(
            f'''
            SELECT p.id,
                   p.numero,
                   p.fecha_solicitud,
                   p.estado,
                   p.monto_solicitado AS monto,
                   '' AS descripcion,
                                     '' AS metodo_retiro,
                                     '' AS banco_tipo_cuenta,
                                     '' AS banco_numero_cuenta,
                                     '' AS destino,
                                     NULL AS prestamo_id,
                                     '' AS prestamo_numero,
                   s.codigo AS socio_codigo,
                   s.nombre || ' ' || s.apellido AS socio_nombre,
                   '' AS cuenta_numero,
                   COALESCE(pc.nombre, 'General') AS categoria_prestamo
            FROM prestamos p
            JOIN socios s ON s.id = p.socio_id
            LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
            {where_prestamo}
            ORDER BY p.fecha_solicitud DESC, p.id DESC
            ''',
            params_prestamo,
        ).fetchall()
        for item in prestamos:
            row = dict(item)
            row['tipo_solicitud'] = 'prestamo'
            solicitudes.append(row)

    solicitudes = sorted(
        solicitudes,
        key=lambda item: ((item.get('fecha_solicitud') or ''), item.get('id') or 0),
        reverse=True,
    )

    conn.close()
    return render_template(
        'gestiones.html',
        solicitudes=solicitudes,
        categorias_prestamo=categorias_prestamo,
        tipo_filtro=tipo_filtro,
        destino_filtro=destino_filtro,
        categoria_id_filtro=categoria_id_filtro,
        estado_filtro=estado_filtro,
    )


@app.route('/gestiones/retiro')
@login_required(role=('Administrador', 'Operador'))
def gestion_retiro():
    conn = get_db()
    cuentas = conn.execute(
        '''
        SELECT c.id,
               c.numero,
               c.saldo,
             c.socio_id,
               c.producto_ahorro,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre,
               s.banco_nombre,
               s.banco_tipo_cuenta,
               s.banco_numero_cuenta
        FROM cuentas c
        JOIN socios s ON s.id = c.socio_id
        WHERE c.tipo='ahorro' AND c.estado='activa' AND s.estado='activo'
        ORDER BY s.codigo, c.numero
        '''
    ).fetchall()
    prestamos_vigentes = conn.execute(
        '''
        SELECT p.id,
               p.socio_id,
               p.numero,
               COALESCE(p.saldo_pendiente, 0) AS saldo_pendiente
        FROM prestamos p
        WHERE p.estado='aprobado' AND COALESCE(p.saldo_pendiente, 0) > 0
        ORDER BY p.numero
        '''
    ).fetchall()
    conn.close()
    return render_template('nuevo_retiro.html', cuentas=cuentas, prestamos_vigentes=prestamos_vigentes)


@app.route('/gestiones/solicitud-prestamo')
@login_required(role=('Administrador', 'Operador'))
def gestion_solicitud_prestamo():
    return redirect(url_for('nuevo_prestamo'))


@app.route('/gestiones/retiro/nuevo', methods=['POST'])
@login_required(role=('Administrador', 'Operador'))
def nueva_solicitud_retiro():
    conn = get_db()
    cuenta_id = (request.form.get('cuenta_id') or '').strip()
    monto_raw = (request.form.get('monto') or '').strip()
    descripcion = (request.form.get('descripcion') or 'Retiro solicitado desde modulo gestiones').strip()
    metodo_retiro = (request.form.get('metodo_retiro') or '').strip().lower()
    destino = (request.form.get('destino') or 'retiro').strip().lower()
    prestamo_id_raw = (request.form.get('prestamo_id') or '').strip()
    banco_tipo_cuenta = ''
    banco_numero_cuenta = ''
    prestamo_id = None

    cuentas = conn.execute(
        '''
        SELECT c.id,
               c.numero,
               c.saldo,
             c.socio_id,
               c.producto_ahorro,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre,
               s.banco_nombre,
               s.banco_tipo_cuenta,
               s.banco_numero_cuenta
        FROM cuentas c
        JOIN socios s ON s.id = c.socio_id
        WHERE c.tipo='ahorro' AND c.estado='activa' AND s.estado='activo'
        ORDER BY s.codigo, c.numero
        '''
    ).fetchall()

    try:
        if not cuenta_id.isdigit():
            raise ValueError('Debe seleccionar una cuenta de ahorro válida.')
        monto = float(monto_raw)
        if monto <= 0:
            raise ValueError('El monto debe ser mayor a cero.')
        if metodo_retiro not in ('cheque', 'deposito'):
            raise ValueError('Debe seleccionar una forma de retiro válida.')
        if destino not in ('retiro', 'amortizacion_prestamo'):
            raise ValueError('Debe seleccionar un destino válido para la solicitud.')

        cuenta = conn.execute(
            '''
            SELECT c.id, c.numero, c.saldo, c.socio_id,
                   s.estado AS socio_estado,
                   s.banco_nombre,
                   s.banco_tipo_cuenta,
                   s.banco_numero_cuenta
            FROM cuentas c
            JOIN socios s ON s.id = c.socio_id
            WHERE c.id=?
            ''',
            [int(cuenta_id)],
        ).fetchone()
        if not cuenta or (cuenta['socio_estado'] or '').lower() != 'activo':
            raise ValueError('La cuenta seleccionada no está disponible para retiro.')

        if monto > float(cuenta['saldo'] or 0):
            raise ValueError('El monto solicitado excede el saldo disponible de la cuenta.')

        if destino == 'amortizacion_prestamo':
            if not prestamo_id_raw.isdigit():
                raise ValueError('Debe seleccionar un préstamo vigente para amortizar.')
            prestamo = conn.execute(
                '''
                SELECT id,
                       socio_id,
                       numero,
                       COALESCE(saldo_pendiente, 0) AS saldo_pendiente,
                       estado
                FROM prestamos
                WHERE id=?
                ''',
                [int(prestamo_id_raw)],
            ).fetchone()
            if not prestamo:
                raise ValueError('El préstamo seleccionado no existe.')
            if int(prestamo['socio_id']) != int(cuenta['socio_id']):
                raise ValueError('El préstamo seleccionado no pertenece al titular de la cuenta.')
            if (prestamo['estado'] or '').lower() != 'aprobado' or float(prestamo['saldo_pendiente'] or 0) <= 0:
                raise ValueError('El préstamo seleccionado ya no está vigente.')
            if float(cuenta['saldo'] or 0) < float(prestamo['saldo_pendiente'] or 0):
                raise ValueError('El saldo de ahorro de la cuenta seleccionada debe ser mayor o igual al saldo pendiente del préstamo a amortizar.')
            if monto > float(prestamo['saldo_pendiente'] or 0):
                raise ValueError('El monto solicitado excede el saldo pendiente del préstamo a amortizar.')
            prestamo_id = int(prestamo['id'])

        if metodo_retiro == 'deposito':
            banco_tipo_cuenta = (cuenta['banco_tipo_cuenta'] or '').strip()
            banco_numero_cuenta = (cuenta['banco_numero_cuenta'] or '').strip()
            banco_nombre = (cuenta['banco_nombre'] or '').strip()
            if not banco_nombre or not banco_tipo_cuenta or not banco_numero_cuenta:
                raise ValueError('El asociado no tiene datos bancarios completos. Actualice banco, tipo y número de cuenta en el perfil del socio.')
        else:
            banco_tipo_cuenta = ''
            banco_numero_cuenta = ''

        count = conn.execute("SELECT COUNT(*) FROM solicitudes_retiro").fetchone()[0] or 0
        numero = f'RET-{count + 1:05d}'

        conn.execute(
            '''
            INSERT INTO solicitudes_retiro
            (numero, cuenta_id, socio_id, monto, descripcion, metodo_retiro, banco_tipo_cuenta, banco_numero_cuenta, destino, prestamo_id, fecha_solicitud, estado)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pendiente')
            ''',
            (
                numero,
                cuenta['id'],
                cuenta['socio_id'],
                monto,
                descripcion,
                metodo_retiro,
                banco_tipo_cuenta,
                banco_numero_cuenta,
                destino,
                prestamo_id,
                date.today().isoformat(),
            ),
        )
        conn.commit()
        flash('Solicitud de retiro enviada correctamente.', 'success')
        return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))
    except Exception as e:
        flash(f'Error: {e}', 'danger')
        return render_template('nuevo_retiro.html', cuentas=cuentas)
    finally:
        conn.close()


@app.route('/gestiones/retiro/<int:rid>/aprobar', methods=['POST'])
@login_required(role=('Administrador', 'Operador'))
def aprobar_solicitud_retiro(rid):
    conn = get_db()
    solicitud = conn.execute(
        '''
        SELECT sr.*, c.numero AS cuenta_numero, c.saldo,
               p.numero AS prestamo_numero,
               COALESCE(p.saldo_pendiente, 0) AS prestamo_saldo_pendiente,
               p.estado AS prestamo_estado
        FROM solicitudes_retiro sr
        JOIN cuentas c ON c.id = sr.cuenta_id
        LEFT JOIN prestamos p ON p.id = sr.prestamo_id
        WHERE sr.id=?
        ''',
        [rid],
    ).fetchone()

    if not solicitud:
        conn.close()
        flash('Solicitud de retiro no encontrada.', 'danger')
        return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

    if (solicitud['estado'] or '').lower() != 'pendiente':
        conn.close()
        flash('Solo se pueden aprobar solicitudes en estado pendiente.', 'warning')
        return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

    monto = float(solicitud['monto'] or 0)
    saldo_actual = float(solicitud['saldo'] or 0)
    destino = (solicitud['destino'] or 'retiro').lower()
    if monto > saldo_actual:
        conn.close()
        flash('La solicitud no se puede aprobar porque el saldo actual es insuficiente.', 'danger')
        return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

    if destino == 'amortizacion_prestamo':
        if not solicitud['prestamo_id']:
            conn.close()
            flash('La solicitud está marcada para amortización, pero no tiene préstamo asociado.', 'danger')
            return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))
        if (solicitud['prestamo_estado'] or '').lower() != 'aprobado' or float(solicitud['prestamo_saldo_pendiente'] or 0) <= 0:
            conn.close()
            flash('El préstamo asociado ya no está vigente. No se pudo aprobar la solicitud.', 'danger')
            return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))
        if saldo_actual < float(solicitud['prestamo_saldo_pendiente'] or 0):
            conn.close()
            flash('El saldo actual de ahorro debe ser mayor o igual al saldo pendiente del préstamo para aplicar amortización.', 'danger')
            return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))
        if monto > float(solicitud['prestamo_saldo_pendiente'] or 0):
            conn.close()
            flash('El monto solicitado supera el saldo pendiente actual del préstamo asociado.', 'danger')
            return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

    nuevo_saldo = saldo_actual - monto
    conn.execute("UPDATE cuentas SET saldo=? WHERE id=?", [nuevo_saldo, solicitud['cuenta_id']])
    conn.execute(
        '''
        INSERT INTO transacciones
        (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha)
        VALUES (?, 'retiro', ?, ?, ?, ?)
        ''',
        (
            solicitud['cuenta_id'],
            monto,
            nuevo_saldo,
            solicitud['descripcion'] or 'Retiro aprobado desde modulo gestiones',
            datetime.now().isoformat(),
        ),
    )

    if destino == 'amortizacion_prestamo':
        prestamo_saldo_actual = float(solicitud['prestamo_saldo_pendiente'] or 0)
        nuevo_saldo_prestamo = round(max(0, prestamo_saldo_actual - monto), 2)
        estado_prestamo = 'pagado' if nuevo_saldo_prestamo == 0 else 'aprobado'
        numero_comprobante = generar_numero_comprobante(conn)
        conn.execute(
            '''
            INSERT INTO pagos_prestamo
            (prestamo_id, monto, capital, interes, saldo_restante, descripcion, boleta_deposito, fecha, numero_comprobante)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                solicitud['prestamo_id'],
                monto,
                monto,
                0,
                nuevo_saldo_prestamo,
                f"Amortización desde solicitud de retiro {solicitud['numero']}",
                solicitud['numero'],
                date.today().isoformat(),
                numero_comprobante,
            ),
        )
        conn.execute(
            "UPDATE prestamos SET saldo_pendiente=?, estado=? WHERE id=?",
            [nuevo_saldo_prestamo, estado_prestamo, solicitud['prestamo_id']]
        )

    conn.execute(
        "UPDATE solicitudes_retiro SET estado='aprobado', fecha_aprobacion=?, aprobado_por=? WHERE id=?",
        [date.today().isoformat(), session.get('username'), rid],
    )
    conn.commit()
    conn.close()

    flash('Retiro realizado con exito.', 'success')
    return redirect(url_for('comprobante_retiro', rid=rid, auto_print='1'))


@app.route('/gestiones/retiro/<int:rid>/comprobante')
@login_required(role=('Administrador', 'Operador'))
def comprobante_retiro(rid):
    conn = get_db()
    retiro = conn.execute(
        '''
        SELECT sr.*, c.numero AS cuenta_numero,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre,
               p.numero AS prestamo_numero
        FROM solicitudes_retiro sr
        JOIN cuentas c ON c.id = sr.cuenta_id
        JOIN socios s ON s.id = sr.socio_id
        LEFT JOIN prestamos p ON p.id = sr.prestamo_id
        WHERE sr.id=?
        ''',
        (rid,),
    ).fetchone()
    conn.close()

    if not retiro:
        flash('Comprobante de retiro no encontrado.', 'danger')
        return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

    if (retiro['estado'] or '').lower() != 'aprobado':
        flash('El comprobante solo esta disponible para retiros aprobados.', 'warning')
        return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

    auto_print = (request.args.get('auto_print') or '').strip() == '1'
    return render_template('comprobante_retiro.html', retiro=retiro, auto_print=auto_print)


@app.route('/gestiones/retiro/<int:rid>/no-procede', methods=['POST'])
@login_required(role=('Administrador', 'Operador'))
def marcar_solicitud_retiro_no_procede(rid):
    conn = get_db()
    solicitud = conn.execute(
        "SELECT id, numero, estado FROM solicitudes_retiro WHERE id=?",
        [rid],
    ).fetchone()

    if not solicitud:
        conn.close()
        flash('Solicitud de retiro no encontrada.', 'danger')
        return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

    if (solicitud['estado'] or '').lower() != 'pendiente':
        conn.close()
        flash('Solo se pueden marcar como no procede las solicitudes pendientes.', 'warning')
        return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

    conn.execute(
        "UPDATE solicitudes_retiro SET estado='no_procede', fecha_aprobacion=?, aprobado_por=? WHERE id=?",
        [date.today().isoformat(), session.get('username'), rid],
    )
    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='ahorro',
        entidad='solicitud_retiro',
        entidad_id=rid,
        accion='actualizar',
        descripcion=f'Solicitud de retiro {solicitud["numero"]} marcada como no procede',
        datos={'estado': 'no_procede'}
    )

    flash('La solicitud de retiro se marco como no procede.', 'success')
    return redirect(url_for('gestiones', tipo='retiro', estado='pendiente'))

# ==================== OPERACIONES DE AHORRO ====================

@app.route('/configuracion_ahorro', methods=['GET', 'POST'])
@login_required()
@permission_required('config.ahorro')
def configuracion_ahorro():
    conn = get_db()
    try:
        ensure_system_settings(conn)
        ensure_module_settings(conn)

        if request.method == 'POST':
            campos = list(AHORRO_SETTINGS_DEFAULTS.keys())
            actualizados = 0

            for clave in campos:
                if clave not in request.form:
                    continue
                valor = (request.form.get(clave) or '').strip()
                if not valor:
                    continue
                set_system_setting(conn, clave, valor, session.get('username'))
                actualizados += 1

            conn.commit()
            if actualizados:
                flash('Configuracion de ahorro actualizada correctamente.', 'success')
            else:
                flash('No se recibieron cambios para guardar.', 'warning')
            return redirect(url_for('configuracion_ahorro'))

        ahorro_cfg = {
            clave: get_system_setting(conn, clave, valor_default)
            for clave, valor_default in AHORRO_SETTINGS_DEFAULTS.items()
        }
        return render_template('configuracion_ahorro.html', ahorro_cfg=ahorro_cfg)
    except Exception as e:
        flash(f'Error cargando configuracion de ahorro: {e}', 'danger')
        return render_template('configuracion_ahorro.html', ahorro_cfg=AHORRO_SETTINGS_DEFAULTS)
    finally:
        conn.close()

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


@app.route('/planilla_retiros_ahorro')
@login_required()
def planilla_retiros_ahorro():
    fecha_actual = date.today().isoformat()
    return render_template('planilla_retiros_ahorro.html', fecha_actual=fecha_actual)


@app.route('/planilla_transferencias_ahorro')
@login_required()
def planilla_transferencias_ahorro():
    fecha_actual = date.today().isoformat()
    return render_template('planilla_transferencias_ahorro.html', fecha_actual=fecha_actual)


@app.route('/reportes_ahorro')
@login_required()
def reportes_ahorro():
    fecha_actual = date.today().isoformat()
    fecha_mes_anterior = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1).isoformat()
    return render_template('reportes_ahorro.html', fecha_actual=fecha_actual, fecha_mes_anterior=fecha_mes_anterior)


@app.route('/validar_retiros_ahorro', methods=['POST'])
@login_required()
def validar_retiros_ahorro():
    try:
        file = request.files.get('archivo')
        if not file:
            return jsonify({'success': False, 'error': 'Archivo no encontrado'}), 400

        filas = _leer_archivo_masivo(file, ['numero_cuenta', 'monto_retiro', 'descripcion'])
        total = len(filas)
        validos = 0
        errores = 0
        monto_total = 0.0
        errores_detalle = []
        datos_validos = []

        conn = get_db()
        for i, fila in enumerate(filas, start=1):
            numero = fila.get('numero_cuenta') or fila.get('numero')
            monto = fila.get('monto_retiro') or fila.get('monto')
            descripcion = fila.get('descripcion', '') or ''

            if not numero:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Falta número de cuenta'})
                continue

            try:
                monto = float(monto)
            except Exception:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Monto no válido'})
                continue

            if monto <= 0:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Monto debe ser mayor que 0'})
                continue

            cuenta = conn.execute('SELECT * FROM cuentas WHERE numero=? AND tipo="ahorro" AND estado="activa"', (numero,)).fetchone()
            if not cuenta:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Cuenta no encontrada o no activa'})
                continue

            if monto > cuenta['saldo']:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Saldo insuficiente'})
                continue

            validos += 1
            monto_total += monto
            datos_validos.append({'cuenta_id': cuenta['id'], 'numero_cuenta': numero, 'monto': monto, 'descripcion': descripcion})

        conn.close()

        return jsonify({
            'success': True,
            'total_registros': total,
            'validos': validos,
            'errores': errores,
            'monto_total': monto_total,
            'errores_detalle': errores_detalle,
            'datos_validos': datos_validos
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/procesar_retiros_ahorro', methods=['POST'])
@login_required()
@permission_required('ahorro.masivo')
def procesar_retiros_ahorro():
    data = request.get_json() or {}
    retiros = data.get('retiros', [])
    fecha_retiro = data.get('fecha_retiro', date.today().isoformat())

    if not retiros:
        return jsonify({'success': False, 'error': 'No hay retiros para procesar'}), 400

    conn = get_db()
    if not validate_idempotency(conn, 'procesar_retiros_ahorro'):
        conn.close()
        return jsonify({'success': False, 'error': 'Solicitud duplicada detectada (idempotencia).'}), 409
    c = conn.cursor()
    procesados = 0
    monto_total = 0.0
    errores = []

    for retiro in retiros:
        try:
            cuenta_id = retiro['cuenta_id']
            monto = float(retiro['monto'])
            descripcion = retiro.get('descripcion', 'Retiro masivo')

            cuenta = c.execute('SELECT saldo FROM cuentas WHERE id=? AND tipo="ahorro" AND estado="activa"', (cuenta_id,)).fetchone()
            if not cuenta:
                errores.append(f'Cuenta {cuenta_id} no encontrada')
                continue

            if monto <= 0 or monto > cuenta['saldo']:
                errores.append(f'Monto inválido para cuenta {cuenta_id}')
                continue

            nuevo_saldo = cuenta['saldo'] - monto
            c.execute('UPDATE cuentas SET saldo=? WHERE id=?', (nuevo_saldo, cuenta_id))
            c.execute('INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)',
                      (cuenta_id, 'retiro', monto, nuevo_saldo, descripcion, fecha_retiro))
            procesados += 1
            monto_total += monto
        except Exception as e:
            errores.append(f'Cuenta {retiro.get("numero_cuenta", cuenta_id)}: {str(e)}')

    conn.commit()
    conn.close()

    return jsonify({'success': True, 'procesados': procesados, 'monto_total': monto_total, 'errores': errores})


@app.route('/validar_transferencias_ahorro', methods=['POST'])
@login_required()
def validar_transferencias_ahorro():
    try:
        file = request.files.get('archivo')
        if not file:
            return jsonify({'success': False, 'error': 'Archivo no encontrado'}), 400

        filas = _leer_archivo_masivo(file, ['cuenta_origen', 'cuenta_destino', 'monto_transferencia', 'descripcion'])
        total = len(filas)
        validos = 0
        errores = 0
        monto_total = 0.0
        errores_detalle = []
        datos_validos = []
        resumen_transferencias = []

        conn = get_db()

        for i, fila in enumerate(filas, start=1):
            origen = fila.get('cuenta_origen')
            destino = fila.get('cuenta_destino')
            monto = fila.get('monto_transferencia') or fila.get('monto')
            descripcion = fila.get('descripcion', '') or ''

            if not origen or not destino:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Faltan cuentas origen o destino'})
                continue

            if origen == destino:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Origen y destino deben ser diferentes'})
                continue

            try:
                monto = float(monto)
            except Exception:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Monto no válido'})
                continue

            if monto <= 0:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Monto debe ser mayor que 0'})
                continue

            c_origen = conn.execute('SELECT * FROM cuentas WHERE numero=? AND tipo="ahorro" AND estado="activa"', (origen,)).fetchone()
            c_destino = conn.execute('SELECT * FROM cuentas WHERE numero=? AND tipo="ahorro" AND estado="activa"', (destino,)).fetchone()

            if not c_origen or not c_destino:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Cuenta origen/destino no existe o no está activa'})
                continue

            if monto > c_origen['saldo']:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Saldo insuficiente en origen'})
                continue

            validos += 1
            monto_total += monto
            datos_validos.append({'cuenta_origen': c_origen['id'], 'cuenta_destino': c_destino['id'], 'monto': monto, 'descripcion': descripcion})

            resumen_transferencias.append({
                'cuenta_origen': origen,
                'cuenta_destino': destino,
                'monto': monto,
                'saldo_origen_despues': c_origen['saldo'] - monto,
                'saldo_destino_despues': c_destino['saldo'] + monto,
                'descripcion': descripcion
            })

        conn.close()

        return jsonify({
            'success': True,
            'total_registros': total,
            'validos': validos,
            'errores': errores,
            'monto_total': monto_total,
            'errores_detalle': errores_detalle,
            'datos_validos': datos_validos,
            'resumen_transferencias': resumen_transferencias
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/procesar_transferencias_ahorro', methods=['POST'])
@login_required()
@permission_required('ahorro.masivo')
def procesar_transferencias_ahorro():
    data = request.get_json() or {}
    movimientos = data.get('transferencias', [])
    fecha_transferencia = data.get('fecha_transferencia', date.today().isoformat())
    tipo_transferencia = data.get('tipo_transferencia', 'inmediata')
    comision = float(data.get('comision_transferencia', 0.0) or 0.0)

    if not movimientos:
        return jsonify({'success': False, 'error': 'No hay transferencias para procesar'}), 400

    conn = get_db()
    if not validate_idempotency(conn, 'procesar_transferencias_ahorro'):
        conn.close()
        return jsonify({'success': False, 'error': 'Solicitud duplicada detectada (idempotencia).'}), 409
    c = conn.cursor()
    procesados = 0
    monto_total = 0.0
    errores = []

    for movimiento in movimientos:
        try:
            origen_id = movimiento['cuenta_origen']
            destino_id = movimiento['cuenta_destino']
            monto = float(movimiento['monto'])
            descripcion = movimiento.get('descripcion', 'Transferencia interna')

            c_origen = c.execute('SELECT saldo FROM cuentas WHERE id=? AND tipo="ahorro" AND estado="activa"', (origen_id,)).fetchone()
            c_destino = c.execute('SELECT saldo FROM cuentas WHERE id=? AND tipo="ahorro" AND estado="activa"', (destino_id,)).fetchone()

            if not c_origen or not c_destino:
                errores.append(f'Origen o destino no válidos para transferencia {origen_id}->{destino_id}')
                continue

            if monto <= 0 or monto > c_origen['saldo']:
                errores.append(f'Monto inválido para transferencia {origen_id}->{destino_id}')
                continue

            saldo_origen_nuevo = c_origen['saldo'] - monto - comision
            saldo_destino_nuevo = c_destino['saldo'] + monto
            if saldo_origen_nuevo < 0:
                errores.append(f'Saldo insuficiente (incluyendo comisión) en cuenta origen {origen_id}')
                continue

            c.execute('UPDATE cuentas SET saldo=? WHERE id=?', (saldo_origen_nuevo, origen_id))
            c.execute('UPDATE cuentas SET saldo=? WHERE id=?', (saldo_destino_nuevo, destino_id))

            c.execute('INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)',
                      (origen_id, 'transferencia_salida', monto, saldo_origen_nuevo, descripcion + f' ({tipo_transferencia})', fecha_transferencia))
            c.execute('INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)',
                      (destino_id, 'transferencia_entrada', monto, saldo_destino_nuevo, descripcion + f' ({tipo_transferencia})', fecha_transferencia))

            if comision > 0:
                c.execute('INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)',
                          (origen_id, 'comision', comision, saldo_origen_nuevo, 'Comisión transferencia', fecha_transferencia))

            procesados += 1
            monto_total += monto

        except Exception as e:
            errores.append(f'Error traslado {origen_id}->{destino_id}: {str(e)}')

    conn.commit()
    conn.close()

    return jsonify({'success': True, 'procesados': procesados, 'monto_total': monto_total, 'errores': errores})


@app.route('/generar_reporte_ahorro', methods=['POST'])
@login_required()
def generar_reporte_ahorro():
    data = request.get_json() or {}
    tipo = data.get('tipo_reporte', 'saldos')
    fecha_inicio = data.get('fecha_inicio')
    fecha_fin = data.get('fecha_fin')

    conn = get_db()
    c = conn.cursor()

    try:
        if tipo == 'saldos':
            rows = c.execute('''
                SELECT c.numero AS numero_cuenta, s.nombre || ' ' || s.apellido AS nombre_socio,
                       c.saldo AS saldo_actual, c.estado,
                       (SELECT fecha FROM transacciones t WHERE t.cuenta_id=c.id ORDER BY t.fecha DESC LIMIT 1) AS ultimo_movimiento
                FROM cuentas c
                JOIN socios s ON c.socio_id=s.id
                WHERE c.tipo='ahorro'
            ''').fetchall()
            resultados = [{'numero_cuenta': r['numero_cuenta'], 'nombre_socio': r['nombre_socio'], 'saldo_actual': r['saldo_actual'] or 0.0, 'ultimo_movimiento': r['ultimo_movimiento'], 'estado': r['estado']} for r in rows]

        elif tipo == 'movimientos':
            if not fecha_inicio or not fecha_fin:
                return jsonify({'success': False, 'error': 'Debe indicar rango de fechas'}), 400
            rows = c.execute('''
                SELECT t.fecha, c.numero AS numero_cuenta, t.tipo, t.monto, t.saldo_despues, t.descripcion
                FROM transacciones t
                JOIN cuentas c ON t.cuenta_id=c.id
                WHERE c.tipo='ahorro' AND date(t.fecha) BETWEEN date(?) AND date(?)
                ORDER BY t.fecha ASC
            ''', (fecha_inicio, fecha_fin)).fetchall()
            resultados = [{'fecha': r['fecha'], 'numero_cuenta': r['numero_cuenta'], 'tipo': r['tipo'], 'monto': r['monto'], 'saldo_despues': r['saldo_despues'], 'descripcion': r['descripcion']} for r in rows]

        elif tipo == 'comparativo':
            if not fecha_inicio or not fecha_fin:
                return jsonify({'success': False, 'error': 'Debe indicar rango de fechas'}), 400
            cuentas_data = c.execute('SELECT id, numero, socio_id, saldo FROM cuentas WHERE tipo="ahorro"').fetchall()
            resultados = []
            for cuenta in cuentas_data:
                saldo_actual = cuenta['saldo'] or 0.0
                anterior = c.execute('''
                    SELECT saldo_despues FROM transacciones
                    WHERE cuenta_id=? AND date(fecha) < date(?)
                    ORDER BY fecha DESC LIMIT 1
                ''', (cuenta['id'], fecha_inicio)).fetchone()
                saldo_anterior = anterior['saldo_despues'] if anterior else 0.0
                socio = c.execute('SELECT nombre, apellido FROM socios WHERE id=?', (cuenta['socio_id'],)).fetchone()
                resultados.append({
                    'numero_cuenta': cuenta['numero'],
                    'nombre_socio': socio['nombre'] + ' ' + socio['apellido'],
                    'saldo_anterior': saldo_anterior,
                    'saldo_actual': saldo_actual
                })

        elif tipo == 'inactivas':
            fecha_corte = fecha_fin or date.today().isoformat()
            rows = c.execute('''
                SELECT c.numero AS numero_cuenta, s.nombre || ' ' || s.apellido AS nombre_socio,
                       c.saldo AS saldo_actual,
                       MAX(t.fecha) AS ultima_actividad,
                       julianday(date(?)) - julianday(MAX(date(t.fecha))) AS dias_inactiva
                FROM cuentas c
                JOIN socios s ON c.socio_id=s.id
                LEFT JOIN transacciones t ON t.cuenta_id=c.id
                WHERE c.tipo='ahorro'
                GROUP BY c.id
                HAVING dias_inactiva > 30
            ''', (fecha_corte,)).fetchall()
            resultados = [{'numero_cuenta': r['numero_cuenta'], 'nombre_socio': r['nombre_socio'], 'saldo_actual': r['saldo_actual'] or 0.0, 'ultima_actividad': r['ultima_actividad'] or 'N/A', 'dias_inactiva': int(r['dias_inactiva'] or 0)} for r in rows]

        else:
            return jsonify({'success': False, 'error': 'Tipo de reporte desconocido'}), 400

        total_cuentas = c.execute('SELECT COUNT(*) FROM cuentas WHERE tipo="ahorro"').fetchone()[0]
        total_saldo = c.execute('SELECT COALESCE(SUM(saldo),0) FROM cuentas WHERE tipo="ahorro"').fetchone()[0]
        promedio_saldo = float(total_saldo) / total_cuentas if total_cuentas else 0.0
        cuentas_activas = c.execute('SELECT COUNT(*) FROM cuentas WHERE tipo="ahorro" AND estado="activa"').fetchone()[0]

        return jsonify({'success': True, 'resultados': resultados, 'estadisticas': {
            'total_cuentas': total_cuentas,
            'saldo_total': total_saldo,
            'promedio_saldo': promedio_saldo,
            'cuentas_activas': cuentas_activas
        }})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        conn.close()


# ==================== OPERACIONES DE PRÉSTAMOS ====================

@app.route('/planilla_amortizaciones')
@login_required()
def planilla_amortizaciones():
    return render_template('planilla_amortizaciones.html')

@app.route('/planilla_refinanciamientos')
@login_required()
def planilla_refinanciamientos():
    return render_template('planilla_refinanciamientos.html')

@app.route('/reportes_prestamos')
@login_required()
def reportes_prestamos():
    return render_template(
        'reportes_prestamos.html',
        fecha_actual=date.today().isoformat(),
        fecha_mes_anterior=(date.today() - timedelta(days=30)).isoformat()
    )

@app.route('/configuracion_prestamos', methods=['GET', 'POST'])
@login_required()
@permission_required('config.prestamos')
def configuracion_prestamos():
    conn = get_db()
    try:
        ensure_system_settings(conn)
        ensure_module_settings(conn)

        if request.method == 'POST':
            campos = list(PRESTAMO_SETTINGS_DEFAULTS.keys())
            actualizados = 0

            for clave in campos:
                if clave not in request.form:
                    continue
                valor = (request.form.get(clave) or '').strip()
                if not valor:
                    continue
                set_system_setting(conn, clave, valor, session.get('username'))
                actualizados += 1

            conn.commit()
            if actualizados:
                flash('Configuracion de prestamos actualizada correctamente.', 'success')
            else:
                flash('No se recibieron cambios para guardar.', 'warning')
            return redirect(url_for('configuracion_prestamos'))

        prestamo_cfg = {
            clave: get_system_setting(conn, clave, valor_default)
            for clave, valor_default in PRESTAMO_SETTINGS_DEFAULTS.items()
        }
        return render_template('configuracion_prestamos.html', prestamo_cfg=prestamo_cfg)
    except Exception as e:
        flash(f'Error cargando configuracion de prestamos: {e}', 'danger')
        return render_template('configuracion_prestamos.html', prestamo_cfg=PRESTAMO_SETTINGS_DEFAULTS)
    finally:
        conn.close()

@app.route('/cobranza_prestamos')
@login_required()
def cobranza_prestamos():
    return render_template('cobranza_prestamos.html', fecha_actual_datetime=datetime.now().strftime('%Y-%m-%dT%H:%M'))


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

    intereses = conn.execute(
        f"SELECT COALESCE(SUM(pp.interes),0) FROM pagos_prestamo pp WHERE 1=1 {where_rango}",
        params_rango
    ).fetchone()[0]
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
        rows = conn.execute(
            '''
            SELECT substr(fecha,1,7) AS mes,
                   COALESCE(SUM(interes),0) AS intereses_cobrados,
                   COALESCE(AVG(saldo_restante),0) AS cartera_promedio
            FROM pagos_prestamo
            GROUP BY substr(fecha,1,7)
            ORDER BY mes DESC
            LIMIT 12
            '''
        ).fetchall()
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
        rows = conn.execute(
            '''
            SELECT substr(fecha_solicitud,1,7) AS mes,
                   COUNT(*) AS nuevos_prestamos,
                   COALESCE(SUM(monto_solicitado),0) AS cartera_actual
            FROM prestamos
            GROUP BY substr(fecha_solicitud,1,7)
            ORDER BY mes DESC
            LIMIT 12
            '''
        ).fetchall()
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


@app.route('/generar_reporte_prestamos', methods=['POST'])
@login_required()
def generar_reporte_prestamos():
    data = request.get_json() or {}
    tipo_reporte = (data.get('tipo_reporte') or 'cartera_activa').strip()
    fecha_inicio = (data.get('fecha_inicio') or '').strip() or None
    fecha_fin = (data.get('fecha_fin') or '').strip() or None

    try:
        resultados, estadisticas, morosidad = _generar_datos_reporte_prestamos(tipo_reporte, fecha_inicio, fecha_fin)
        return jsonify({'success': True, 'resultados': resultados, 'estadisticas': estadisticas, 'morosidad': morosidad})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/reporte_prestamos/export')
@login_required()
def exportar_reporte_prestamos():
    tipo_reporte = request.args.get('tipo_reporte', 'cartera_activa').strip()
    fecha_inicio = request.args.get('fecha_inicio', '').strip() or None
    fecha_fin = request.args.get('fecha_fin', '').strip() or None
    formato = request.args.get('formato', 'excel').strip().lower()
    resultados, _, _ = _generar_datos_reporte_prestamos(tipo_reporte, fecha_inicio, fecha_fin)

    if formato == 'pdf':
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas

        buffer = BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        y = height - 40

        pdf.setFont('Helvetica-Bold', 12)
        pdf.drawString(40, y, f'Reporte de Prestamos: {tipo_reporte}')
        y -= 16
        pdf.setFont('Helvetica', 9)
        pdf.drawString(40, y, f'Fecha de exportacion: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
        y -= 24

        if resultados:
            columnas = list(resultados[0].keys())
            pdf.setFont('Helvetica-Bold', 8)
            x = 40
            for col in columnas[:6]:
                pdf.drawString(x, y, str(col)[:18])
                x += 88
            y -= 14
            pdf.setFont('Helvetica', 7)

            for row in resultados:
                x = 40
                for col in columnas[:6]:
                    valor = str(row.get(col, ''))
                    pdf.drawString(x, y, valor[:18])
                    x += 88
                y -= 12
                if y < 60:
                    pdf.showPage()
                    y = height - 40
                    pdf.setFont('Helvetica', 7)
        else:
            pdf.setFont('Helvetica', 10)
            pdf.drawString(40, y, 'Sin datos para el rango seleccionado.')

        pdf.save()
        buffer.seek(0)
        filename = f"reporte_prestamos_{tipo_reporte}_{date.today().isoformat()}.pdf"
        return send_file(buffer, as_attachment=True, download_name=filename, mimetype='application/pdf')

    if formato == 'csv':
        output = StringIO()
        if resultados:
            writer = csv.DictWriter(output, fieldnames=list(resultados[0].keys()))
            writer.writeheader()
            writer.writerows(resultados)
        else:
            output.write('sin_datos\n')
        filename = f"reporte_prestamos_{tipo_reporte}_{date.today().isoformat()}.csv"
        return Response(
            output.getvalue(),
            mimetype='text/csv; charset=utf-8',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )

    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = 'Reporte Prestamos'
    if resultados:
        headers = list(resultados[0].keys())
        ws.append(headers)
        for row in resultados:
            ws.append([row.get(h) for h in headers])
    else:
        ws.append(['Sin datos'])
    file_data = BytesIO()
    wb.save(file_data)
    file_data.seek(0)
    filename = f"reporte_prestamos_{tipo_reporte}_{date.today().isoformat()}.xlsx"
    return send_file(
        file_data,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.route('/obtener_estadisticas_cobranza')
@login_required()
def obtener_estadisticas_cobranza():
    cartera = _obtener_cartera_con_alertas()
    morosos = [p for p in cartera if p['dias_atraso'] > 0 and p['estado'] == 'aprobado']
    conn = get_db()
    recuperado_mes = conn.execute(
        """
        SELECT COALESCE(SUM(monto),0)
        FROM pagos_prestamo
        WHERE strftime('%Y-%m', fecha) = strftime('%Y-%m', 'now')
        """
    ).fetchone()[0]
    acciones_pendientes = conn.execute(
        """
        SELECT COUNT(*)
        FROM cobranza_acciones
        WHERE date(fecha_compromiso) < date('now')
          AND resultado IN ('compromiso', 'sin_respuesta')
        """
    ).fetchone()[0]
    conn.close()
    return jsonify({
        'prestamos_morosos': len(morosos),
        'monto_moroso': float(sum(p['monto_vencido'] for p in morosos)),
        'acciones_pendientes': int(acciones_pendientes),
        'recuperado_mes': float(recuperado_mes or 0),
    })


@app.route('/obtener_lista_cobranza', methods=['POST'])
@login_required()
def obtener_lista_cobranza():
    data = request.get_json() or {}
    filtro = (data.get('filtro_morosidad') or 'todos').strip()
    ordenar_por = (data.get('ordenar_por') or 'dias_atraso').strip()
    filtro_socio = (data.get('filtro_socio') or '').strip().lower()
    filtro_responsable = (data.get('filtro_responsable') or '').strip().lower()

    cartera = [p for p in _obtener_cartera_con_alertas() if p['estado'] == 'aprobado' and p['dias_atraso'] > 0]

    if filtro == '1-30':
        cartera = [p for p in cartera if 1 <= p['dias_atraso'] <= 30]
    elif filtro == '31-60':
        cartera = [p for p in cartera if 31 <= p['dias_atraso'] <= 60]
    elif filtro == '61-90':
        cartera = [p for p in cartera if 61 <= p['dias_atraso'] <= 90]
    elif filtro == '90+':
        cartera = [p for p in cartera if p['dias_atraso'] > 90]

    key_map = {
        'dias_atraso': lambda x: x['dias_atraso'],
        'monto_vencido': lambda x: x['monto_vencido'],
        'fecha_ultimo_pago': lambda x: x.get('ultimo_pago') or '',
        'numero_prestamo': lambda x: x['numero'],
    }
    cartera.sort(key=key_map.get(ordenar_por, key_map['dias_atraso']), reverse=(ordenar_por != 'numero_prestamo'))

    conn = get_db()
    ult_contactos = conn.execute(
        '''
        SELECT p.numero AS numero_prestamo, MAX(ca.fecha_accion) AS ultimo_contacto
        FROM cobranza_acciones ca
        JOIN prestamos p ON ca.prestamo_id = p.id
        GROUP BY p.numero
        '''
    ).fetchall()
    ult_responsables = conn.execute(
        '''
        SELECT p.numero AS numero_prestamo, ca.responsable
        FROM cobranza_acciones ca
        JOIN prestamos p ON ca.prestamo_id = p.id
        JOIN (
            SELECT prestamo_id, MAX(id) AS ultimo_id
            FROM cobranza_acciones
            GROUP BY prestamo_id
        ) ult ON ult.ultimo_id = ca.id
        '''
    ).fetchall()
    conn.close()
    mapa_contacto = {r['numero_prestamo']: r['ultimo_contacto'] for r in ult_contactos}
    mapa_responsable = {r['numero_prestamo']: (r['responsable'] or '') for r in ult_responsables}

    respuesta = []
    for p in cartera:
        etapa = (p.get('etapa_cobranza') or 'activo').lower()
        responsable = mapa_responsable.get(p['numero'], '')

        if filtro_socio:
            texto_socio = f"{(p.get('socio_codigo') or '').lower()} {(p.get('nombre_socio') or '').lower()}"
            if filtro_socio not in texto_socio:
                continue

        if filtro_responsable and filtro_responsable not in responsable.lower():
            continue

        respuesta.append({
            'numero_prestamo': p['numero'],
            'nombre_socio': p['nombre_socio'],
            'dias_atraso': int(p['dias_atraso']),
            'monto_vencido': float(p['monto_vencido']),
            'ultimo_pago': p['ultimo_pago'],
            'ultimo_contacto': mapa_contacto.get(p['numero']),
            'responsable': responsable or 'Sin asignar',
            'estado_cobranza': etapa,
        })

    return jsonify({'prestamos': respuesta})


@app.route('/guardar_accion_cobranza', methods=['POST'])
@login_required()
@permission_required('cobranza.gestion')
def guardar_accion_cobranza():
    data = request.get_json() or {}
    numero_prestamo = (data.get('numero_prestamo') or '').strip()
    if not numero_prestamo:
        return jsonify({'success': False, 'error': 'Número de préstamo requerido'}), 400

    conn = get_db()
    prestamo = conn.execute('SELECT id, numero FROM prestamos WHERE numero=?', (numero_prestamo,)).fetchone()
    if not prestamo:
        conn.close()
        return jsonify({'success': False, 'error': 'Préstamo no encontrado'}), 404

    conn.execute(
        '''
        INSERT INTO cobranza_acciones
        (prestamo_id, tipo_accion, resultado, notas, monto_comprometido, fecha_compromiso, fecha_accion, responsable)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            prestamo['id'],
            data.get('tipo_accion', 'llamada'),
            data.get('resultado', 'sin_respuesta'),
            data.get('notas', ''),
            float(data.get('monto_comprometido', 0) or 0),
            data.get('fecha_compromiso') or None,
            data.get('fecha_accion') or datetime.now().isoformat(),
            session.get('username', 'operador'),
        ),
    )
    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='prestamos',
        entidad='cobranza_accion',
        entidad_id=prestamo['id'],
        accion='crear',
        descripcion=f'Acción de cobranza para préstamo {numero_prestamo}',
        datos=data
    )

    return jsonify({'success': True})


@app.route('/obtener_historial_cobranza')
@login_required()
def obtener_historial_cobranza():
    responsable = request.args.get('responsable', '').strip().lower()
    numero = request.args.get('numero_prestamo', '').strip().lower()

    filtros = 'WHERE 1=1'
    params = []
    if responsable:
        filtros += ' AND lower(COALESCE(ca.responsable, "")) LIKE ?'
        params.append(f'%{responsable}%')
    if numero:
        filtros += ' AND lower(p.numero) LIKE ?'
        params.append(f'%{numero}%')

    conn = get_db()
    rows = conn.execute(
        f'''
        SELECT ca.fecha_accion AS fecha,
               p.numero AS numero_prestamo,
               ca.tipo_accion,
               ca.resultado,
               ca.responsable
        FROM cobranza_acciones ca
        JOIN prestamos p ON p.id = ca.prestamo_id
        {filtros}
        ORDER BY ca.id DESC
        LIMIT 200
        ''',
        params
    ).fetchall()
    conn.close()
    return jsonify({'historial': [dict(r) for r in rows]})


@app.route('/auditoria_eventos')
@login_required(role=('Administrador',))
def auditoria_eventos():
    modulo = request.args.get('modulo', '').strip().lower()
    entidad = request.args.get('entidad', '').strip().lower()
    usuario = request.args.get('usuario', '').strip().lower()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()

    query = 'SELECT * FROM auditoria_eventos WHERE 1=1'
    params = []

    if modulo:
        query += ' AND lower(modulo) = ?'
        params.append(modulo)
    if entidad:
        query += ' AND lower(entidad) LIKE ?'
        params.append(f'%{entidad}%')
    if usuario:
        query += ' AND lower(COALESCE(usuario, "")) LIKE ?'
        params.append(f'%{usuario}%')
    if fecha_desde:
        query += ' AND date(fecha) >= date(?)'
        params.append(fecha_desde)
    if fecha_hasta:
        query += ' AND date(fecha) <= date(?)'
        params.append(fecha_hasta)

    query += ' ORDER BY id DESC LIMIT 500'

    conn = get_db()
    eventos = conn.execute(query, params).fetchall()
    conn.close()

    return render_template(
        'auditoria_eventos.html',
        eventos=eventos,
        filtros={
            'modulo': modulo,
            'entidad': request.args.get('entidad', '').strip(),
            'usuario': request.args.get('usuario', '').strip(),
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta,
        }
    )


@app.route('/enviar_recordatorios_cobranza', methods=['POST'])
@login_required()
@permission_required('cobranza.recordatorios')
def enviar_recordatorios_cobranza():
    data = request.get_json() or {}
    numeros = data.get('prestamos', [])
    if not numeros:
        return jsonify({'message': 'No se seleccionaron préstamos.'}), 400

    conn = get_db()
    enviados = 0
    for numero in numeros:
        prestamo = conn.execute('SELECT id FROM prestamos WHERE numero=?', (numero,)).fetchone()
        if not prestamo:
            continue
        conn.execute(
            '''
            INSERT INTO cobranza_acciones
            (prestamo_id, tipo_accion, resultado, notas, fecha_accion, responsable)
            VALUES (?, 'recordatorio', 'contactado', 'Recordatorio automático generado desde panel', ?, ?)
            ''',
            (prestamo['id'], datetime.now().isoformat(), session.get('username', 'operador')),
        )
        enviados += 1
    conn.commit()
    conn.close()
    return jsonify({'message': f'Recordatorios generados para {enviados} préstamos.'})


@app.route('/marcar_revision_legal', methods=['POST'])
@login_required()
@permission_required('cobranza.legal')
def marcar_revision_legal():
    data = request.get_json() or {}
    numeros = data.get('prestamos', [])
    if not numeros:
        return jsonify({'message': 'No se seleccionaron préstamos.'}), 400

    conn = get_db()
    marcados = 0
    for numero in numeros:
        cur = conn.execute("UPDATE prestamos SET etapa_cobranza='legal' WHERE numero=?", (numero,))
        marcados += cur.rowcount
    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='prestamos',
        entidad='prestamo',
        accion='marcar_legal',
        descripcion='Préstamos marcados para revisión legal',
        datos={'prestamos': numeros, 'total': marcados}
    )

    return jsonify({'message': f'{marcados} préstamos enviados a revisión legal.'})


@app.route('/cierres_periodo', methods=['GET', 'POST'])
@login_required(role=('Administrador',))
def cierres_periodo():
    conn = get_db()

    if request.method == 'POST':
        modulo = request.form.get('modulo', '').strip()
        fecha_inicio = request.form.get('fecha_inicio', '').strip()
        fecha_fin = request.form.get('fecha_fin', '').strip()
        observaciones = request.form.get('observaciones', '').strip()

        if modulo not in ('ahorro', 'prestamos') or not fecha_inicio or not fecha_fin:
            conn.close()
            flash('Debe completar módulo, fecha inicio y fecha fin.', 'danger')
            return redirect(url_for('cierres_periodo'))

        conn.execute(
            '''
            INSERT INTO cierres_periodo (modulo, fecha_inicio, fecha_fin, estado, observaciones, usuario, fecha_creacion)
            VALUES (?, ?, ?, 'cerrado', ?, ?, ?)
            ''',
            (modulo, fecha_inicio, fecha_fin, observaciones, session.get('username'), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()

        log_auditoria_evento(
            modulo=modulo,
            entidad='cierre_periodo',
            accion='crear',
            descripcion=f'Cierre de periodo {modulo} {fecha_inicio} a {fecha_fin}',
            datos={'observaciones': observaciones}
        )
        flash('Cierre de periodo registrado correctamente.', 'success')
        return redirect(url_for('cierres_periodo'))

    cierres = conn.execute(
        "SELECT * FROM cierres_periodo ORDER BY id DESC LIMIT 100"
    ).fetchall()
    conn.close()
    return render_template('cierres_periodo.html', cierres=cierres)


@app.route('/socios/<int:sid>/estado_cuenta_prestamo')
@login_required()
def estado_cuenta_prestamo(sid):
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()
    export = request.args.get('export', '').strip().lower()
    printable = request.args.get('print', '').strip() == '1'

    conn = get_db()
    socio = conn.execute('SELECT * FROM socios WHERE id=?', (sid,)).fetchone()
    if not socio:
        conn.close()
        flash('Socio no encontrado.', 'danger')
        return redirect(url_for('socios'))

    filtros = ''
    params = [sid]
    if fecha_desde:
        filtros += ' AND date(pp.fecha) >= date(?)'
        params.append(fecha_desde)
    if fecha_hasta:
        filtros += ' AND date(pp.fecha) <= date(?)'
        params.append(fecha_hasta)

    pagos = conn.execute(
        f'''
        SELECT pp.*, p.numero AS numero_prestamo
        FROM pagos_prestamo pp
        JOIN prestamos p ON p.id = pp.prestamo_id
        WHERE p.socio_id=? {filtros}
        ORDER BY date(pp.fecha) DESC, pp.id DESC
        ''',
        params,
    ).fetchall()

    resumen = conn.execute(
        '''
        SELECT COUNT(*) AS total_prestamos,
               COALESCE(SUM(CASE WHEN estado='aprobado' THEN saldo_pendiente ELSE 0 END),0) AS saldo_activo,
               COALESCE(SUM(CASE WHEN estado='pagado' THEN 1 ELSE 0 END),0) AS prestamos_cancelados
        FROM prestamos
        WHERE socio_id=?
        ''',
        (sid,),
    ).fetchone()
    conn.close()

    total_pagado = sum(float(p['monto'] or 0) for p in pagos)

    if export == 'csv':
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Fecha', 'Prestamo', 'Monto', 'Capital', 'Interes', 'Saldo Restante', 'Comprobante', 'Boleta'])
        for p in pagos:
            writer.writerow([
                p['fecha'], p['numero_prestamo'], p['monto'], p['capital'], p['interes'], p['saldo_restante'],
                p['numero_comprobante'] or '', p['boleta_deposito'] or ''
            ])
        filename = f"estado_cuenta_prestamo_{socio['codigo']}_{date.today().isoformat()}.csv"
        return Response(
            output.getvalue(),
            mimetype='text/csv; charset=utf-8',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )

    if export == 'excel':
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = 'Estado Cuenta'
        ws.append(['Fecha', 'Prestamo', 'Monto', 'Capital', 'Interes', 'Saldo Restante', 'Comprobante', 'Boleta'])
        for p in pagos:
            ws.append([
                p['fecha'], p['numero_prestamo'], float(p['monto'] or 0), float(p['capital'] or 0),
                float(p['interes'] or 0), float(p['saldo_restante'] or 0), p['numero_comprobante'] or '', p['boleta_deposito'] or ''
            ])
        mem = BytesIO()
        wb.save(mem)
        mem.seek(0)
        filename = f"estado_cuenta_prestamo_{socio['codigo']}_{date.today().isoformat()}.xlsx"
        return send_file(
            mem,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    return render_template(
        'estado_cuenta_prestamo.html',
        socio=socio,
        pagos=pagos,
        resumen=resumen,
        total_pagado=total_pagado,
        filtros={'fecha_desde': fecha_desde, 'fecha_hasta': fecha_hasta},
        printable=printable,
    )


@app.route('/prestamos/comprobante/<int:pago_id>')
@login_required()
def comprobante_pago_prestamo(pago_id):
    conn = get_db()
    pago = conn.execute(
        '''
        SELECT pp.*, p.numero AS numero_prestamo,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre
        FROM pagos_prestamo pp
        JOIN prestamos p ON p.id = pp.prestamo_id
        JOIN socios s ON s.id = p.socio_id
        WHERE pp.id=?
        ''',
        (pago_id,),
    ).fetchone()
    conn.close()

    if not pago:
        flash('Comprobante no encontrado.', 'danger')
        return redirect(url_for('prestamos'))

    return render_template('comprobante_prestamo.html', pago=pago)

@app.route('/generar_planilla_ahorro')
@login_required()
def generar_planilla_ahorro():
    conn = get_db_connection()
    c = conn.cursor()

    # Obtener todas las cuentas activas con información del socio
    c.execute('''
        SELECT c.id, c.numero, c.saldo, s.nombre, s.apellido, s.codigo
        FROM cuentas c
        JOIN socios s ON c.socio_id = s.id
        WHERE c.estado = 'activa' AND s.estado = 'activo'
        ORDER BY s.apellido, s.nombre
    ''')

    cuentas = c.fetchall()
    conn.close()

    return render_template('planilla_ahorro.html', cuentas=cuentas)


@app.route('/planilla_ahorro')
@login_required()
def planilla_ahorro():
    return redirect(url_for('generar_planilla_ahorro'))


@app.route('/planillas_ahorro_pendientes')
@login_required()
def planillas_ahorro_pendientes():
    conn = get_db_connection()
    nombre = request.args.get('nombre', '').strip()
    frecuencia = request.args.get('frecuencia', '').strip()
    estado = request.args.get('estado', '').strip().lower()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()

    query = '''
        SELECT * FROM planillas_masivas
        WHERE tipo = 'ahorro_cuotas'
    '''
    params = []

    if nombre:
        query += ' AND nombre LIKE ?'
        params.append(f'%{nombre}%')
    if frecuencia:
        query += ' AND frecuencia = ?'
        params.append(frecuencia)
    if estado:
        query += ' AND estado = ?'
        params.append(estado)
    if fecha_desde:
        query += ' AND date(fecha_pago) >= date(?)'
        params.append(fecha_desde)
    if fecha_hasta:
        query += ' AND date(fecha_pago) <= date(?)'
        params.append(fecha_hasta)

    query += '''
        ORDER BY CASE estado
            WHEN 'pendiente' THEN 1
            WHEN 'parcial' THEN 2
            WHEN 'aplicada' THEN 3
            ELSE 4
        END, fecha_creacion DESC, id DESC
    '''

    planillas_rows = conn.execute(query, params).fetchall()
    conn.close()

    planillas = []
    for row in planillas_rows:
        item = dict(row)
        item['tipo_cuenta_label'] = obtener_tipo_cuenta_desde_planilla(item.get('nombre'))
        planillas.append(item)

    return render_template(
        'planillas_ahorro_pendientes.html',
        planillas=planillas,
        filtros={
            'nombre': nombre,
            'frecuencia': frecuencia,
            'estado': estado,
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta
        },
        total_planillas=len(planillas),
        total_monto=sum(float(p['total_monto'] or 0) for p in planillas)
    )


@app.route('/planillas_ahorro/<int:planilla_id>')
@login_required()
def detalle_planilla_ahorro(planilla_id):
    conn = get_db_connection()
    planilla = conn.execute('''
        SELECT * FROM planillas_masivas
        WHERE id=? AND tipo='ahorro_cuotas'
    ''', (planilla_id,)).fetchone()

    if not planilla:
        conn.close()
        flash('Planilla de ahorro no encontrada.', 'danger')
        return redirect(url_for('planillas_ahorro_pendientes'))

    detalles = conn.execute('''
        SELECT d.*, c.saldo AS saldo_actual
        FROM planilla_masiva_detalles d
        LEFT JOIN cuentas c ON d.referencia_id = c.id AND d.referencia_tipo = 'cuenta_ahorro'
        WHERE d.planilla_id=?
        ORDER BY socio_nombre, numero_referencia
    ''', (planilla_id,)).fetchall()
    conn.close()

    return render_template(
        'planilla_cuotas_ahorro.html',
        planilla=planilla,
        detalles=detalles,
        nombre_planilla=planilla['nombre'],
        tipo_cuenta_label=obtener_tipo_cuenta_desde_planilla(planilla['nombre']),
        frecuencia=planilla['frecuencia'],
        fecha_pago=planilla['fecha_pago'],
        total_cuotas=planilla['total_monto'] or 0
    )


@app.route('/planillas_ahorro/<int:planilla_id>/editar', methods=['GET', 'POST'])
@login_required()
def editar_planilla_ahorro(planilla_id):
    conn = get_db_connection()
    planilla = conn.execute('''
        SELECT * FROM planillas_masivas
        WHERE id=? AND tipo='ahorro_cuotas'
    ''', (planilla_id,)).fetchone()

    if not planilla:
        conn.close()
        flash('Planilla de ahorro no encontrada.', 'danger')
        return redirect(url_for('planillas_ahorro_pendientes'))

    if planilla['estado'] == 'aplicada':
        conn.close()
        flash('No se puede modificar una planilla ya aplicada.', 'warning')
        return redirect(url_for('detalle_planilla_ahorro', planilla_id=planilla_id))

    detalles = conn.execute(
        '''
        SELECT id, socio_codigo, socio_nombre, numero_referencia, monto, estado
        FROM planilla_masiva_detalles
        WHERE planilla_id=?
        ORDER BY socio_nombre, numero_referencia
        ''',
        (planilla_id,)
    ).fetchall()

    if request.method == 'POST':
        nombre = request.form.get('nombre_planilla', '').strip()
        fecha_pago = request.form.get('fecha_pago', '').strip()
        frecuencia = request.form.get('frecuencia', '').strip()
        accion = request.form.get('accion', 'guardar').strip().lower()

        if periodo_cerrado('ahorro', fecha_pago):
            conn.close()
            flash('No se puede modificar la planilla porque el periodo de ahorro está cerrado.', 'warning')
            return redirect(url_for('detalle_planilla_ahorro', planilla_id=planilla_id))

        if not nombre or not fecha_pago or frecuencia not in ('Quincenal', 'Catorcenal'):
            conn.close()
            flash('Debe completar nombre, fecha de pago y frecuencia valida.', 'danger')
            return redirect(url_for('editar_planilla_ahorro', planilla_id=planilla_id))

        if accion == 'recalcular':
            detalles_recalculo = conn.execute(
                '''
                SELECT d.id, d.monto, d.estado, d.referencia_id,
                       s.cuota_ahorro
                FROM planilla_masiva_detalles d
                LEFT JOIN cuentas c ON c.id = d.referencia_id AND d.referencia_tipo = 'cuenta_ahorro'
                LEFT JOIN socios s ON s.id = c.socio_id
                WHERE d.planilla_id=?
                ''',
                (planilla_id,)
            ).fetchall()

            for d in detalles_recalculo:
                if (d['estado'] or '').lower() != 'pendiente':
                    continue
                nueva_cuota = round(float(d['cuota_ahorro'] or 0), 2)
                if nueva_cuota < 0:
                    nueva_cuota = 0
                conn.execute(
                    '''
                    UPDATE planilla_masiva_detalles
                    SET monto=?
                    WHERE id=? AND planilla_id=?
                    ''',
                    (nueva_cuota, d['id'], planilla_id)
                )
        else:
            detalle_ids = request.form.getlist('detalle_id[]')
            detalle_montos = request.form.getlist('detalle_monto[]')
            if len(detalle_ids) != len(detalle_montos):
                conn.close()
                flash('Los datos de cuotas programadas son inconsistentes.', 'danger')
                return redirect(url_for('editar_planilla_ahorro', planilla_id=planilla_id))

            for detalle_id, monto_str in zip(detalle_ids, detalle_montos):
                try:
                    monto_valor = round(float(monto_str or 0), 2)
                except Exception:
                    conn.close()
                    flash('Cada cuota programada debe tener un monto valido.', 'danger')
                    return redirect(url_for('editar_planilla_ahorro', planilla_id=planilla_id))

                if monto_valor < 0:
                    conn.close()
                    flash('La cuota programada no puede ser negativa.', 'danger')
                    return redirect(url_for('editar_planilla_ahorro', planilla_id=planilla_id))

                conn.execute(
                    '''
                    UPDATE planilla_masiva_detalles
                    SET monto=?
                    WHERE id=? AND planilla_id=? AND estado='pendiente'
                    ''',
                    (monto_valor, detalle_id, planilla_id)
                )

        total_monto = conn.execute(
            '''
            SELECT COALESCE(SUM(monto), 0)
            FROM planilla_masiva_detalles
            WHERE planilla_id=?
            ''',
            (planilla_id,)
        ).fetchone()[0]

        conn.execute('''
            UPDATE planillas_masivas
            SET nombre=?, fecha_pago=?, frecuencia=?, total_monto=?
            WHERE id=?
        ''', (nombre, fecha_pago, frecuencia, round(total_monto, 2), planilla_id))
        conn.commit()
        conn.close()
        log_auditoria_evento(
            modulo='ahorro',
            entidad='planilla_masiva',
            entidad_id=planilla_id,
            accion='editar',
            descripcion='Planilla de ahorro modificada',
            datos={
                'nombre': nombre,
                'fecha_pago': fecha_pago,
                'frecuencia': frecuencia,
                'total_monto': round(float(total_monto or 0), 2),
                'accion': accion,
            }
        )
        if accion == 'recalcular':
            flash('Cuotas recalculadas con la cuota de ahorro vigente y planilla actualizada.', 'success')
            return redirect(url_for('detalle_planilla_ahorro', planilla_id=planilla_id))
        else:
            flash('Planilla de ahorro actualizada correctamente.', 'success')
            return redirect(url_for('planillas_ahorro_pendientes'))

    conn.close()
    return render_template('editar_planilla_ahorro.html', planilla=planilla, detalles=detalles)


@app.route('/planillas_ahorro/<int:planilla_id>/eliminar', methods=['POST'])
@login_required()
def eliminar_planilla_ahorro(planilla_id):
    conn = get_db_connection()
    planilla = conn.execute('''
        SELECT * FROM planillas_masivas
        WHERE id=? AND tipo='ahorro_cuotas'
    ''', (planilla_id,)).fetchone()

    if not planilla:
        conn.close()
        flash('Planilla de ahorro no encontrada.', 'danger')
        return redirect(url_for('planillas_ahorro_pendientes'))

    if planilla['estado'] == 'aplicada':
        conn.close()
        flash('No se puede eliminar una planilla ya aplicada.', 'warning')
        return redirect(url_for('planillas_ahorro_pendientes'))

    conn.execute('DELETE FROM planilla_masiva_detalles WHERE planilla_id=?', (planilla_id,))
    conn.execute('DELETE FROM planillas_masivas WHERE id=?', (planilla_id,))
    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='ahorro',
        entidad='planilla_masiva',
        entidad_id=planilla_id,
        accion='eliminar',
        descripcion='Planilla de ahorro eliminada'
    )

    flash('Planilla de ahorro eliminada correctamente.', 'success')
    return redirect(url_for('planillas_ahorro_pendientes'))

@app.route('/generar_planilla_cuotas_ahorro', methods=['GET', 'POST'])
@login_required()
def generar_planilla_cuotas_ahorro():
    form_data = {
        'nombre_planilla': '',
        'frecuencia': 'Quincenal',
        'fecha_pago': date.today().isoformat(),
        'tipo_cuenta': 'ahorro_corriente',
    }

    if request.method == 'POST':
        nombre_planilla = request.form.get('nombre_planilla', '').strip()
        frecuencia = request.form.get('frecuencia', '').strip()
        fecha_pago = request.form.get('fecha_pago', '').strip()
        tipo_cuenta = request.form.get('tipo_cuenta', '').strip()

        form_data = {
            'nombre_planilla': nombre_planilla,
            'frecuencia': frecuencia or 'Quincenal',
            'fecha_pago': fecha_pago or date.today().isoformat(),
            'tipo_cuenta': tipo_cuenta or 'ahorro_corriente',
        }

        if not nombre_planilla or not frecuencia or not fecha_pago or not tipo_cuenta:
            flash('Todos los campos son obligatorios.', 'danger')
            return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

        if frecuencia not in ('Quincenal', 'Catorcenal'):
            flash('Frecuencia no valida.', 'danger')
            return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

        tipos_validos = {'ahorro_aportacion', 'ahorro_corriente', 'ahorro_plazo_fijo'}
        if tipo_cuenta not in tipos_validos:
            flash('Tipo de cuenta no valido.', 'danger')
            return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

        conn = get_db_connection()
        c = conn.cursor()

        filtro_tipo = "AND COALESCE(c.producto_ahorro, 'ahorro_corriente') = ?"
        params = [frecuencia, tipo_cuenta]

        # Obtener socios con cuota de ahorro > 1, frecuencia y tipo de cuenta configurados
        c.execute(
            f'''
            SELECT c.id, c.numero, c.saldo, s.nombre, s.apellido, s.codigo,
                   s.cuota_ahorro, s.frecuencia
            FROM cuentas c
            JOIN socios s ON c.socio_id = s.id
            WHERE c.estado = 'activa'
              AND c.tipo = 'ahorro'
              AND s.estado = 'activo'
              AND s.cuota_ahorro > 1
              AND s.frecuencia = ?
              {filtro_tipo}
            ORDER BY s.apellido, s.nombre
            ''',
            params
        )

        cuentas = c.fetchall()

        # Calcular total de cuotas
        total_cuotas = sum(cuenta['cuota_ahorro'] for cuenta in cuentas)

        if not cuentas:
            conn.close()
            flash('No se encontraron socios para generar la planilla con los filtros seleccionados.', 'warning')
            return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

        tipo_label = {
            'ahorro_aportacion': 'Aportacion',
            'ahorro_corriente': 'Ahorro corriente',
            'ahorro_plazo_fijo': 'Plazo fijo',
        }.get(tipo_cuenta, tipo_cuenta)

        nombre_planilla_guardado = f"{nombre_planilla} [{tipo_label}]"

        c.execute('''
            INSERT INTO planillas_masivas
            (tipo, nombre, fecha_pago, frecuencia, estado, total_monto, total_registros, fecha_creacion, usuario_creacion)
            VALUES (?, ?, ?, ?, 'pendiente', ?, ?, ?, ?)
        ''', (
            'ahorro_cuotas', nombre_planilla_guardado, fecha_pago, frecuencia,
            total_cuotas, len(cuentas), datetime.now().isoformat(), session.get('username')
        ))
        planilla_id = c.lastrowid

        for cuenta in cuentas:
            c.execute('''
                INSERT INTO planilla_masiva_detalles
                (planilla_id, referencia_tipo, referencia_id, numero_referencia, socio_codigo, socio_nombre, monto, estado)
                VALUES (?, 'cuenta_ahorro', ?, ?, ?, ?, ?, 'pendiente')
            ''', (
                planilla_id,
                cuenta['id'],
                cuenta['numero'],
                cuenta['codigo'],
                f"{cuenta['nombre']} {cuenta['apellido']}",
                cuenta['cuota_ahorro']
            ))

        conn.commit()
        conn.close()
        flash('Planilla generada y guardada como pendiente.', 'success')
        return redirect(url_for('detalle_planilla_ahorro', planilla_id=planilla_id))

    return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

@app.route('/planilla_prestamos')
@login_required()
def planilla_prestamos():
    return redirect(url_for('planillas_prestamos_pendientes'))


@app.route('/planillas_prestamos_pendientes')
@login_required()
def planillas_prestamos_pendientes():
    conn = get_db_connection()
    nombre = request.args.get('nombre', '').strip()
    frecuencia = request.args.get('frecuencia', '').strip()
    estado = request.args.get('estado', '').strip().lower()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()

    query = '''
        SELECT * FROM planillas_masivas
        WHERE tipo = 'prestamo_cuotas'
    '''
    params = []

    if nombre:
        query += ' AND nombre LIKE ?'
        params.append(f'%{nombre}%')
    if frecuencia:
        query += ' AND frecuencia = ?'
        params.append(frecuencia)
    if estado:
        query += ' AND estado = ?'
        params.append(estado)
    if fecha_desde:
        query += ' AND date(fecha_pago) >= date(?)'
        params.append(fecha_desde)
    if fecha_hasta:
        query += ' AND date(fecha_pago) <= date(?)'
        params.append(fecha_hasta)

    query += '''
        ORDER BY CASE estado
            WHEN 'pendiente' THEN 1
            WHEN 'parcial' THEN 2
            WHEN 'aplicada' THEN 3
            ELSE 4
        END, fecha_creacion DESC, id DESC
    '''

    planillas = conn.execute(query, params).fetchall()
    conn.close()
    return render_template(
        'planillas_prestamos_pendientes.html',
        planillas=planillas,
        filtros={
            'nombre': nombre,
            'frecuencia': frecuencia,
            'estado': estado,
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta
        },
        total_planillas=len(planillas),
        total_monto=sum(float(p['total_monto'] or 0) for p in planillas)
    )


@app.route('/planillas_prestamos/<int:planilla_id>')
@login_required()
def detalle_planilla_prestamos(planilla_id):
    conn = get_db_connection()
    planilla = conn.execute('''
        SELECT * FROM planillas_masivas
        WHERE id=? AND tipo='prestamo_cuotas'
    ''', (planilla_id,)).fetchone()

    if not planilla:
        conn.close()
        flash('Planilla de prestamos no encontrada.', 'danger')
        return redirect(url_for('planillas_prestamos_pendientes'))

    detalles = conn.execute('''
        SELECT d.*, p.monto_aprobado, p.saldo_pendiente, p.cuota_mensual,
               COALESCE(pp.capital_pagado, 0) AS capital_pagado,
               COALESCE(pp.interes_pagado, 0) AS interes_pagado
        FROM planilla_masiva_detalles d
        LEFT JOIN prestamos p ON d.referencia_id = p.id AND d.referencia_tipo = 'prestamo'
        LEFT JOIN (
            SELECT prestamo_id,
                   SUM(capital) AS capital_pagado,
                   SUM(interes) AS interes_pagado
            FROM pagos_prestamo
            GROUP BY prestamo_id
        ) pp ON pp.prestamo_id = p.id
        WHERE d.planilla_id=?
        ORDER BY socio_nombre, numero_referencia
    ''', (planilla_id,)).fetchall()
    conn.close()

    return render_template(
        'planilla_prestamos.html',
        planilla=planilla,
        detalles=detalles,
        nombre_planilla=planilla['nombre'],
        fecha_pago=planilla['fecha_pago'],
        boleta_deposito=planilla['boleta_deposito'],
        frecuencia=planilla['frecuencia']
    )

@app.route('/generar_planilla_prestamos', methods=['GET', 'POST'])
@login_required()
def generar_planilla_prestamos():
    form_data = {
        'nombre_planilla': '',
        'fecha_pago': date.today().isoformat(),
        'frecuencia': 'Quincenal',
    }

    if request.method == 'POST':
        nombre_planilla = request.form.get('nombre_planilla', '').strip()
        fecha_pago = request.form.get('fecha_pago', '').strip()
        frecuencia = request.form.get('frecuencia', '').strip()

        form_data = {
            'nombre_planilla': nombre_planilla,
            'fecha_pago': fecha_pago or date.today().isoformat(),
            'frecuencia': frecuencia or 'Quincenal',
        }

        if not nombre_planilla or not fecha_pago or not frecuencia:
            flash('Todos los campos son obligatorios.', 'danger')
            return render_template('generar_planilla_prestamos.html', form_data=form_data)

        if frecuencia not in ('Quincenal', 'Catorcenal'):
            flash('Frecuencia no valida.', 'danger')
            return render_template('generar_planilla_prestamos.html', form_data=form_data)

        conn = get_db_connection()
        c = conn.cursor()

        # Obtener prestamos activos filtrados por frecuencia del socio.
        c.execute('''
            SELECT p.id, p.numero, p.monto_aprobado, p.saldo_pendiente, p.cuota_mensual,
                   p.tasa_interes, p.plazo_meses, s.id AS socio_id, s.nombre, s.apellido,
                   s.codigo, s.frecuencia, COUNT(pp.id) AS cuotas_pagadas
            FROM prestamos p
            JOIN socios s ON p.socio_id = s.id
            LEFT JOIN pagos_prestamo pp ON p.id = pp.prestamo_id
            WHERE p.estado = 'aprobado'
              AND p.saldo_pendiente > 0
              AND s.estado = 'activo'
              AND s.frecuencia = ?
            GROUP BY p.id
            ORDER BY s.apellido, s.nombre
        ''', (frecuencia,))

        prestamos = c.fetchall()

        if not prestamos:
            conn.close()
            flash('No se encontraron prestamos para generar la planilla con los filtros seleccionados.', 'warning')
            return render_template('generar_planilla_prestamos.html', form_data=form_data)

        total_planilla = sum(min(float(prestamo['cuota_mensual'] or 0), float(prestamo['saldo_pendiente'] or 0)) for prestamo in prestamos)

        c.execute('''
            INSERT INTO planillas_masivas
            (tipo, nombre, fecha_pago, frecuencia, estado, total_monto, total_registros, fecha_creacion, usuario_creacion)
            VALUES (?, ?, ?, ?, 'pendiente', ?, ?, ?, ?)
        ''', (
            'prestamo_cuotas', nombre_planilla, fecha_pago, frecuencia,
            total_planilla, len(prestamos), datetime.now().isoformat(), session.get('username')
        ))
        planilla_id = c.lastrowid

        for prestamo in prestamos:
            monto_programado = min(float(prestamo['cuota_mensual'] or 0), float(prestamo['saldo_pendiente'] or 0))
            c.execute('''
                INSERT INTO planilla_masiva_detalles
                (planilla_id, referencia_tipo, referencia_id, numero_referencia, socio_codigo, socio_nombre, monto, estado)
                VALUES (?, 'prestamo', ?, ?, ?, ?, ?, 'pendiente')
            ''', (
                planilla_id,
                prestamo['id'],
                prestamo['numero'],
                prestamo['codigo'],
                f"{prestamo['nombre']} {prestamo['apellido']}",
                monto_programado
            ))

        conn.commit()

        conn.close()
        flash('Planilla de prestamos generada y guardada como pendiente.', 'success')
        return redirect(url_for('detalle_planilla_prestamos', planilla_id=planilla_id))

    return render_template('generar_planilla_prestamos.html', form_data=form_data)

@app.route('/procesar_abonos_masivos', methods=['POST'])
@login_required()
@permission_required('ahorro.masivo')
def procesar_abonos_masivos():
    data = request.get_json()
    planilla_id = data.get('planilla_id')
    abonos = data.get('abonos', [])
    fecha_pago = data.get('fecha', datetime.now().isoformat())
    nombre_planilla = data.get('nombre_planilla', 'Abono masivo')
    boleta_deposito = data.get('boleta_deposito', '').strip()
    frecuencia = data.get('frecuencia', '').strip()

    if periodo_cerrado('ahorro', fecha_pago):
        return jsonify({'error': 'El periodo de ahorro está cerrado para la fecha indicada.'}), 400

    if not boleta_deposito:
        return jsonify({'error': 'Debe indicar numero de boleta de pago para aplicar la planilla.'}), 400
    
    conn = get_db_connection()
    if not validate_idempotency(conn, 'procesar_abonos_masivos'):
        conn.close()
        return jsonify({'error': 'Solicitud duplicada detectada (idempotencia).'}), 409
    c = conn.cursor()

    planilla = None
    detalles_planilla = []
    if planilla_id:
        planilla = c.execute('''
            SELECT * FROM planillas_masivas
            WHERE id=? AND tipo='ahorro_cuotas'
        ''', (planilla_id,)).fetchone()

        if not planilla:
            conn.close()
            return jsonify({'error': 'La planilla seleccionada no existe.'}), 404

        if planilla['estado'] == 'aplicada':
            conn.close()
            return jsonify({'error': 'La planilla ya fue aplicada anteriormente.'}), 400

        detalles_planilla = c.execute('''
            SELECT * FROM planilla_masiva_detalles
            WHERE planilla_id=? AND estado='pendiente'
        ''', (planilla_id,)).fetchall()
        abonos = [
            {
                'cuenta_id': detalle['referencia_id'],
                'numero': detalle['numero_referencia'],
                'monto': detalle['monto'],
                'detalle_id': detalle['id']
            }
            for detalle in detalles_planilla
        ]
        nombre_planilla = planilla['nombre']
        fecha_pago = planilla['fecha_pago']
        frecuencia = planilla['frecuencia'] or frecuencia
    
    procesados = 0
    errores = []
    
    for abono in abonos:
        try:
            cuenta_id = abono['cuenta_id']
            monto = float(abono['monto'])
            
            if monto <= 0:
                errores.append(f"Monto inválido para cuenta {abono.get('numero', cuenta_id)}")
                continue
            
            # Obtener información de la cuenta y socio
            c.execute('SELECT c.saldo, c.socio_id FROM cuentas c WHERE c.id = ?', (cuenta_id,))
            cuenta = c.fetchone()
            
            if not cuenta:
                errores.append(f"Cuenta {abono.get('numero', cuenta_id)} no encontrada")
                continue
            
            nuevo_saldo = cuenta[0] + monto
            
            # Actualizar saldo
            c.execute('UPDATE cuentas SET saldo = ? WHERE id = ?', (nuevo_saldo, cuenta_id))
            
            # Registrar transacción
            descripcion_planilla = f"Planilla: {nombre_planilla}"
            if boleta_deposito:
                descripcion_planilla += f" | Boleta: {boleta_deposito}"
            if frecuencia:
                descripcion_planilla += f" | Frecuencia: {frecuencia}"

            c.execute('''
                INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha)
                VALUES (?, 'deposito', ?, ?, ?, ?)
            ''', (cuenta_id, monto, nuevo_saldo, descripcion_planilla, fecha_pago))

            if abono.get('detalle_id'):
                c.execute("UPDATE planilla_masiva_detalles SET estado='aplicado' WHERE id=?", (abono['detalle_id'],))
            
            procesados += 1
            
        except Exception as e:
            errores.append(f"Error procesando cuenta {abono.get('numero', cuenta_id)}: {str(e)}")
    
    if planilla_id and planilla:
        estado_final = 'aplicada' if procesados == len(abonos) and not errores else ('parcial' if procesados > 0 else 'pendiente')
        c.execute('''
            UPDATE planillas_masivas
            SET estado=?, boleta_deposito=?, fecha_aplicacion=?, usuario_aplicacion=?
            WHERE id=?
        ''', (estado_final, boleta_deposito, datetime.now().isoformat(), session.get('username'), planilla_id))

    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='ahorro',
        entidad='planilla_masiva',
        entidad_id=planilla_id,
        accion='aplicar',
        descripcion='Aplicación de abonos masivos',
        datos={'procesados': procesados, 'errores': len(errores), 'boleta': boleta_deposito}
    )
    
    return jsonify({
        'procesados': procesados,
        'errores': errores,
        'total': len(abonos),
        'planilla_id': planilla_id
    })

@app.route('/procesar_pagos_masivos', methods=['POST'])
@login_required()
@permission_required('prestamos.masivo')
def procesar_pagos_masivos():
    data = request.get_json()
    planilla_id = data.get('planilla_id')
    pagos = data.get('pagos', [])
    fecha_pago = data.get('fecha_pago', date.today().isoformat())
    nombre_planilla = data.get('nombre_planilla', 'Planilla de prestamos').strip()
    boleta_deposito = data.get('boleta_deposito', '').strip()
    frecuencia = data.get('frecuencia', '').strip()

    if periodo_cerrado('prestamos', fecha_pago):
        return jsonify({'error': 'El periodo de préstamos está cerrado para la fecha indicada.'}), 400

    if not boleta_deposito:
        return jsonify({'error': 'Debe indicar numero de boleta de pago para aplicar la planilla.'}), 400
    
    conn = get_db_connection()
    if not validate_idempotency(conn, 'procesar_pagos_masivos'):
        conn.close()
        return jsonify({'error': 'Solicitud duplicada detectada (idempotencia).'}), 409
    c = conn.cursor()

    planilla = None
    if planilla_id:
        planilla = c.execute('''
            SELECT * FROM planillas_masivas
            WHERE id=? AND tipo='prestamo_cuotas'
        ''', (planilla_id,)).fetchone()

        if not planilla:
            conn.close()
            return jsonify({'error': 'La planilla seleccionada no existe.'}), 404

        if planilla['estado'] == 'aplicada':
            conn.close()
            return jsonify({'error': 'La planilla ya fue aplicada anteriormente.'}), 400
    
    procesados = 0
    errores = []
    total_capital = 0.0
    total_interes = 0.0
    resumen_aplicados = []
    
    for pago in pagos:
        try:
            prestamo_id = pago['prestamo_id']
            monto = float(pago['monto'])
            
            if monto <= 0:
                errores.append(f"Monto inválido para préstamo {pago.get('numero', prestamo_id)}")
                continue
            
            # Obtener información del préstamo
            c.execute('''
                SELECT p.saldo_pendiente, p.cuota_mensual, p.socio_id, s.frecuencia
                FROM prestamos p
                JOIN socios s ON p.socio_id = s.id
                WHERE p.id = ? AND p.estado = "aprobado"
            ''', (prestamo_id,))
            prestamo = c.fetchone()
            
            if not prestamo:
                errores.append(f"Préstamo {pago.get('numero', prestamo_id)} no encontrado o no aprobado")
                continue
            
            saldo_pendiente = prestamo[0]
            cuota_mensual = prestamo[1]

            if frecuencia and prestamo[3] != frecuencia:
                errores.append(f"Prestamo {pago.get('numero', prestamo_id)} no coincide con la frecuencia seleccionada")
                continue
            
            if monto > saldo_pendiente:
                errores.append(f"Monto excede saldo pendiente para préstamo {pago.get('numero', prestamo_id)}")
                continue
            
            # Calcular capital e intereses (simplificado)
            if monto >= cuota_mensual:
                # Pago completo de cuota
                capital = cuota_mensual * 0.8  # 80% capital, 20% intereses (aproximado)
                interes = cuota_mensual * 0.2
            else:
                # Pago parcial
                capital = monto * 0.8
                interes = monto * 0.2

            capital = round(capital, 2)
            interes = round(interes, 2)
            
            nuevo_saldo = saldo_pendiente - monto
            
            # Actualizar saldo del préstamo
            c.execute('UPDATE prestamos SET saldo_pendiente = ? WHERE id = ?', (nuevo_saldo, prestamo_id))
            
            # Registrar pago
            descripcion_planilla = f"Planilla: {nombre_planilla}"
            if boleta_deposito:
                descripcion_planilla += f" | Boleta: {boleta_deposito}"
            if frecuencia:
                descripcion_planilla += f" | Frecuencia: {frecuencia}"

            c.execute('''
                                INSERT INTO pagos_prestamo (prestamo_id, monto, capital, interes, saldo_restante, descripcion, boleta_deposito, fecha, numero_comprobante)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (prestamo_id, monto, capital, interes, nuevo_saldo,
                                    descripcion_planilla, boleta_deposito, fecha_pago, generar_numero_comprobante(conn)))

            if pago.get('detalle_id'):
                c.execute('UPDATE planilla_masiva_detalles SET estado=?, monto=? WHERE id=?', ('aplicado', monto, pago['detalle_id']))
            
            procesados += 1
            total_capital += capital
            total_interes += interes
            resumen_aplicados.append({
                'numero': pago.get('numero', str(prestamo_id)),
                'monto': round(monto, 2),
                'capital': capital,
                'interes': interes,
            })
            
        except Exception as e:
            errores.append(f"Error procesando préstamo {pago.get('numero', prestamo_id)}: {str(e)}")
    
    if planilla_id and planilla:
        pendientes = c.execute(
            "SELECT COUNT(*) FROM planilla_masiva_detalles WHERE planilla_id=? AND estado='pendiente'",
            (planilla_id,)
        ).fetchone()[0]
        estado_final = 'aplicada' if pendientes == 0 and procesados > 0 else ('parcial' if procesados > 0 else 'pendiente')
        c.execute('''
            UPDATE planillas_masivas
            SET estado=?, boleta_deposito=?, fecha_aplicacion=?, usuario_aplicacion=?
            WHERE id=?
        ''', (estado_final, boleta_deposito, datetime.now().isoformat(), session.get('username'), planilla_id))

    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='prestamos',
        entidad='planilla_masiva',
        entidad_id=planilla_id,
        accion='aplicar',
        descripcion='Aplicación de pagos masivos de préstamos',
        datos={
            'procesados': procesados,
            'errores': len(errores),
            'boleta': boleta_deposito,
            'capital_total': round(total_capital, 2),
            'interes_total': round(total_interes, 2),
        }
    )
    
    return jsonify({
        'procesados': procesados,
        'errores': errores,
        'total': len(pagos),
        'planilla_id': planilla_id,
        'capital_total': round(total_capital, 2),
        'interes_total': round(total_interes, 2),
        'resumen_aplicados': resumen_aplicados,
    })


@app.route('/historial_planillas')
@login_required()
def historial_planillas():
    tipo = request.args.get('tipo', 'todos').strip().lower()
    nombre = request.args.get('nombre', '').strip().lower()
    boleta = request.args.get('boleta', '').strip().lower()
    frecuencia = request.args.get('frecuencia', '').strip()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()

    planillas, total_general, total_registros = _obtener_historial_planillas(
        tipo=tipo,
        nombre=nombre,
        boleta=boleta,
        frecuencia=frecuencia,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta
    )

    formato_export = request.args.get('export', '').strip().lower()
    if formato_export == 'csv':
        return _exportar_historial_csv(planillas)
    if formato_export == 'excel':
        return _exportar_historial_excel(planillas)

    filtros = {
        'tipo': tipo,
        'nombre': request.args.get('nombre', '').strip(),
        'boleta': request.args.get('boleta', '').strip(),
        'frecuencia': frecuencia,
        'fecha_desde': fecha_desde,
        'fecha_hasta': fecha_hasta
    }

    return render_template(
        'historial_planillas.html',
        planillas=planillas,
        total_general=total_general,
        total_registros=total_registros,
        filtros=filtros
    )


def _obtener_historial_planillas(tipo='todos', nombre='', boleta='', frecuencia='', fecha_desde='', fecha_hasta=''):

    conn = get_db_connection()
    c = conn.cursor()

    ahorro_rows = c.execute('''
        SELECT t.fecha, t.monto, t.descripcion, s.frecuencia
        FROM transacciones t
        JOIN cuentas c ON t.cuenta_id = c.id
        JOIN socios s ON c.socio_id = s.id
        WHERE t.tipo = 'deposito'
          AND t.descripcion LIKE 'Planilla:%'
    ''').fetchall()

    prestamo_rows = c.execute('''
        SELECT pp.fecha, pp.monto, pp.descripcion, pp.boleta_deposito, s.frecuencia
        FROM pagos_prestamo pp
        JOIN prestamos p ON pp.prestamo_id = p.id
        JOIN socios s ON p.socio_id = s.id
        WHERE pp.descripcion LIKE 'Planilla:%'
           OR COALESCE(pp.boleta_deposito, '') <> ''
    ''').fetchall()
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
