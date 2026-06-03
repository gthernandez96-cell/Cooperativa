from datetime import date, datetime
import json
import re
from flask import session
from utils.db import get_db, db_execute, db_fetchone, db_fetchall
from utils.financial import normalizar_fecha_referencia, calcular_proximo_pago

from config import TRANSACCION_LABELS, TRANSACCIONES_POSITIVAS, CONFIG_LABELS
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

def tipo_transaccion_label(tipo):
    """Convierte identificadores tecnicos de transaccion en etiquetas amigables."""
    if not tipo:
        return 'Movimiento'
    return TRANSACCION_LABELS.get(tipo, tipo.replace('_', ' ').title())

def es_transaccion_positiva(tipo):
    """Indica si una transaccion debe mostrarse como positiva."""
    if not tipo:
        return False
    return tipo in TRANSACCIONES_POSITIVAS

def validar_pago_frecuencia(socio_id, tipo_pago, fecha_referencia=None):
    """
    Valida si un socio puede hacer un pago según su frecuencia configurada.
    Retorna True si puede pagar, False si no.
    """
    conn = get_db()

    socio = db_fetchone(conn, "SELECT frecuencia, cuota_ahorro FROM socios WHERE id=?", [socio_id])
    if not socio or not socio['frecuencia']:
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
            if hoy < proximo_pago:
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
            if hoy < proximo_pago:
                return False

    return True

def obtener_mensaje_validacion_frecuencia(socio_id, tipo_pago, fecha_referencia=None):
    """
    Retorna un mensaje explicativo cuando un pago no puede hacerse por frecuencia.
    """
    conn = get_db()
    socio = db_fetchone(conn, "SELECT frecuencia FROM socios WHERE id=?", [socio_id])
    
    if not socio or not socio['frecuencia']:
        return ""
    
    frecuencia_dias = 14 if socio['frecuencia'] == 'Catorcenal' else 15
    fecha_ref = normalizar_fecha_referencia(fecha_referencia)
    return f"Según la frecuencia {socio['frecuencia'].lower()} configurada para la fecha {fecha_ref.isoformat()}, debe esperar {frecuencia_dias} días entre pagos."

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
    return cierre is not None

def generar_numero_comprobante(conn):
    ultimo = db_fetchone(conn, 'SELECT MAX(id) FROM pagos_prestamo')[0] or 0
    return f'REC-{ultimo + 1:06d}'

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

def log_auditoria_socio(socio_id, user_id, accion, datos_previos=None, datos_nuevos=None):
    conn = get_db()
    db_execute(conn, '''
        INSERT INTO auditoria_socios (socio_id, user_id, accion, datos_previos, datos_nuevos, fecha)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (socio_id, user_id, accion, datos_previos, datos_nuevos, datetime.now().isoformat()))
    conn.commit()

def calcular_bono_14(socio_id, conn):
    socio = conn.execute("SELECT salario, fecha_ingreso_laborar FROM socios WHERE id = ?", (socio_id,)).fetchone()
    if not socio or not socio['salario']:
        return 0.0
    
    salario = socio['salario']
    fecha_ingreso = date.fromisoformat(socio['fecha_ingreso_laborar']) if socio['fecha_ingreso_laborar'] else None
    hoy = date.today()
    
    ultimo_corte = date(hoy.year - 1 if hoy.month < 7 else hoy.year, 6, 30)
    
    if fecha_ingreso and fecha_ingreso > ultimo_corte:
        inicio = fecha_ingreso
    else:
        inicio = ultimo_corte
        
    dias = (hoy.year - inicio.year) * 360 + (hoy.month - inicio.month) * 30 + (hoy.day - inicio.day)
    if dias < 0: dias = 0
        
    return salario * dias / 360.0

def calcular_aguinaldo(socio_id, conn):
    socio = conn.execute("SELECT salario, fecha_ingreso_laborar FROM socios WHERE id = ?", (socio_id,)).fetchone()
    if not socio or not socio['salario']:
        return 0.0
    
    salario = socio['salario']
    fecha_ingreso = date.fromisoformat(socio['fecha_ingreso_laborar']) if socio['fecha_ingreso_laborar'] else None
    hoy = date.today()
    
    ultimo_corte = date(hoy.year - 1 if hoy.month < 12 else hoy.year, 11, 30)
    
    if fecha_ingreso and fecha_ingreso > ultimo_corte:
        inicio = fecha_ingreso
    else:
        inicio = ultimo_corte
        
    dias = (hoy.year - inicio.year) * 360 + (hoy.month - inicio.month) * 30 + (hoy.day - inicio.day)
    if dias < 0: dias = 0
        
    return salario * dias / 360.0

def guardar_historial_salario_actual(socio_id, salario, conn):
    hoy = date.today()
    conn.execute(
        """
        INSERT INTO historial_salarios (socio_id, salario, mes, anio, fecha_registro)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(socio_id, mes, anio) DO UPDATE SET
            salario=excluded.salario,
            fecha_registro=excluded.fecha_registro
        """,
        (socio_id, salario, hoy.month, hoy.year, hoy.isoformat())
    )
    conn.commit()

def limpiar_descripcion_filter(s):
    """Elimina referencias a boletas de la descripción para reportes limpios."""
    if not s:
        return ""
    # Eliminar el patrón "| Boleta: XXXX" o "Boleta: XXXX |" o similares
    s = re.sub(r'\s*\|\s*Boleta:\s*[^|]+', '', s)
    s = re.sub(r'^Boleta:\s*[^|]+\s*\|?\s*', '', s)
    return s.strip()

def formatear_fecha_dmy(val):
    """Convierte fechas ISO (YYYY-MM-DD) o datetimes a formato de visualización Día/Mes/Año (DD/MM/YYYY)."""
    if not val:
        return "—"
    try:
        val = str(val).strip()
        # Verificar si tiene componente de hora
        if " " in val or "T" in val:
            val_clean = val.replace("T", " ")
            parts = val_clean.split(" ")
            date_part = parts[0]
            time_part = parts[1].split(".")[0]  # eliminar microsegundos
            
            date_y, date_m, date_d = date_part.split("-")
            time_subparts = time_part.split(":")
            time_formatted = ":".join(time_subparts[:2]) # HH:MM
            return f"{date_d}/{date_m}/{date_y} {time_formatted}"
        else:
            y, m, d = val.split("-")
            return f"{d}/{m}/{y}"
    except Exception:
        return val

