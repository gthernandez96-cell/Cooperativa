"""
utils/db.py — Gestión de conexiones a base de datos usando Flask's g.

Usar get_db() en cada ruta obtiene la misma conexión dentro de la
request, y close_db() la cierra automáticamente al terminar.
"""
import sqlite3
import logging
import json
from datetime import date, datetime
from flask import g, has_app_context
from werkzeug.security import generate_password_hash
from config import (
    DB, DB_BACKEND, DATABASE_URL, REQUIRED_CONFIGURACIONES,
    SYSTEM_SETTINGS_DEFAULTS, AHORRO_SETTINGS_DEFAULTS,
    PRESTAMO_SETTINGS_DEFAULTS, DEFAULT_PRESTAMO_CATEGORIAS,
    ROLE_PERMISSION_DEFAULTS, DEFAULT_COOPERATIVA_NOMBRE
)

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

logger = logging.getLogger('cooperativa.db')


def _connection_is_closed(conn):
    if conn is None:
        return True
    if hasattr(conn, 'closed'):
        try:
            if conn.closed:
                return True
        except Exception:
            pass
    try:
        conn.execute('SELECT 1')
        return False
    except sqlite3.ProgrammingError:
        return True
    except Exception:
        return False


def get_db():
    """Devuelve la conexión de BD del contexto de la request actual (Flask g).
    Si no existe o está cerrada, la crea. Garantiza una sola conexión por request.
    Si no hay contexto de aplicación, devuelve una conexión temporal."""
    def _create_connection():
        if DB_BACKEND == 'postgres' and psycopg and DATABASE_URL:
            try:
                return psycopg.connect(DATABASE_URL, row_factory=dict_row)
            except Exception as e:
                logger.error(f"Error conectando a Postgres: {e}. Usando SQLite de respaldo.")

        conn = sqlite3.connect(DB, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA busy_timeout = 30000')
        conn.execute('PRAGMA journal_mode = WAL')
        return conn

    if has_app_context():
        if 'db' in g and not _connection_is_closed(g.db):
            return g.db

        g.db = _create_connection()
        return g.db

    return _create_connection()


def close_db(error=None):
    """Cierra la conexión de BD al terminar la request. Registrar con app.teardown_appcontext."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def _is_postgres_connection(conn):
    return conn.__class__.__module__.startswith('psycopg')


def _adapt_query_for_backend(conn, query):
    if _is_postgres_connection(conn):
        return query.replace('?', '%s')
    return query


def db_execute(conn, query, params=()):
    q = _adapt_query_for_backend(conn, query)
    if params is None:
        return conn.execute(q)
    return conn.execute(q, tuple(params))


def db_fetchone(conn, query, params=()):
    return db_execute(conn, query, params).fetchone()


def db_fetchall(conn, query, params=()):
    return db_execute(conn, query, params).fetchall()


def db_executemany(conn, query, rows):
    q = _adapt_query_for_backend(conn, query)
    return conn.executemany(q, rows)


def db_insert_ignore(conn, table, columns, values, conflict_columns):
    """Inserta un registro ignorando conflictos. Compatible con SQLite y PostgreSQL."""
    cols_sql = ', '.join(columns)
    conflict_sql = ', '.join(conflict_columns)
    # Usamos db_execute para que _adapt_query_for_backend maneje el placeholder correcto (? vs %s)
    placeholder_char = '%s' if _is_postgres_connection(conn) else '?'
    placeholders = ', '.join([placeholder_char] * len(columns))
    db_execute(
        conn,
        f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) ON CONFLICT ({conflict_sql}) DO NOTHING",
        values,
    )


def db_insert_and_get_id(conn, query, params=(), id_column='id'):
    q = _adapt_query_for_backend(conn, query)
    if _is_postgres_connection(conn):
        if 'returning' not in q.lower():
            q = f"{q} RETURNING {id_column}"
        row = conn.execute(q, tuple(params)).fetchone()
        if row is None:
            return None
        try:
            return row[id_column]
        except Exception:
            return row[0]
    cur = conn.execute(q, tuple(params))
    return cur.lastrowid


def get_config(tipo):
    """Obtiene el valor de una configuración por tipo. No cierra la conexión del contexto."""
    conn = get_db()
    config = db_fetchone(conn, "SELECT tasa_interes FROM configuraciones WHERE tipo=?", [tipo])
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

from functools import lru_cache

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
    obtener_marca_cooperativa.cache_clear()

@lru_cache(maxsize=1)
def obtener_marca_cooperativa():
    conn = get_db()
    try:
        nombre = get_system_setting(conn, 'cooperativa_nombre', DEFAULT_COOPERATIVA_NOMBRE)
        foto = get_system_setting(conn, 'cooperativa_foto', '')
        # Nuevos campos de identidad
        mision = get_system_setting(conn, 'cooperativa_mision', SYSTEM_SETTINGS_DEFAULTS.get('cooperativa_mision', ''))
        vision = get_system_setting(conn, 'cooperativa_vision', SYSTEM_SETTINGS_DEFAULTS.get('cooperativa_vision', ''))
        principios = get_system_setting(conn, 'cooperativa_principios', SYSTEM_SETTINGS_DEFAULTS.get('cooperativa_principios', ''))
        bg_login = get_system_setting(conn, 'login_background_image', '')
    except Exception:
        nombre = DEFAULT_COOPERATIVA_NOMBRE
        foto = ''
        mision = vision = principios = bg_login = ''
    finally:
        if has_app_context() and 'db' in g:
            pass # g.db se cierra en teardown
        else:
            conn.close()

    return {
        'cooperativa_nombre': nombre or DEFAULT_COOPERATIVA_NOMBRE,
        'cooperativa_foto': foto or None,
        'cooperativa_mision': mision,
        'cooperativa_vision': vision,
        'cooperativa_principios': principios,
        'login_background_image': bg_login,
    }

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
        departamento TEXT,
        municipio TEXT,
        rol TEXT DEFAULT 'Asociado',
        fecha_ingreso TEXT NOT NULL,
        estado TEXT DEFAULT "activo",
        frecuencia TEXT DEFAULT 'Quincenal',
        cuota_ahorro REAL DEFAULT 0,
        cuota_aportacion REAL DEFAULT 0,
        cuota_inscripcion REAL DEFAULT 0,
        tipo_ahorro TEXT DEFAULT 'ahorro corriente',
        nit TEXT,
        beneficiario TEXT,
        finca TEXT,
        banco_nombre TEXT,
        banco_tipo_cuenta TEXT,
        banco_numero_cuenta TEXT,
        foto TEXT,
        salario REAL,
        fecha_ingreso_laborar TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS socio_beneficiarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        socio_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        parentesco TEXT NOT NULL,
        porcentaje REAL NOT NULL,
        dpi TEXT,
        FOREIGN KEY (socio_id) REFERENCES socios(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS historial_salarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        socio_id INTEGER NOT NULL,
        salario REAL NOT NULL,
        mes INTEGER NOT NULL,
        anio INTEGER NOT NULL,
        fecha_registro TEXT NOT NULL,
        FOREIGN KEY (socio_id) REFERENCES socios(id),
        UNIQUE(socio_id, mes, anio)
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
        jornalizado INTEGER DEFAULT 0,
        fecha_jornalizado TEXT,
        boleta_jornalizado TEXT,
        metodo_pago TEXT DEFAULT 'deposito',
        boleta_numero TEXT,
        boleta_fecha TEXT,
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
        destino TEXT DEFAULT 'retiro',
        prestamo_id INTEGER,
        boleta_numero TEXT,
        boleta_fecha TEXT,
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

    # Nota: socio_beneficiarios se crea más arriba en init_db con la definición completa (incluye dpi).
    # El bloque duplicado fue eliminado para evitar inconsistencias.

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
        'cuota_aportacion': "REAL DEFAULT 0",
        'cuota_inscripcion': "REAL DEFAULT 0",
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
        'banco_numero_cuenta': "TEXT",
        'departamento': "TEXT",
        'municipio': "TEXT",
        'salario': "REAL",
        'fecha_ingreso_laborar': "TEXT"
    }

    for columna, definicion in columnas_nuevas.items():
        if columna not in cols:
            try:
                c.execute(f"ALTER TABLE socios ADD COLUMN {columna} {definicion}")
            except Exception:
                pass

    # Los CREATE TABLE de prestamos y pagos_prestamo deben ejecutarse una sola vez, fuera del loop.
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
    if 'fecha_boleta' not in pagos_cols:
        try:
            c.execute("ALTER TABLE pagos_prestamo ADD COLUMN fecha_boleta TEXT")
        except Exception:
            pass
    if 'jornalizado' not in pagos_cols:
        try:
            c.execute("ALTER TABLE pagos_prestamo ADD COLUMN jornalizado INTEGER DEFAULT 0")
        except Exception:
            pass
    if 'fecha_jornalizado' not in pagos_cols:
        try:
            c.execute("ALTER TABLE pagos_prestamo ADD COLUMN fecha_jornalizado TEXT")
        except Exception:
            pass
    if 'boleta_jornalizado' not in pagos_cols:
        try:
            c.execute("ALTER TABLE pagos_prestamo ADD COLUMN boleta_jornalizado TEXT")
        except Exception:
            pass
    if 'metodo_pago' not in pagos_cols:
        try:
            c.execute("ALTER TABLE pagos_prestamo ADD COLUMN metodo_pago TEXT DEFAULT 'deposito'")
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
    if 'capital_amortizado' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN capital_amortizado REAL DEFAULT 0")
        except Exception:
            pass
    if 'interes_amortizado' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN interes_amortizado REAL DEFAULT 0")
        except Exception:
            pass
    if 'jornalizado' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN jornalizado INTEGER DEFAULT 0")
        except Exception:
            pass
    if 'fecha_jornalizado' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN fecha_jornalizado TEXT")
        except Exception:
            pass
    if 'boleta_jornalizado' not in prestamos_cols:
        try:
            c.execute("ALTER TABLE prestamos ADD COLUMN boleta_jornalizado TEXT")
        except Exception:
            pass

    c.execute("PRAGMA table_info(transacciones)")
    transacciones_cols = [row[1] for row in c.fetchall()]
    for col, defn in [('jornalizado', 'INTEGER DEFAULT 0'), ('fecha_jornalizado', 'TEXT'), ('boleta_jornalizado', 'TEXT'), ('metodo_pago', "TEXT DEFAULT 'deposito'"), ('boleta_numero', 'TEXT'), ('boleta_fecha', 'TEXT')]:
        if col not in transacciones_cols:
            try:
                c.execute(f"ALTER TABLE transacciones ADD COLUMN {col} {defn}")
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
    for col, defn in [('boleta_numero', 'TEXT'), ('boleta_fecha', 'TEXT')]:
        if col not in solicitudes_retiro_cols:
            try:
                c.execute(f"ALTER TABLE solicitudes_retiro ADD COLUMN {col} {defn}")
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
            ('ahorro_corriente', 5.0, 'Tasa de interés para cuentas de ahorro corriente', date.today().isoformat()),
            ('ahorro_plazo_fijo', 4.0, 'Tasa de interés para cuentas de ahorro a plazo fijo', date.today().isoformat()),
            ('ahorro_aportacion', 5.0, 'Tasa de interés para cuentas de aportación', date.today().isoformat()),
            ('prestamo_personal', 18.0, 'Tasa de interés para préstamos personales', date.today().isoformat()),
            ('prestamo_vivienda', 12.0, 'Tasa de interés para préstamos de vivienda', date.today().isoformat()),
            ('prestamo_negocio', 15.0, 'Tasa de interés para préstamos de negocio', date.today().isoformat()),
        ]
        for conf in configuraciones_demo:
            c.execute("INSERT OR IGNORE INTO configuraciones (tipo,tasa_interes,descripcion,fecha_actualizacion) VALUES (?,?,?,?)", conf)
         
        c.execute("INSERT OR IGNORE INTO usuarios (username,password,rol_id,fecha_creacion) VALUES (?,?,?,?)", ('admin',generate_password_hash('admin123'),1,date.today().isoformat()))
 
        cuentas_demo = [
            ('AHO-0001',1,'ahorro',15000,5.0,'2022-01-16'),
            ('AHO-0002',2,'ahorro',8500,5.0,'2022-03-11'),
            ('COR-0001',1,'corriente',3200,0,'2022-01-16'),
            ('AHO-0003',3,'ahorro',22000,5.0,'2023-06-21'),
            ('AHO-0004',4,'ahorro',5000,5.0,'2023-09-06'),
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
 
    # Migración de 'isr' a 'ipf' en transacciones y auditoría existentes
    c.execute("UPDATE transacciones SET tipo = 'ipf' WHERE tipo = 'isr'")
    c.execute("UPDATE auditoria_eventos SET accion = 'ipf' WHERE accion = 'isr'")
    c.execute("UPDATE transacciones SET descripcion = REPLACE(descripcion, 'ISR', 'IPF') WHERE descripcion LIKE '%ISR%'")
 
    # Migración de tasas de interés de Ahorro y Aportación a 5%
    c.execute("UPDATE configuraciones SET tasa_interes = 5.0 WHERE tipo IN ('ahorro_corriente', 'ahorro_aportacion')")
    c.execute("UPDATE cuentas SET tasa_interes = 5.0 WHERE tipo = 'ahorro' AND tasa_interes IN (3.5, 2.5, 3.0)")
    c.execute("UPDATE ajustes_sistema SET valor = '5.0' WHERE clave = 'ahorro_tasa_interes_default'")
 
    # Corrección de préstamos con saldos insignificantes (cancelados por errores de redondeo de flotante)
    c.execute("UPDATE prestamos SET saldo_pendiente = 0.0, estado = 'pagado' WHERE estado = 'aprobado' AND saldo_pendiente <= 0.01")
    c.execute("UPDATE prestamo_calendario_pagos SET estado = 'pagado' WHERE prestamo_id IN (SELECT id FROM prestamos WHERE estado = 'pagado') AND estado = 'pendiente'")

    # ── Módulo de Contabilidad ─────────────────────────────────────────────────
    c.execute('''CREATE TABLE IF NOT EXISTS cont_cuentas (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo          TEXT    UNIQUE NOT NULL,
        nombre          TEXT    NOT NULL,
        tipo            TEXT    NOT NULL, -- activo, pasivo, patrimonio, ingreso, gasto
        naturaleza      TEXT    NOT NULL DEFAULT 'deudora', -- deudora, acreedora
        parent_id       INTEGER,
        nivel           INTEGER NOT NULL DEFAULT 1,
        acepta_movimientos INTEGER DEFAULT 0, -- 1=cuenta de detalle, 0=cuenta de agrupación
        saldo           REAL    DEFAULT 0,
        estado          TEXT    DEFAULT 'activa',
        descripcion     TEXT,
        fecha_creacion  TEXT    NOT NULL,
        FOREIGN KEY (parent_id) REFERENCES cont_cuentas(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS cont_partidas (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        numero          TEXT    UNIQUE NOT NULL,
        fecha           TEXT    NOT NULL,
        descripcion     TEXT    NOT NULL,
        estado          TEXT    DEFAULT 'borrador', -- borrador, asentado, anulado
        origen_tipo     TEXT,   -- ahorro, prestamo, pos, manual
        origen_id       INTEGER,
        usuario         TEXT,
        fecha_creacion  TEXT    NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS cont_apuntes (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        partida_id      INTEGER NOT NULL,
        cuenta_id       INTEGER NOT NULL,
        socio_id        INTEGER,
        descripcion     TEXT,
        debe            REAL    DEFAULT 0,
        haber           REAL    DEFAULT 0,
        FOREIGN KEY (partida_id) REFERENCES cont_partidas(id),
        FOREIGN KEY (cuenta_id) REFERENCES cont_cuentas(id),
        FOREIGN KEY (socio_id) REFERENCES socios(id)
    )''')

    c.execute("CREATE INDEX IF NOT EXISTS idx_cont_partidas_fecha ON cont_partidas(fecha)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cont_apuntes_partida ON cont_apuntes(partida_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cont_apuntes_cuenta ON cont_apuntes(cuenta_id)")

    # ── Módulo de Punto de Venta (POS) ─────────────────────────────────────────
    c.execute('''CREATE TABLE IF NOT EXISTS pos_categorias (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre          TEXT    UNIQUE NOT NULL,
        descripcion     TEXT,
        estado          TEXT    DEFAULT 'activa'
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_productos (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo          TEXT    UNIQUE NOT NULL,
        nombre          TEXT    NOT NULL,
        descripcion     TEXT,
        categoria_id    INTEGER,
        precio_venta    REAL    NOT NULL DEFAULT 0,
        costo           REAL    DEFAULT 0,
        stock           REAL    DEFAULT 0,
        stock_minimo    REAL    DEFAULT 0,
        unidad          TEXT    DEFAULT 'unidad',
        imagen          TEXT,
        estado          TEXT    DEFAULT 'activo',
        puede_venderse  INTEGER DEFAULT 1,
        puede_comprarse INTEGER DEFAULT 1,
        disponible_pos  INTEGER DEFAULT 1,
        fecha_creacion  TEXT    NOT NULL,
        proveedor_id    INTEGER,
        stock_maximo    REAL    DEFAULT 0,
        FOREIGN KEY (categoria_id) REFERENCES pos_categorias(id),
        FOREIGN KEY (proveedor_id) REFERENCES pos_proveedores(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_producto_componentes (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        producto_id     INTEGER NOT NULL,
        componente_id   INTEGER NOT NULL,
        cantidad        REAL    NOT NULL DEFAULT 1,
        FOREIGN KEY (producto_id) REFERENCES pos_productos(id),
        FOREIGN KEY (componente_id) REFERENCES pos_productos(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_ventas (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        numero          TEXT    UNIQUE NOT NULL,
        socio_id        INTEGER,
        cliente_nombre  TEXT,
        cliente_nit     TEXT    DEFAULT 'CF',
        cliente_direccion TEXT  DEFAULT 'Ciudad',
        subtotal        REAL    DEFAULT 0,
        descuento       REAL    DEFAULT 0,
        total           REAL    NOT NULL DEFAULT 0,
        metodo_pago     TEXT    NOT NULL DEFAULT 'efectivo', -- efectivo, debito_ahorro, tarjeta
        cuenta_id       INTEGER,          -- cuenta de ahorro si se debita
        estado          TEXT    DEFAULT 'completada', -- completada, anulada
        notas           TEXT,
        usuario         TEXT,
        fecha           TEXT    NOT NULL,
        fel_uuid        TEXT,
        fel_serie       TEXT,
        fel_numero      TEXT,
        fel_fecha_certificacion TEXT,
        FOREIGN KEY (socio_id) REFERENCES socios(id),
        FOREIGN KEY (cuenta_id) REFERENCES cuentas(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_venta_detalles (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        venta_id        INTEGER NOT NULL,
        producto_id     INTEGER NOT NULL,
        cantidad        REAL    NOT NULL DEFAULT 1,
        precio_unitario REAL    NOT NULL,
        descuento       REAL    DEFAULT 0,
        subtotal        REAL    NOT NULL,
        FOREIGN KEY (venta_id) REFERENCES pos_ventas(id),
        FOREIGN KEY (producto_id) REFERENCES pos_productos(id)
    )''')

    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_ventas_fecha ON pos_ventas(fecha)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_ventas_socio ON pos_ventas(socio_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_detalles_venta ON pos_venta_detalles(venta_id)")

    # Extensiones de la base de datos para el módulo POS extendido

    # Caja y Turnos
    c.execute('''CREATE TABLE IF NOT EXISTS pos_cajas (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre          TEXT    UNIQUE NOT NULL,
        estado          TEXT    DEFAULT 'activo'
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_caja_sesiones (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        caja_id         INTEGER NOT NULL,
        usuario_apertura TEXT    NOT NULL,
        usuario_cierre   TEXT,
        fecha_apertura   TEXT    NOT NULL,
        fecha_cierre     TEXT,
        saldo_apertura   REAL    NOT NULL DEFAULT 0,
        saldo_cierre     REAL,
        saldo_esperado   REAL,
        estado          TEXT    DEFAULT 'abierta',
        notas           TEXT,
        FOREIGN KEY (caja_id) REFERENCES pos_cajas(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_caja_movimientos (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        sesion_id       INTEGER NOT NULL,
        tipo            TEXT    NOT NULL, -- entrada, salida
        monto           REAL    NOT NULL,
        motivo          TEXT    NOT NULL,
        fecha           TEXT    NOT NULL,
        usuario         TEXT    NOT NULL,
        FOREIGN KEY (sesion_id) REFERENCES pos_caja_sesiones(id)
    )''')

    # Cotizaciones
    c.execute('''CREATE TABLE IF NOT EXISTS pos_cotizaciones (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        numero          TEXT    UNIQUE NOT NULL,
        socio_id        INTEGER,
        cliente_nombre  TEXT,
        subtotal        REAL    DEFAULT 0,
        descuento       REAL    DEFAULT 0,
        total           REAL    NOT NULL DEFAULT 0,
        fecha           TEXT    NOT NULL,
        fecha_vencimiento TEXT  NOT NULL,
        estado          TEXT    DEFAULT 'borrador', -- borrador, convertida, vencida
        notas           TEXT,
        usuario         TEXT,
        FOREIGN KEY (socio_id) REFERENCES socios(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_cotizacion_detalles (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        cotizacion_id   INTEGER NOT NULL,
        producto_id     INTEGER NOT NULL,
        cantidad        REAL    NOT NULL DEFAULT 1,
        precio_unitario REAL    NOT NULL,
        descuento       REAL    DEFAULT 0,
        subtotal        REAL    NOT NULL,
        FOREIGN KEY (cotizacion_id) REFERENCES pos_cotizaciones(id),
        FOREIGN KEY (producto_id) REFERENCES pos_productos(id)
    )''')

    # Devoluciones y Notas de Crédito
    c.execute('''CREATE TABLE IF NOT EXISTS pos_devoluciones (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        numero          TEXT    UNIQUE NOT NULL,
        venta_id        INTEGER NOT NULL,
        socio_id        INTEGER,
        total_reembolsado REAL  DEFAULT 0,
        fecha           TEXT    NOT NULL,
        usuario         TEXT    NOT NULL,
        motivo          TEXT,
        FOREIGN KEY (venta_id) REFERENCES pos_ventas(id),
        FOREIGN KEY (socio_id) REFERENCES socios(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_devolucion_detalles (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        devolucion_id   INTEGER NOT NULL,
        producto_id     INTEGER NOT NULL,
        cantidad        REAL    NOT NULL,
        precio_unitario REAL    NOT NULL,
        subtotal        REAL    NOT NULL,
        FOREIGN KEY (devolucion_id) REFERENCES pos_devoluciones(id),
        FOREIGN KEY (producto_id) REFERENCES pos_productos(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_notas_credito (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        numero          TEXT    UNIQUE NOT NULL,
        socio_id        INTEGER NOT NULL,
        monto_original  REAL    NOT NULL,
        saldo_disponible REAL   NOT NULL,
        fecha           TEXT    NOT NULL,
        estado          TEXT    DEFAULT 'activo',
        venta_origen_id INTEGER,
        FOREIGN KEY (socio_id) REFERENCES socios(id),
        FOREIGN KEY (venta_origen_id) REFERENCES pos_ventas(id)
    )''')

    # Proveedores y Compras
    c.execute('''CREATE TABLE IF NOT EXISTS pos_proveedores (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre          TEXT    NOT NULL,
        vendedor_nombre TEXT,
        nit             TEXT,
        telefono        TEXT,
        email           TEXT,
        direccion       TEXT,
        dias_credito    INTEGER DEFAULT 0,
        terminos_pago   TEXT    DEFAULT 'Pago inmediato',
        metodo_pago     TEXT    DEFAULT 'Cheque',
        banco_nombre    TEXT,
        banco_tipo_cuenta TEXT,
        banco_numero_cuenta TEXT,
        estado          TEXT    DEFAULT 'activo'
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_compras (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        numero          TEXT    UNIQUE NOT NULL,
        proveedor_id    INTEGER NOT NULL,
        total           REAL    NOT NULL DEFAULT 0,
        fecha           TEXT    NOT NULL,
        estado          TEXT    DEFAULT 'recibida',
        estado_pago     TEXT    DEFAULT 'Pendiente',
        usuario         TEXT,
        notas           TEXT,
        FOREIGN KEY (proveedor_id) REFERENCES pos_proveedores(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_compra_detalles (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        compra_id       INTEGER NOT NULL,
        producto_id     INTEGER NOT NULL,
        cantidad        REAL    NOT NULL,
        costo_unitario  REAL    NOT NULL,
        subtotal        REAL    NOT NULL,
        FOREIGN KEY (compra_id) REFERENCES pos_compras(id),
        FOREIGN KEY (producto_id) REFERENCES pos_productos(id)
    )''')

    # Ajustes de Inventario
    c.execute('''CREATE TABLE IF NOT EXISTS pos_ajustes_inventario (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        numero          TEXT    UNIQUE NOT NULL,
        tipo            TEXT    NOT NULL,
        fecha           TEXT    NOT NULL,
        usuario         TEXT    NOT NULL,
        notas           TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_ajuste_detalles (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ajuste_id       INTEGER NOT NULL,
        producto_id     INTEGER NOT NULL,
        cantidad_anterior REAL  NOT NULL,
        cantidad_nueva  REAL    NOT NULL,
        diferencia      REAL    NOT NULL,
        FOREIGN KEY (ajuste_id) REFERENCES pos_ajustes_inventario(id),
        FOREIGN KEY (producto_id) REFERENCES pos_productos(id)
    )''')

    # Puntos de Fidelidad
    c.execute('''CREATE TABLE IF NOT EXISTS pos_puntos_fidelidad (
        socio_id        INTEGER PRIMARY KEY,
        puntos_acumulados INTEGER NOT NULL DEFAULT 0,
        puntos_canjeados INTEGER NOT NULL DEFAULT 0,
        fecha_actualizacion TEXT NOT NULL,
        FOREIGN KEY (socio_id) REFERENCES socios(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pos_puntos_historial (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        socio_id        INTEGER NOT NULL,
        venta_id        INTEGER,
        tipo            TEXT    NOT NULL,
        puntos          INTEGER NOT NULL,
        fecha           TEXT    NOT NULL,
        FOREIGN KEY (socio_id) REFERENCES socios(id),
        FOREIGN KEY (venta_id) REFERENCES pos_ventas(id)
    )''')

    # Pagos Múltiples (Ventas Pagos)
    c.execute('''CREATE TABLE IF NOT EXISTS pos_venta_pagos (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        venta_id        INTEGER NOT NULL,
        metodo_pago     TEXT    NOT NULL,
        cuenta_id       INTEGER,
        monto           REAL    NOT NULL DEFAULT 0,
        detalle_id      INTEGER,
        FOREIGN KEY (venta_id) REFERENCES pos_ventas(id),
        FOREIGN KEY (cuenta_id) REFERENCES cuentas(id)
    )''')

    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_caja_sesiones_caja ON pos_caja_sesiones(caja_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_caja_movimientos_sesion ON pos_caja_movimientos(sesion_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_cotizaciones_socio ON pos_cotizaciones(socio_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_devoluciones_venta ON pos_devoluciones(venta_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_notas_credito_socio ON pos_notas_credito(socio_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_compras_proveedor ON pos_compras(proveedor_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_venta_pagos_venta ON pos_venta_pagos(venta_id)")

    # Migraciones para agregar columnas faltantes
    try:
        c.execute("ALTER TABLE pos_productos ADD COLUMN codigo_barras TEXT")
    except Exception:
        pass
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pos_prod_barras ON pos_productos(codigo_barras)")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_productos ADD COLUMN referencia_proveedor TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE socios ADD COLUMN limite_credito_pos REAL DEFAULT 500.0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE socios ADD COLUMN saldo_credito_pos REAL DEFAULT 0.0")
    except Exception:
        pass

    try:
        c.execute("ALTER TABLE pos_productos ADD COLUMN precio_socio REAL DEFAULT 0.0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE socios ADD COLUMN frecuencia_credito_pos TEXT DEFAULT 'Quincenal'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE socios ADD COLUMN cuota_credito_pos REAL DEFAULT 0.0")
    except Exception:
        pass

    try:
        c.execute("ALTER TABLE pos_productos ADD COLUMN proveedor_id INTEGER REFERENCES pos_proveedores(id)")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_productos ADD COLUMN stock_maximo REAL DEFAULT 0")
    except Exception:
        pass
    
    try:
        c.execute("ALTER TABLE pos_proveedores ADD COLUMN vendedor_nombre TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_proveedores ADD COLUMN dias_credito INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_proveedores ADD COLUMN terminos_pago TEXT DEFAULT 'Pago inmediato'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_proveedores ADD COLUMN metodo_pago TEXT DEFAULT 'Cheque'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_proveedores ADD COLUMN banco_nombre TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_proveedores ADD COLUMN banco_tipo_cuenta TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_proveedores ADD COLUMN banco_numero_cuenta TEXT")
    except Exception:
        pass

    try:
        c.execute("ALTER TABLE pos_compras ADD COLUMN estado_pago TEXT DEFAULT 'Pendiente'")
    except Exception:
        pass

    try:
        c.execute("ALTER TABLE pos_productos ADD COLUMN puede_venderse INTEGER DEFAULT 1")
        c.execute("ALTER TABLE pos_productos ADD COLUMN puede_comprarse INTEGER DEFAULT 1")
        c.execute("ALTER TABLE pos_productos ADD COLUMN disponible_pos INTEGER DEFAULT 1")
    except Exception:
        pass
    
    # --- MIGRACION BODEGAS INTEGRADA ---
    try:
        c.execute("""
        CREATE TABLE IF NOT EXISTS pos_bodegas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT UNIQUE NOT NULL,
            ubicacion TEXT,
            estado TEXT DEFAULT 'activo'
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS pos_producto_bodegas (
            producto_id INTEGER NOT NULL,
            bodega_id INTEGER NOT NULL,
            stock REAL DEFAULT 0,
            PRIMARY KEY (producto_id, bodega_id),
            FOREIGN KEY (producto_id) REFERENCES pos_productos(id),
            FOREIGN KEY (bodega_id) REFERENCES pos_bodegas(id)
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS pos_traslados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero TEXT UNIQUE NOT NULL,
            origen_bodega_id INTEGER NOT NULL,
            destino_bodega_id INTEGER NOT NULL,
            fecha TEXT NOT NULL,
            usuario TEXT,
            estado TEXT DEFAULT 'completado',
            notas TEXT,
            FOREIGN KEY (origen_bodega_id) REFERENCES pos_bodegas(id),
            FOREIGN KEY (destino_bodega_id) REFERENCES pos_bodegas(id)
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS pos_traslado_detalles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            traslado_id INTEGER NOT NULL,
            producto_id INTEGER NOT NULL,
            cantidad REAL NOT NULL,
            FOREIGN KEY (traslado_id) REFERENCES pos_traslados(id),
            FOREIGN KEY (producto_id) REFERENCES pos_productos(id)
        )
        """)
    except Exception:
        pass

    try:
        c.execute("ALTER TABLE pos_compras ADD COLUMN numero_factura TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_compras ADD COLUMN bodega_id INTEGER REFERENCES pos_bodegas(id)")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE pos_caja_sesiones ADD COLUMN bodega_id INTEGER REFERENCES pos_bodegas(id)")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE usuarios ADD COLUMN bodega_id INTEGER REFERENCES pos_bodegas(id)")
    except Exception:
        pass

    # Sembrar bodega por defecto
    try:
        c.execute("SELECT id FROM pos_bodegas WHERE nombre='Bodega Principal'")
        row = c.fetchone()
        if not row:
            c.execute("INSERT INTO pos_bodegas (nombre, ubicacion) VALUES ('Bodega Principal', 'Sede Central')")
            bodega_id = c.lastrowid
        else:
            bodega_id = row[0]
            
        c.execute("UPDATE pos_caja_sesiones SET bodega_id=? WHERE bodega_id IS NULL", (bodega_id,))
        c.execute("UPDATE pos_compras SET bodega_id=? WHERE bodega_id IS NULL", (bodega_id,))
        c.execute("UPDATE usuarios SET bodega_id=? WHERE bodega_id IS NULL", (bodega_id,))
        
        c.execute("SELECT id, stock FROM pos_productos")
        productos = c.fetchall()
        for p_row in productos:
            c.execute("INSERT OR IGNORE INTO pos_producto_bodegas (producto_id, bodega_id, stock) VALUES (?, ?, ?)", (p_row[0], bodega_id, p_row[1]))
    except Exception:
        pass

    # Sembrar caja por defecto
    c.execute("SELECT COUNT(*) FROM pos_cajas")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO pos_cajas (nombre, estado) VALUES (?,?)", ('Caja Principal', 'activo'))


    # ── Sembrar Nomenclatura Contable Estándar ─────────────────────────────────
    c.execute("SELECT COUNT(*) FROM cont_cuentas")
    if c.fetchone()[0] == 0:
        hoy = date.today().isoformat()
        cuentas_base = [
            # (codigo, nombre, tipo, naturaleza, parent_id, nivel, acepta_movimientos, descripcion)
            # NIVEL 1 — Grupos Principales
            ('1',    'ACTIVO',                           'activo',     'deudora',   None, 1, 0, 'Recursos controlados por la entidad'),
            ('2',    'PASIVO',                           'pasivo',     'acreedora', None, 1, 0, 'Obligaciones presentes de la entidad'),
            ('3',    'PATRIMONIO',                       'patrimonio', 'acreedora', None, 1, 0, 'Parte residual de los activos'),
            ('4',    'INGRESOS',                         'ingreso',    'acreedora', None, 1, 0, 'Incrementos en beneficios económicos'),
            ('5',    'GASTOS',                           'gasto',      'deudora',   None, 1, 0, 'Decrementos en beneficios económicos'),
            # NIVEL 2 — Subgrupos Activo
            ('11',   'Activo Corriente',                 'activo',     'deudora',   None, 2, 0, 'Activos realizables en el corto plazo'),
            ('12',   'Activo No Corriente',              'activo',     'deudora',   None, 2, 0, 'Activos de largo plazo'),
            # NIVEL 2 — Subgrupos Pasivo
            ('21',   'Pasivo Corriente',                 'pasivo',     'acreedora', None, 2, 0, 'Obligaciones a corto plazo'),
            ('22',   'Pasivo No Corriente',              'pasivo',     'acreedora', None, 2, 0, 'Obligaciones a largo plazo'),
            # NIVEL 2 — Patrimonio
            ('31',   'Capital Social',                   'patrimonio', 'acreedora', None, 2, 0, 'Aportes de los asociados'),
            ('32',   'Reservas',                         'patrimonio', 'acreedora', None, 2, 0, 'Reservas legales y estatutarias'),
            ('33',   'Resultados',                       'patrimonio', 'acreedora', None, 2, 0, 'Utilidades y pérdidas acumuladas'),
            # NIVEL 2 — Ingresos y Gastos
            ('41',   'Ingresos Financieros',             'ingreso',    'acreedora', None, 2, 0, 'Intereses y comisiones ganadas'),
            ('42',   'Ingresos por Servicios',           'ingreso',    'acreedora', None, 2, 0, 'Ingresos operativos de servicios'),
            ('43',   'Ingresos por Ventas POS',          'ingreso',    'acreedora', None, 2, 0, 'Ventas del punto de venta'),
            ('51',   'Gastos Financieros',               'gasto',      'deudora',   None, 2, 0, 'Intereses pagados a asociados'),
            ('52',   'Gastos Operativos',                'gasto',      'deudora',   None, 2, 0, 'Gastos de administración y operación'),
            ('53',   'Gastos de Personal',               'gasto',      'deudora',   None, 2, 0, 'Sueldos, salarios y prestaciones'),
            ('54',   'Costo de Ventas POS',              'gasto',      'deudora',   None, 2, 0, 'Costo de los productos vendidos en POS'),
        ]
        # Insertar grupos principales primero para obtener IDs
        for cod, nom, tip, nat, pid, niv, ace, desc in cuentas_base:
            c.execute(
                '''INSERT OR IGNORE INTO cont_cuentas
                   (codigo, nombre, tipo, naturaleza, parent_id, nivel, acepta_movimientos, descripcion, fecha_creacion)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (cod, nom, tip, nat, pid, niv, ace, desc, hoy)
            )

        # Función auxiliar para obtener id de cuenta por código
        def _get_cid(codigo):
            row = c.execute("SELECT id FROM cont_cuentas WHERE codigo=?", (codigo,)).fetchone()
            return row[0] if row else None

        # NIVEL 3 — Cuentas de Agrupación
        cuentas_n3 = [
            # Activo Corriente
            ('1101', 'Caja y Bancos',              'activo',     'deudora',   '11', 3, 0, 'Efectivo y depósitos bancarios'),
            ('1102', 'Inversiones a Corto Plazo',  'activo',     'deudora',   '11', 3, 0, 'Inversiones temporales'),
            ('1103', 'Cartera de Créditos',        'activo',     'deudora',   '11', 3, 0, 'Préstamos otorgados a asociados'),
            ('1104', 'Cuentas por Cobrar',         'activo',     'deudora',   '11', 3, 0, 'Derechos de cobro'),
            ('1105', 'Inventario POS',             'activo',     'deudora',   '11', 3, 0, 'Mercadería disponible para venta'),
            # Activo No Corriente
            ('1201', 'Propiedades y Equipos',      'activo',     'deudora',   '12', 3, 0, 'Bienes de uso duradero'),
            ('1202', 'Depreciación Acumulada',     'activo',     'deudora',   '12', 3, 0, 'Valor depreciado de activos fijos'),
            # Pasivo Corriente
            ('2101', 'Captaciones de Ahorro',      'pasivo',     'acreedora', '21', 3, 0, 'Depósitos de ahorro de asociados'),
            ('2102', 'Cuentas por Pagar',          'pasivo',     'acreedora', '21', 3, 0, 'Obligaciones de corto plazo'),
            ('2103', 'Intereses por Pagar',        'pasivo',     'acreedora', '21', 3, 0, 'Intereses devengados sobre ahorros'),
            # Pasivo No Corriente
            ('2201', 'Préstamos Recibidos',        'pasivo',     'acreedora', '22', 3, 0, 'Financiamiento externo'),
            # Capital
            ('3101', 'Aportaciones de Asociados',  'patrimonio', 'acreedora', '31', 3, 0, 'Capital aportado por los socios'),
            ('3201', 'Reserva Legal',              'patrimonio', 'acreedora', '32', 3, 0, 'Reserva legal mínima'),
            ('3301', 'Utilidad del Ejercicio',     'patrimonio', 'acreedora', '33', 3, 0, 'Resultado del período'),
            # Ingresos
            ('4101', 'Intereses Ganados Préstamos','ingreso',    'acreedora', '41', 3, 0, 'Intereses cobrados a asociados'),
            ('4201', 'Comisiones y Servicios',     'ingreso',    'acreedora', '42', 3, 0, 'Ingresos por servicios varios'),
            ('4301', 'Ventas Netas POS',           'ingreso',    'acreedora', '43', 3, 0, 'Ventas realizadas en el POS'),
            # Gastos
            ('5101', 'Intereses Pagados Ahorro',   'gasto',      'deudora',   '51', 3, 0, 'Intereses reconocidos a asociados'),
            ('5201', 'Gastos Administrativos',     'gasto',      'deudora',   '52', 3, 0, 'Papelería, servicios, alquileres'),
            ('5301', 'Sueldos y Salarios',         'gasto',      'deudora',   '53', 3, 0, 'Remuneraciones al personal'),
            ('5401', 'Costo de Mercadería Vendida','gasto',      'deudora',   '54', 3, 0, 'Costo directo de ventas POS'),
        ]
        for cod, nom, tip, nat, pcod, niv, ace, desc in cuentas_n3:
            pid = _get_cid(pcod)
            c.execute(
                '''INSERT OR IGNORE INTO cont_cuentas
                   (codigo, nombre, tipo, naturaleza, parent_id, nivel, acepta_movimientos, descripcion, fecha_creacion)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (cod, nom, tip, nat, pid, niv, ace, desc, hoy)
            )

        # NIVEL 4 — Cuentas de Detalle (acepta_movimientos=1)
        cuentas_n4 = [
            ('110101', 'Caja General',              'activo', 'deudora',   '1101', 4, 1, 'Efectivo en caja'),
            ('110102', 'Banco — Cuenta Corriente',  'activo', 'deudora',   '1101', 4, 1, 'Depósito en institución bancaria'),
            ('110301', 'Créditos Vigentes',         'activo', 'deudora',   '1103', 4, 1, 'Saldo de préstamos al día'),
            ('110302', 'Créditos en Mora',          'activo', 'deudora',   '1103', 4, 1, 'Saldo de préstamos vencidos'),
            ('110501', 'Inventario de Mercadería',  'activo', 'deudora',   '1105', 4, 1, 'Existencias disponibles para POS'),
            ('210101', 'Depósitos Ahorro Corriente','pasivo', 'acreedora', '2101', 4, 1, 'Saldos de ahorro corriente'),
            ('210102', 'Depósitos Aportaciones',    'pasivo', 'acreedora', '2101', 4, 1, 'Saldos de aportaciones'),
            ('210103', 'Depósitos Plazo Fijo',      'pasivo', 'acreedora', '2101', 4, 1, 'Saldos de cuentas a plazo fijo'),
            ('310101', 'Aportaciones Corrientes',   'patrimonio','acreedora','3101', 4, 1, 'Cuotas de aportación mensuales'),
            ('410101', 'Int. Préstamos Personales', 'ingreso', 'acreedora', '4101', 4, 1, 'Intereses de préstamos personales'),
            ('410102', 'Int. Préstamos Vivienda',   'ingreso', 'acreedora', '4101', 4, 1, 'Intereses de préstamos de vivienda'),
            ('430101', 'Ventas al Contado POS',     'ingreso', 'acreedora', '4301', 4, 1, 'Ventas en efectivo del POS'),
            ('430102', 'Ventas Débito Ahorro POS',  'ingreso', 'acreedora', '4301', 4, 1, 'Ventas debitadas de ahorros en POS'),
            ('510101', 'Int. Ahorro Corriente',     'gasto',  'deudora',   '5101', 4, 1, 'Intereses pagados sobre ahorro corriente'),
            ('510102', 'Int. Aportaciones',         'gasto',  'deudora',   '5101', 4, 1, 'Intereses pagados sobre aportaciones'),
            ('520101', 'Papelería y Útiles',        'gasto',  'deudora',   '5201', 4, 1, 'Gastos de papelería'),
            ('520102', 'Servicios Públicos',        'gasto',  'deudora',   '5201', 4, 1, 'Agua, luz, internet'),
            ('530101', 'Salarios del Personal',     'gasto',  'deudora',   '5301', 4, 1, 'Sueldos mensuales'),
            ('540101', 'Costo de Ventas',           'gasto',  'deudora',   '5401', 4, 1, 'Costo directo de mercadería vendida'),
        ]
        for cod, nom, tip, nat, pcod, niv, ace, desc in cuentas_n4:
            pid = _get_cid(pcod)
            c.execute(
                '''INSERT OR IGNORE INTO cont_cuentas
                   (codigo, nombre, tipo, naturaleza, parent_id, nivel, acepta_movimientos, descripcion, fecha_creacion)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (cod, nom, tip, nat, pid, niv, ace, desc, hoy)
            )

    # ── Sembrar Categorías y Productos Demo POS ────────────────────────────────
    c.execute("SELECT COUNT(*) FROM pos_categorias")
    if c.fetchone()[0] == 0:
        hoy = date.today().isoformat()
        cats = [('General', 'Productos generales'), ('Alimentos', 'Artículos alimenticios'), ('Servicios', 'Servicios varios')]
        for nombre, desc in cats:
            c.execute("INSERT OR IGNORE INTO pos_categorias (nombre, descripcion) VALUES (?,?)", (nombre, desc))

        cat_gen = c.execute("SELECT id FROM pos_categorias WHERE nombre='General'").fetchone()
        cat_ali = c.execute("SELECT id FROM pos_categorias WHERE nombre='Alimentos'").fetchone()
        cat_ser = c.execute("SELECT id FROM pos_categorias WHERE nombre='Servicios'").fetchone()
        cat_gen_id = cat_gen[0] if cat_gen else 1
        cat_ali_id = cat_ali[0] if cat_ali else 1
        cat_ser_id = cat_ser[0] if cat_ser else 1

        productos_demo = [
            ('PROD-001', 'Cuaderno universitario', 'Cuaderno 100 hojas', cat_gen_id, 18.00, 12.00, 50, 5),
            ('PROD-002', 'Bolígrafo azul',         'Bolígrafo BIC azul', cat_gen_id, 3.00,  1.50,  200, 20),
            ('PROD-003', 'Azúcar 1lb',             'Azúcar blanca 1 libra', cat_ali_id, 4.50, 3.20, 100, 10),
            ('PROD-004', 'Café molido 250g',       'Café tostado y molido', cat_ali_id, 22.00, 15.00, 30, 5),
            ('PROD-005', 'Servicio de Fotocopias', 'Por hoja impresa', cat_ser_id, 0.50, 0.10, 999, 0),
        ]
        for cod, nom, desc, cat_id, pv, co, stk, stk_min in productos_demo:
            c.execute(
                '''INSERT OR IGNORE INTO pos_productos
                   (codigo, nombre, descripcion, categoria_id, precio_venta, costo, stock, stock_minimo, estado, fecha_creacion)
                   VALUES (?,?,?,?,?,?,?,?,'activo',?)''',
                (cod, nom, desc, cat_id, pv, co, stk, stk_min, date.today().isoformat())
            )

    # Asegurar sincronización de stock de productos a Bodega Principal
    try:
        row = c.execute("SELECT id FROM pos_bodegas WHERE nombre='Bodega Principal'").fetchone()
        if row:
            bodega_id = row[0]
            c.execute("SELECT id, stock FROM pos_productos")
            for p_row in c.fetchall():
                c.execute("INSERT OR IGNORE INTO pos_producto_bodegas (producto_id, bodega_id, stock) VALUES (?, ?, ?)", (p_row[0], bodega_id, p_row[1]))
    except Exception:
        pass

    # Índices para el módulo POS
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_productos_codigo ON pos_productos(codigo)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_productos_cat ON pos_productos(categoria_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_ventas_numero ON pos_ventas(numero)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_ventas_fecha ON pos_ventas(fecha)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_venta_detalles_venta ON pos_venta_detalles(venta_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_pos_producto_bodegas ON pos_producto_bodegas(producto_id, bodega_id)")

    conn.commit()
    conn.close()


def ensure_required_configurations(conn):
    """Asegura que existan las configuraciones base editables en el panel."""
    hoy = date.today().isoformat()
    for tipo, tasa, descripcion in REQUIRED_CONFIGURACIONES:
        db_insert_ignore(
            conn,
            'configuraciones',
            ('tipo', 'tasa_interes', 'descripcion', 'fecha_actualizacion'),
            (tipo, tasa, descripcion, hoy),
            ('tipo',),
        )
        db_execute(conn, "UPDATE configuraciones SET descripcion=? WHERE tipo=?", (descripcion, tipo))
