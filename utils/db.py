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
    ROLE_PERMISSION_DEFAULTS
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
    placeholders = ', '.join(['?'] * len(columns))
    cols_sql = ', '.join(columns)
    conflict_sql = ', '.join(conflict_columns)
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


def get_db_connection():
    # Alias de compatibilidad para rutas legacy.
    return get_db()


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
    except Exception:
        nombre = DEFAULT_COOPERATIVA_NOMBRE
        foto = ''

    return {
        'cooperativa_nombre': nombre or DEFAULT_COOPERATIVA_NOMBRE,
        'cooperativa_foto': foto or None,
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
