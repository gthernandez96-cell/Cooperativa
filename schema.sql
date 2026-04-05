-- schema.sql
-- Esquema completo de la base de datos de Cooperativa.
-- Para inicializar una base de datos limpia:
--   sqlite3 cooperativa.db < schema.sql
-- Las migraciones de columnas nuevas se gestionan en init_db() de app.py.

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ── Socios ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS socios (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo              TEXT    UNIQUE NOT NULL,
    nombre              TEXT    NOT NULL,
    primer_nombre       TEXT,
    segundo_nombre      TEXT,
    tercer_nombre       TEXT,
    apellido            TEXT    NOT NULL,
    primer_apellido     TEXT,
    segundo_apellido    TEXT,
    estado_civil        TEXT    DEFAULT 'Soltero',
    apellido_casada     TEXT,
    dpi                 TEXT    UNIQUE NOT NULL,
    telefono            TEXT,
    email               TEXT,
    direccion           TEXT,
    rol                 TEXT    DEFAULT 'Asociado',
    fecha_ingreso       TEXT    NOT NULL,
    estado              TEXT    DEFAULT 'activo',
    frecuencia          TEXT    DEFAULT 'Quincenal',
    cuota_ahorro        REAL    DEFAULT 0,
    tipo_ahorro         TEXT    DEFAULT 'ahorro corriente',
    nit                 TEXT,
    beneficiario        TEXT,
    finca               TEXT,
    foto                TEXT,
    banco_nombre        TEXT,
    banco_tipo_cuenta   TEXT,
    banco_numero_cuenta TEXT
);

-- ── Beneficiarios de socios ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS socio_beneficiarios (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    socio_id    INTEGER NOT NULL,
    nombre      TEXT    NOT NULL,
    parentesco  TEXT    NOT NULL,
    porcentaje  REAL    NOT NULL,
    FOREIGN KEY (socio_id) REFERENCES socios(id)
);

-- ── Roles y usuarios ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS roles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre      TEXT    UNIQUE NOT NULL,
    descripcion TEXT,
    estado      TEXT    DEFAULT 'activo'
);

CREATE TABLE IF NOT EXISTS usuarios (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    username        TEXT    UNIQUE NOT NULL,
    password        TEXT    NOT NULL,
    rol_id          INTEGER,
    activo          TEXT    DEFAULT 'si',
    fecha_creacion  TEXT    NOT NULL,
    FOREIGN KEY (rol_id) REFERENCES roles(id)
);

-- ── Configuración de tasas de interés ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS configuraciones (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo                    TEXT    UNIQUE NOT NULL,
    tasa_interes            REAL    NOT NULL,
    descripcion             TEXT,
    fecha_actualizacion     TEXT    NOT NULL,
    usuario_actualizacion   TEXT
);

-- ── Ajustes del sistema (clave-valor) ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ajustes_sistema (
    clave                   TEXT    PRIMARY KEY,
    valor                   TEXT,
    fecha_actualizacion     TEXT    NOT NULL,
    usuario_actualizacion   TEXT
);

-- ── Cuentas de ahorro ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cuentas (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    numero          TEXT    UNIQUE NOT NULL,
    socio_id        INTEGER NOT NULL,
    tipo            TEXT    NOT NULL,
    producto_ahorro TEXT,
    saldo           REAL    DEFAULT 0,
    tasa_interes    REAL    DEFAULT 0,
    fecha_apertura  TEXT    NOT NULL,
    estado          TEXT    DEFAULT 'activa',
    FOREIGN KEY (socio_id) REFERENCES socios(id)
);

-- ── Transacciones de ahorro ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transacciones (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cuenta_id       INTEGER NOT NULL,
    tipo            TEXT    NOT NULL,
    monto           REAL    NOT NULL,
    saldo_despues   REAL    NOT NULL,
    descripcion     TEXT,
    fecha           TEXT    NOT NULL,
    FOREIGN KEY (cuenta_id) REFERENCES cuentas(id)
);

-- ── Categorías de préstamos ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prestamo_categorias (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre                  TEXT    UNIQUE NOT NULL,
    descripcion             TEXT,
    estado                  TEXT    DEFAULT 'activo',
    fecha_actualizacion     TEXT    NOT NULL,
    usuario_actualizacion   TEXT
);

-- ── Préstamos ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prestamos (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    numero              TEXT    UNIQUE NOT NULL,
    socio_id            INTEGER NOT NULL,
    categoria_id        INTEGER,
    monto_solicitado    REAL    NOT NULL,
    monto_aprobado      REAL,
    tasa_interes        REAL    NOT NULL,
    plazo_meses         INTEGER NOT NULL,
    cuota_mensual       REAL,
    saldo_pendiente     REAL,
    fecha_solicitud     TEXT    NOT NULL,
    fecha_aprobacion    TEXT,
    estado              TEXT    DEFAULT 'pendiente',
    etapa_cobranza      TEXT    DEFAULT 'activo',
    desembolso_tipo     TEXT,
    desembolso_referencia TEXT,
    FOREIGN KEY (socio_id) REFERENCES socios(id),
    FOREIGN KEY (categoria_id) REFERENCES prestamo_categorias(id)
);

-- ── Pagos de préstamos ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pagos_prestamo (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    prestamo_id         INTEGER NOT NULL,
    monto               REAL    NOT NULL,
    capital             REAL    NOT NULL,
    interes             REAL    NOT NULL,
    saldo_restante      REAL    NOT NULL,
    descripcion         TEXT,
    boleta_deposito     TEXT,
    numero_comprobante  TEXT,
    fecha               TEXT    NOT NULL,
    FOREIGN KEY (prestamo_id) REFERENCES prestamos(id)
);

-- ── Calendario de pagos de préstamos ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prestamo_calendario_pagos (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    prestamo_id         INTEGER NOT NULL,
    numero_cuota        INTEGER NOT NULL,
    fecha_programada    TEXT    NOT NULL,
    monto_programado    REAL    NOT NULL,
    estado              TEXT    DEFAULT 'pendiente',
    FOREIGN KEY (prestamo_id) REFERENCES prestamos(id)
);

-- ── Planillas masivas ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS planillas_masivas (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo                TEXT    NOT NULL,
    nombre              TEXT    NOT NULL,
    fecha_pago          TEXT    NOT NULL,
    frecuencia          TEXT,
    estado              TEXT    DEFAULT 'pendiente',
    boleta_deposito     TEXT,
    total_monto         REAL    DEFAULT 0,
    total_registros     INTEGER DEFAULT 0,
    fecha_creacion      TEXT    NOT NULL,
    fecha_aplicacion    TEXT,
    usuario_creacion    TEXT,
    usuario_aplicacion  TEXT
);

CREATE TABLE IF NOT EXISTS planilla_masiva_detalles (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    planilla_id         INTEGER NOT NULL,
    referencia_tipo     TEXT    NOT NULL,
    referencia_id       INTEGER NOT NULL,
    numero_referencia   TEXT,
    socio_codigo        TEXT,
    socio_nombre        TEXT,
    monto               REAL    NOT NULL,
    estado              TEXT    DEFAULT 'pendiente',
    FOREIGN KEY (planilla_id) REFERENCES planillas_masivas(id)
);

-- ── Auditoría ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS auditoria_socios (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    socio_id        INTEGER NOT NULL,
    user_id         INTEGER,
    accion          TEXT    NOT NULL,
    datos_previos   TEXT,
    datos_nuevos    TEXT,
    fecha           TEXT    NOT NULL,
    FOREIGN KEY (socio_id) REFERENCES socios(id),
    FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS auditoria_eventos (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    modulo      TEXT    NOT NULL,
    entidad     TEXT    NOT NULL,
    entidad_id  INTEGER,
    accion      TEXT    NOT NULL,
    descripcion TEXT,
    datos       TEXT,
    usuario     TEXT,
    fecha       TEXT    NOT NULL
);

-- ── Cobranza ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cobranza_acciones (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    prestamo_id         INTEGER NOT NULL,
    tipo_accion         TEXT    NOT NULL,
    resultado           TEXT    NOT NULL,
    notas               TEXT,
    monto_comprometido  REAL    DEFAULT 0,
    fecha_compromiso    TEXT,
    fecha_accion        TEXT    NOT NULL,
    responsable         TEXT,
    FOREIGN KEY (prestamo_id) REFERENCES prestamos(id)
);

-- ── Cierres de período ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cierres_periodo (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    modulo          TEXT    NOT NULL,
    fecha_inicio    TEXT    NOT NULL,
    fecha_fin       TEXT    NOT NULL,
    estado          TEXT    DEFAULT 'cerrado',
    observaciones   TEXT,
    usuario         TEXT,
    fecha_creacion  TEXT    NOT NULL
);
