"""
config.py — Constantes y configuraciones del sistema.
Centraliza todos los valores que antes estaban dispersos en app.py.
"""
import os

# ── Rutas del sistema ──────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(__file__)
DB = os.path.join(BASE_DIR, 'cooperativa.db')
SOCIOS_UPLOAD_DIR = os.path.join(BASE_DIR, 'static', 'uploads', 'socios')
COOPERATIVA_UPLOAD_DIR = os.path.join(BASE_DIR, 'static', 'uploads', 'cooperativa')

# ── Identidad corporativa ──────────────────────────────────────────────────────
DEFAULT_COOPERATIVA_NOMBRE = 'Cooperativa de Consumo del Sur. R.L'

# ── Etiquetas de UI ────────────────────────────────────────────────────────────
CONFIG_LABELS = {
    'ahorro_corriente': 'Ahorro',
    'ahorro_plazo_fijo': 'Plazo Fijo',
    'ahorro_aportacion': 'Aportacion',
    'prestamo': 'Prestamos',
}

TRANSACCION_LABELS = {
    'deposito': 'Depósito',
    'retiro': 'Retiro',
    'interes': 'Interés',
    'transferencia': 'Transferencia',
    'abono': 'Abono',
    'cargo': 'Cargo',
}

TRANSACCIONES_POSITIVAS = {'deposito', 'interes', 'abono', 'transferencia_entrada'}

# ── Configuraciones de tasas requeridas ────────────────────────────────────────
REQUIRED_CONFIGURACIONES = [
    ('ahorro_corriente', 2.5, 'Tasa de interes para Ahorro'),
    ('ahorro_plazo_fijo', 4.0, 'Tasa de interes para Plazo Fijo'),
    ('ahorro_aportacion', 3.0, 'Tasa de interes para Aportacion'),
    ('prestamo', 18.0, 'Tasa de interes para Prestamos'),
]

# ── Ajustes del sistema por defecto ───────────────────────────────────────────
SYSTEM_SETTINGS_DEFAULTS = {
    'cooperativa_nombre': DEFAULT_COOPERATIVA_NOMBRE,
    'cooperativa_foto': '',
    'prestamo_finiquito_texto': (
        'Por este medio, {cooperativa_nombre} hace constar que el asociado {socio_nombre} '
        'con codigo {socio_codigo} mantiene el prestamo {prestamo_numero} en categoria {categoria_nombre}, '
        'por un monto aprobado de Q{monto_aprobado}, con cuota programada de Q{cuota} y frecuencia {frecuencia}.\n\n'
        'Forma de desembolso: {desembolso_tipo}. Referencia: {desembolso_referencia}.\n\n'
        'Fecha de aprobacion: {fecha_aprobacion}.\n'
        'Primer pago programado: {fecha_primer_pago}.\n'
        'Ultima cuota programada: {fecha_ultima_cuota}.\n\n'
        'Se extiende el presente finiquito para los usos administrativos que correspondan.'
    ),
}

# ── Ajustes de ahorro por defecto ──────────────────────────────────────────────
AHORRO_SETTINGS_DEFAULTS = {
    'ahorro_tasa_interes_default': '3.5',
    'ahorro_saldo_minimo_default': '100.00',
    'ahorro_limite_retiro_diario': '5000.00',
    'ahorro_comision_transferencia': '2.00',
    'ahorro_dia_corte': '30',
    'ahorro_dia_interes': '1',
    'ahorro_periodo_inactividad': '90',
    'ahorro_lim_tx_deposito_max': '10000.00',
    'ahorro_lim_tx_retiro_max': '5000.00',
    'ahorro_lim_tx_transferencia_max': '25000.00',
    'ahorro_lim_dia_depositos_max': '50000.00',
    'ahorro_lim_dia_retiros_max': '25000.00',
    'ahorro_lim_dia_transferencias_max': '100000.00',
    'ahorro_lim_mes_depositos_max': '200000.00',
    'ahorro_lim_mes_retiros_max': '100000.00',
    'ahorro_lim_mes_transferencias_max': '500000.00',
}

# ── Ajustes de préstamos por defecto ──────────────────────────────────────────
PRESTAMO_SETTINGS_DEFAULTS = {
    'prestamo_tasa_interes_default': '12.0',
    'prestamo_plazo_maximo_default': '60',
    'prestamo_monto_maximo_default': '100000.00',
    'prestamo_edad_minima': '18',
    'prestamo_edad_maxima': '75',
    'prestamo_tasa_mora_diaria': '0.1',
    'prestamo_cargo_amortizacion': '2.0',
    'prestamo_cargo_refinanciamiento': '1.5',
    'prestamo_comision_desembolso': '1.0',
    'prestamo_seguro_vida': '0.5',
    'prestamo_ratio_deuda_ingreso_max': '0.35',
    'prestamo_ratio_deuda_activos_max': '0.60',
    'prestamo_historial_crediticio_min_meses': '6',
    'prestamo_score_crediticio_min': '550',
    'prestamo_sin_garantia_max': '5000.00',
    'prestamo_cobertura_garantia_min': '120',
    'prestamo_fiadores_min': '1',
    'prestamo_fiadores_max': '2',
    'prestamo_dias_primer_recordatorio': '5',
    'prestamo_dias_cobranza_intensiva': '30',
    'prestamo_dias_accion_legal': '90',
    'prestamo_cargo_recordatorio': '10.00',
    'prestamo_cargo_carta_cobranza': '25.00',
    'prestamo_cargo_gestion_judicial': '100.00',
    'prestamo_dias_considerar_perdida': '180',
    'prestamo_pct_recuperacion_esperado': '70',
    'prestamo_frecuencia_reportes_dias': '7',
}

# ── Categorías de préstamo por defecto ────────────────────────────────────────
DEFAULT_PRESTAMO_CATEGORIAS = [
    ('Personal', 'Prestamos para necesidades personales y familiares.'),
    ('Vivienda', 'Prestamos orientados a mejoras, compra o ampliacion de vivienda.'),
    ('Negocio', 'Prestamos para capital de trabajo o inversion comercial.'),
]

# ── Extensiones de imagen permitidas ──────────────────────────────────────────
ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp'}
