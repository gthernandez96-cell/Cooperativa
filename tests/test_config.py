"""
tests/test_config.py — Pruebas de config.py y constantes del sistema.
"""
import config


def test_db_path_es_string():
    assert isinstance(config.DB, str)
    assert config.DB.endswith('.db')


def test_config_labels_contiene_tipos_esperados():
    tipos = {'ahorro_corriente', 'ahorro_plazo_fijo', 'ahorro_aportacion', 'prestamo'}
    assert set(config.CONFIG_LABELS.keys()) == tipos


def test_transaccion_labels_no_vacio():
    assert len(config.TRANSACCION_LABELS) > 0


def test_transacciones_positivas_es_set():
    assert isinstance(config.TRANSACCIONES_POSITIVAS, set)
    assert 'deposito' in config.TRANSACCIONES_POSITIVAS


def test_required_configuraciones_son_4():
    assert len(config.REQUIRED_CONFIGURACIONES) == 4


def test_tasas_default_son_positivas():
    for tipo, tasa, desc in config.REQUIRED_CONFIGURACIONES:
        assert tasa > 0, f"Tasa de {tipo} debe ser positiva"


def test_ahorro_settings_defaults_contiene_claves_criticas():
    claves_criticas = {
        'ahorro_tasa_interes_default',
        'ahorro_saldo_minimo_default',
        'ahorro_limite_retiro_diario',
    }
    assert claves_criticas <= set(config.AHORRO_SETTINGS_DEFAULTS.keys())


def test_prestamo_settings_defaults_contiene_claves_criticas():
    claves_criticas = {
        'prestamo_tasa_interes_default',
        'prestamo_plazo_maximo_default',
        'prestamo_monto_maximo_default',
    }
    assert claves_criticas <= set(config.PRESTAMO_SETTINGS_DEFAULTS.keys())


def test_extensiones_imagen_permitidas():
    assert 'jpg' in config.ALLOWED_IMAGE_EXTENSIONS
    assert 'png' in config.ALLOWED_IMAGE_EXTENSIONS
    assert 'webp' in config.ALLOWED_IMAGE_EXTENSIONS
    assert 'gif' not in config.ALLOWED_IMAGE_EXTENSIONS


def test_cooperativa_nombre_default_no_vacio():
    assert config.DEFAULT_COOPERATIVA_NOMBRE.strip() != ''
