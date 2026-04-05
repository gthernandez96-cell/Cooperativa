"""
tests/test_utils.py — Pruebas unitarias de funciones utilitarias.
Estas pruebas no necesitan base de datos ni cliente HTTP.
"""
import pytest
from utils.nombres import (
    descomponer_nombre,
    construir_nombre_completo,
    construir_apellido_completo,
    preparar_datos_socio,
    resumen_beneficiarios,
)
from utils.images import allowed_image


# ── utils/nombres.py ─────────────────────────────────────────────────────────

class TestDescomponerNombre:
    def test_nombre_completo_tres_partes(self):
        result = descomponer_nombre('María Elena Guadalupe', 3)
        assert result == ['María', 'Elena', 'Guadalupe']

    def test_nombre_corto_rellena_vacios(self):
        result = descomponer_nombre('Carlos', 3)
        assert result == ['Carlos', '', '']

    def test_texto_vacio(self):
        result = descomponer_nombre('', 2)
        assert result == ['', '']

    def test_texto_none(self):
        result = descomponer_nombre(None, 2)
        assert result == ['', '']

    def test_exceso_de_palabras_se_trunca(self):
        result = descomponer_nombre('Ana María de los Angeles', 2)
        assert result == ['Ana', 'María']


class TestConstruirNombreCompleto:
    def test_tres_nombres(self):
        assert construir_nombre_completo('Juan', 'Carlos', 'López') == 'Juan Carlos López'

    def test_ignora_vacios(self):
        assert construir_nombre_completo('Juan', '', 'López') == 'Juan López'

    def test_solo_primer_nombre(self):
        assert construir_nombre_completo('Ana') == 'Ana'

    def test_todos_vacios(self):
        assert construir_nombre_completo() == ''


class TestConstruirApellidoCompleto:
    def test_dos_apellidos(self):
        assert construir_apellido_completo('García', 'López') == 'García López'

    def test_solo_primer_apellido(self):
        assert construir_apellido_completo('Martínez') == 'Martínez'

    def test_ambos_vacios(self):
        assert construir_apellido_completo('', '') == ''


class TestPrepararDatosSocio:
    def test_none_retorna_none(self):
        assert preparar_datos_socio(None) is None

    def test_campos_nombre_se_construyen(self):
        socio = {
            'primer_nombre': 'María',
            'segundo_nombre': 'Elena',
            'tercer_nombre': '',
            'primer_apellido': 'García',
            'segundo_apellido': 'López',
            'nombre': '',
            'apellido': '',
            'estado_civil': 'Casado',
            'apellido_casada': 'de Pérez',
            'banco_nombre': None,
            'banco_tipo_cuenta': None,
            'banco_numero_cuenta': None,
        }
        result = preparar_datos_socio(socio)
        assert result['nombre'] == 'María Elena'
        assert result['apellido'] == 'García López'
        assert result['nombre_completo'] == 'María Elena García López'

    def test_nombre_desde_campo_nombre_si_no_hay_partes(self):
        socio = {
            'primer_nombre': None,
            'segundo_nombre': None,
            'tercer_nombre': None,
            'primer_apellido': None,
            'segundo_apellido': None,
            'nombre': 'Ana Sofía',
            'apellido': 'Ramírez Cruz',
            'banco_nombre': None,
            'banco_tipo_cuenta': None,
            'banco_numero_cuenta': None,
        }
        result = preparar_datos_socio(socio)
        assert result['primer_nombre'] == 'Ana'
        assert result['segundo_nombre'] == 'Sofía'

    def test_estado_civil_default_soltero(self):
        socio = {
            'primer_nombre': 'X', 'segundo_nombre': '', 'tercer_nombre': '',
            'primer_apellido': 'Y', 'segundo_apellido': '',
            'nombre': 'X', 'apellido': 'Y',
            'banco_nombre': None, 'banco_tipo_cuenta': None, 'banco_numero_cuenta': None,
        }
        result = preparar_datos_socio(socio)
        assert result['estado_civil'] == 'Soltero'


class TestResumenBeneficiarios:
    def test_multiples_beneficiarios(self):
        bens = [
            {'nombre': 'Juan', 'parentesco': 'Hijo', 'porcentaje': 50},
            {'nombre': 'María', 'parentesco': 'Esposa', 'porcentaje': 50},
        ]
        assert resumen_beneficiarios(bens) == 'Juan, María'

    def test_vacio(self):
        assert resumen_beneficiarios([]) == ''

    def test_ignora_sin_nombre(self):
        bens = [{'nombre': '', 'parentesco': 'Hijo', 'porcentaje': 100}]
        assert resumen_beneficiarios(bens) == ''


# ── utils/images.py ──────────────────────────────────────────────────────────

class TestAllowedImage:
    def test_jpg_permitido(self):
        assert allowed_image('foto.jpg') is True

    def test_jpeg_permitido(self):
        assert allowed_image('foto.jpeg') is True

    def test_png_permitido(self):
        assert allowed_image('foto.png') is True

    def test_webp_permitido(self):
        assert allowed_image('foto.webp') is True

    def test_gif_no_permitido(self):
        assert allowed_image('foto.gif') is False

    def test_pdf_no_permitido(self):
        assert allowed_image('archivo.pdf') is False

    def test_sin_extension_no_permitido(self):
        assert allowed_image('archivo') is False

    def test_nombre_vacio_no_permitido(self):
        assert allowed_image('') is False

    def test_none_no_permitido(self):
        assert allowed_image(None) is False
