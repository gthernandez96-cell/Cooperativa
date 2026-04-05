"""
utils/nombres.py — Utilidades para manejo de nombres y apellidos de socios.
"""


def descomponer_nombre(texto, max_partes):
    """Divide un texto en hasta max_partes palabras, rellenando con vacíos."""
    partes = [parte.strip() for parte in (texto or '').split() if parte.strip()]
    resultado = partes[:max_partes]
    while len(resultado) < max_partes:
        resultado.append('')
    return resultado


def construir_nombre_completo(primer_nombre='', segundo_nombre='', tercer_nombre=''):
    return ' '.join(p for p in [primer_nombre, segundo_nombre, tercer_nombre] if p).strip()


def construir_apellido_completo(primer_apellido='', segundo_apellido=''):
    return ' '.join(p for p in [primer_apellido, segundo_apellido] if p).strip()


def preparar_datos_socio(socio):
    """Normaliza y completa los campos de nombre/apellido de un socio."""
    if not socio:
        return None

    data = dict(socio)
    primer_nombre = data.get('primer_nombre')
    segundo_nombre = data.get('segundo_nombre')
    tercer_nombre = data.get('tercer_nombre')

    if not any([primer_nombre, segundo_nombre, tercer_nombre]):
        primer_nombre, segundo_nombre, tercer_nombre = descomponer_nombre(data.get('nombre', ''), 3)

    primer_apellido = data.get('primer_apellido')
    segundo_apellido = data.get('segundo_apellido')

    if not any([primer_apellido, segundo_apellido]):
        primer_apellido, segundo_apellido = descomponer_nombre(data.get('apellido', ''), 2)

    data['primer_nombre'] = primer_nombre or ''
    data['segundo_nombre'] = segundo_nombre or ''
    data['tercer_nombre'] = tercer_nombre or ''
    data['primer_apellido'] = primer_apellido or ''
    data['segundo_apellido'] = segundo_apellido or ''
    data['nombre'] = construir_nombre_completo(
        data['primer_nombre'], data['segundo_nombre'], data['tercer_nombre']
    ) or (data.get('nombre') or '')
    data['apellido'] = construir_apellido_completo(
        data['primer_apellido'], data['segundo_apellido']
    ) or (data.get('apellido') or '')
    data['nombre_completo'] = ' '.join(p for p in [data['nombre'], data['apellido']] if p).strip()
    data['apellido_casada'] = data.get('apellido_casada') or ''
    data['estado_civil'] = data.get('estado_civil') or 'Soltero'
    data['banco_nombre'] = data.get('banco_nombre') or ''
    data['banco_tipo_cuenta'] = data.get('banco_tipo_cuenta') or ''
    data['banco_numero_cuenta'] = data.get('banco_numero_cuenta') or ''
    return data


def resumen_beneficiarios(beneficiarios):
    """Devuelve los nombres de beneficiarios separados por coma."""
    return ', '.join(item['nombre'] for item in beneficiarios if item.get('nombre')).strip()
