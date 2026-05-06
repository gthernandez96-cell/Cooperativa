import math
import calendar as _calendar
from datetime import datetime, timedelta, date


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


def _ultimo_dia_mes(anio, mes):
    """Retorna el último día del mes dado."""
    return _calendar.monthrange(anio, mes)[1]


def _es_ultimo_dia_mes(d):
    return d.day == _ultimo_dia_mes(d.year, d.month)


def siguiente_fecha_quincenal(d):
    """
    Dado un date quincenal (día 15 o último del mes), retorna el siguiente:
      - Si es día 15  → último día del mismo mes
      - Si es último  → día 15 del siguiente mes
    """
    if d.day == 15:
        ultimo = _ultimo_dia_mes(d.year, d.month)
        return date(d.year, d.month, ultimo)
    else:
        # último día del mes → ir al 15 del mes siguiente
        if d.month == 12:
            return date(d.year + 1, 1, 15)
        return date(d.year, d.month + 1, 15)


def fecha_quincenal_mas_cercana(desde=None):
    """
    Dado un date de referencia, retorna la próxima fecha quincenal
    (día 15 o último del mes) que sea estrictamente posterior a 'desde'.
    """
    hoy = normalizar_fecha_referencia(desde)
    anio, mes, dia = hoy.year, hoy.month, hoy.day
    ultimo = _ultimo_dia_mes(anio, mes)
    if dia < 15:
        return date(anio, mes, 15)
    elif dia < ultimo:
        return date(anio, mes, ultimo)
    else:
        # Ya es fin de mes, ir al 15 del mes siguiente
        if mes == 12:
            return date(anio + 1, 1, 15)
        return date(anio, mes + 1, 15)


def obtener_dias_frecuencia(frecuencia):
    """Días promedio por período (para cálculo de tasa e intereses)."""
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


def calcular_proximo_pago(fecha_ultimo_pago, frecuencia):
    """Calcula la fecha del próximo pago basado en la frecuencia."""
    d = normalizar_fecha_referencia(fecha_ultimo_pago)
    if (frecuencia or '').strip().lower() == 'catorcenal':
        return d + timedelta(days=14)
    # Quincenal: día 15 ↔ último del mes
    return siguiente_fecha_quincenal(d)


def _generar_fechas_quincenal(fecha_inicio, n):
    """Genera n fechas quincenales (día 15 / último del mes) desde fecha_inicio."""
    fechas = []
    actual = normalizar_fecha_referencia(fecha_inicio)
    for _ in range(n):
        fechas.append(actual)
        actual = siguiente_fecha_quincenal(actual)
    return fechas


def generar_calendario_prestamo(fecha_primer_pago, total_cuotas, monto_cuota,
                                frecuencia, monto=None, tasa_anual=None):
    fecha_base = normalizar_fecha_referencia(fecha_primer_pago)
    total_cuotas = int(total_cuotas or 0)
    monto_cuota = round(float(monto_cuota or 0), 2)
    es_quincenal = (frecuencia or '').strip().lower() != 'catorcenal'
    dias = obtener_dias_frecuencia(frecuencia)

    # Generar lista de fechas según frecuencia
    if es_quincenal:
        fechas = _generar_fechas_quincenal(fecha_base, total_cuotas)
    else:
        fechas = [fecha_base + timedelta(days=i * 14) for i in range(total_cuotas)]

    # Desglose capital/interés
    tasa_periodica = None
    if monto and tasa_anual:
        tasa_periodica = (float(tasa_anual) / 100) * (dias / 365)

    saldo_restante = float(monto or 0)
    calendario = []

    for numero, fecha_cuota in enumerate(fechas, start=1):
        if tasa_periodica and tasa_periodica > 0 and saldo_restante > 0:
            interes_cuota = round(saldo_restante * tasa_periodica, 2)
            capital_cuota = round(min(monto_cuota - interes_cuota, saldo_restante), 2)
            if numero == total_cuotas:
                capital_cuota = round(saldo_restante, 2)
                interes_cuota = round(monto_cuota - capital_cuota, 2)
            saldo_restante = round(max(0, saldo_restante - capital_cuota), 2)
        else:
            interes_cuota = 0.0
            capital_cuota = round(monto_cuota, 2)
            saldo_restante = 0.0

        calendario.append({
            'numero_cuota': numero,
            'fecha_programada': fecha_cuota.isoformat(),
            'monto_programado': monto_cuota,
            'capital': capital_cuota,
            'interes': interes_cuota,
        })

    return calendario
