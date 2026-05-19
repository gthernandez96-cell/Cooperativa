import math
from datetime import date
from utils.db import get_db, db_fetchall, db_fetchone, ensure_required_configurations, ensure_default_prestamo_categories
from utils.financial import calcular_total_cuotas_prestamo, calcular_alerta_prestamo

def obtener_cartera_con_alertas(fecha_inicio=None, fecha_fin=None):
    """Obtiene la lista de préstamos con sus estados de alerta calculados."""
    conn = get_db()
    filtros = ''
    params = []
    if fecha_inicio:
        filtros += ' AND date(p.fecha_solicitud) >= date(?)'
        params.append(fecha_inicio)
    if fecha_fin:
        filtros += ' AND date(p.fecha_solicitud) <= date(?)'
        params.append(fecha_fin)

    rows = db_fetchall(
        conn,
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
    )
    conn.close()

    cartera = []
    for row in rows:
        item = dict(row)
        if (item.get('estado') or '').lower() == 'amortizado':
            item['estado'] = 'pagado'
        
        alerta = calcular_alerta_prestamo(item)
        item.update(alerta)
        
        cuota = float(item.get('cuota_mensual') or 0)
        saldo = float(item.get('saldo_pendiente') or 0)
        item['cuotas_pendientes'] = math.ceil(saldo / cuota) if cuota > 0 and saldo > 0 else 0
        item['total_cuotas'] = item.get('total_cuotas') or calcular_total_cuotas_prestamo(item.get('plazo_meses'), item.get('frecuencia'))
        cartera.append(item)

    return cartera

def cargar_contexto_nuevo_prestamo(socio_id_seleccionado=''):
    """Prepara los datos necesarios para el formulario de nuevo préstamo."""
    conn = get_db()
    ensure_required_configurations(conn)
    ensure_default_prestamo_categories(conn)

    socios = db_fetchall(
        conn,
        "SELECT id, codigo, nombre, apellido, dpi, frecuencia, banco_nombre, banco_tipo_cuenta, banco_numero_cuenta FROM socios WHERE estado='activo' ORDER BY codigo, nombre, apellido"
    )
    configs = db_fetchall(conn, "SELECT * FROM configuraciones WHERE tipo='prestamo'")
    categorias_prestamo = db_fetchall(
        conn,
        "SELECT id, nombre, descripcion FROM prestamo_categorias WHERE estado='activo' ORDER BY nombre"
    )
    
    prestamos_rows = db_fetchall(
        conn,
        '''
        SELECT p.id, p.socio_id, p.numero, p.estado, p.categoria_id,
               COALESCE(p.cuota_mensual, 0) AS cuota_mensual,
               p.fecha_solicitud,
               COALESCE(p.saldo_pendiente, p.monto_aprobado, p.monto_solicitado, 0) AS saldo_vigente,
               p.monto_solicitado,
               pc.nombre AS categoria_nombre,
               COALESCE(cal.cuotas_pendientes, 0) AS cuotas_pendientes
        FROM prestamos p
        JOIN socios s ON s.id = p.socio_id
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        LEFT JOIN (
            SELECT prestamo_id, COUNT(*) AS cuotas_pendientes
            FROM prestamo_calendario_pagos
            WHERE estado = 'pendiente'
            GROUP BY prestamo_id
        ) cal ON cal.prestamo_id = p.id
        WHERE p.estado IN ('pendiente', 'aprobado')
        ORDER BY p.socio_id, date(p.fecha_solicitud) DESC, p.id DESC
        '''
    )
    conn.close()

    prestamos_vigentes_por_socio = {}
    for row in prestamos_rows:
        socio_key = str(row['socio_id'])
        saldo = float(row['saldo_vigente'] or 0)
        cuota = float(row['cuota_mensual'] or 0)
        cuotas_pendientes = int(row['cuotas_pendientes'] or 0)
        
        interes_total = round(max(0, cuota * cuotas_pendientes - saldo), 2) if cuota > 0 and cuotas_pendientes > 0 else 0.0
        
        prestamos_vigentes_por_socio.setdefault(socio_key, []).append({
            'id': row['id'],
            'numero': row['numero'],
            'estado': row['estado'],
            'categoria_id': row['categoria_id'],
            'fecha_solicitud': row['fecha_solicitud'],
            'monto_solicitado': float(row['monto_solicitado'] or 0),
            'saldo_vigente': saldo,
            'interes_periodo': interes_total,
            'capital_periodo': round(saldo, 2),
            'categoria_nombre': row['categoria_nombre'] or 'Sin categoria',
        })

    return {
        'socios': socios,
        'configuraciones': configs,
        'categorias_prestamo': categorias_prestamo,
        'prestamos_vigentes_por_socio': prestamos_vigentes_por_socio,
        'socio_id_seleccionado': str(socio_id_seleccionado or ''),
    }
