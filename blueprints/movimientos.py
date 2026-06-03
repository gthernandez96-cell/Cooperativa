from flask import Blueprint, render_template, request, jsonify, flash, session, url_for, redirect
from datetime import date, datetime
from utils.db import get_db, db_execute, db_fetchone, db_fetchall
from utils.decorators import login_required, permission_required
from utils.helpers import log_auditoria_evento

bp = Blueprint('movimientos', __name__, url_prefix='/movimientos')

@bp.route('/diarios')
@login_required()
def diarios():
    conn = get_db()
    
    fecha_desde = request.args.get('fecha_desde', date.today().isoformat())
    fecha_hasta = request.args.get('fecha_hasta', date.today().isoformat())
    tipo_filtro = request.args.get('tipo', 'todos')  # todos, retiros, prestamos, amortizaciones, cheques, depositos
    estado_filtro = request.args.get('estado', 'todos')  # todos, pendientes, jornalizados
    
    # 1. Obtener Transacciones de ahorro (excluyendo depósitos)
    query_txns = '''
        SELECT 
            'transaccion' AS tipo_origen,
            t.id,
            t.fecha,
            t.monto,
            s.nombre || ' ' || s.apellido AS socio,
            s.codigo AS socio_codigo,
            t.jornalizado,
            t.fecha_jornalizado,
            t.boleta_jornalizado,
            t.metodo_pago,
            t.tipo AS sub_tipo,
            t.descripcion AS detalle,
            c.numero AS referencia,
            NULL AS doc_ref
        FROM transacciones t
        JOIN cuentas c ON t.cuenta_id = c.id
        JOIN socios s ON c.socio_id = s.id
        WHERE date(t.fecha) BETWEEN date(?) AND date(?) AND t.tipo != 'deposito'
    '''
    
    # 2. Obtener Desembolsos de Préstamos
    query_loans = '''
        SELECT 
            'prestamo' AS tipo_origen,
            p.id,
            p.fecha_aprobacion AS fecha,
            p.monto_aprobado AS monto,
            s.nombre || ' ' || s.apellido AS socio,
            s.codigo AS socio_codigo,
            p.jornalizado,
            p.fecha_jornalizado,
            p.boleta_jornalizado,
            p.desembolso_tipo AS metodo_pago,
            'desembolso' AS sub_tipo,
            'Desembolso Préstamo ' || p.numero AS detalle,
            p.numero AS referencia,
            p.desembolso_referencia AS doc_ref
        FROM prestamos p
        JOIN socios s ON p.socio_id = s.id
        WHERE p.estado IN ('aprobado', 'pagado') AND date(p.fecha_aprobacion) BETWEEN date(?) AND date(?)
    '''
    
    # 3. Obtener Amortizaciones (Pagos de Préstamos)
    query_pagos = '''
        SELECT 
            'pago_prestamo' AS tipo_origen,
            pp.id,
            pp.fecha,
            pp.monto,
            s.nombre || ' ' || s.apellido AS socio,
            s.codigo AS socio_codigo,
            pp.jornalizado,
            pp.fecha_jornalizado,
            pp.boleta_jornalizado,
            COALESCE(pp.metodo_pago, 'deposito') AS metodo_pago,
            'amortizacion' AS sub_tipo,
            pp.descripcion AS detalle,
            p.numero AS referencia,
            pp.boleta_deposito AS doc_ref
        FROM pagos_prestamo pp
        JOIN prestamos p ON pp.prestamo_id = p.id
        JOIN socios s ON p.socio_id = s.id
        WHERE date(pp.fecha) BETWEEN date(?) AND date(?)
    '''
    
    try:
        txns_rows = db_fetchall(conn, query_txns, [fecha_desde, fecha_hasta])
        loans_rows = db_fetchall(conn, query_loans, [fecha_desde, fecha_hasta])
        pagos_rows = db_fetchall(conn, query_pagos, [fecha_desde, fecha_hasta])
    except Exception as e:
        flash(f'Error al consultar movimientos: {e}', 'danger')
        txns_rows, loans_rows, pagos_rows = [], [], []

    # Combinar y Normalizar en Python
    todos_movimientos = []
    for r in txns_rows:
        todos_movimientos.append(dict(r))
    for r in loans_rows:
        todos_movimientos.append(dict(r))
    for r in pagos_rows:
        todos_movimientos.append(dict(r))
        
    # Ordenar por fecha (más reciente primero, o ascendente? Ascendente por fecha y ID es mejor para contabilidad)
    todos_movimientos.sort(key=lambda x: (x['fecha'], x['id']))
    
    # Filtrar
    movimientos_filtrados = []
    for m in todos_movimientos:
        # Filtrado por Estado
        if estado_filtro == 'pendientes' and m['jornalizado'] != 0:
            continue
        if estado_filtro == 'jornalizados' and m['jornalizado'] != 1:
            continue
            
        # Filtrado por Tipo
        if tipo_filtro == 'retiros' and m['sub_tipo'] != 'retiro':
            continue
        if tipo_filtro == 'prestamos' and m['sub_tipo'] != 'desembolso':
            continue
        if tipo_filtro == 'amortizaciones' and m['sub_tipo'] != 'amortizacion':
            continue
        if tipo_filtro == 'intereses' and m['sub_tipo'] != 'interes':
            continue
        if tipo_filtro == 'ipf' and m['sub_tipo'] != 'ipf':
            continue
        if tipo_filtro == 'cheques' and m['metodo_pago'] != 'cheque':
            continue
                
        movimientos_filtrados.append(m)

    # Calcular estadísticas del día
    stats = {
        'total_pendiente': 0.0,
        'cant_pendiente': 0,
        'total_jornalizado': 0.0,
        'cant_jornalizado': 0,
    }
    for m in todos_movimientos:
        if m['jornalizado'] == 1:
            stats['total_jornalizado'] += float(m['monto'] or 0)
            stats['cant_jornalizado'] += 1
        else:
            stats['total_pendiente'] += float(m['monto'] or 0)
            stats['cant_pendiente'] += 1

    conn.close()
    
    return render_template(
        'movimientos_diarios.html',
        movimientos=movimientos_filtrados,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        tipo_filtro=tipo_filtro,
        estado_filtro=estado_filtro,
        stats=stats
    )

@bp.route('/jornalizar', methods=['POST'])
@login_required()
def jornalizar():
    data = request.get_json() or {}
    items = data.get('items', [])
    fecha_jornalizacion = data.get('fecha_jornalizacion', '').strip()
    boleta_jornalizacion = data.get('boleta_jornalizacion', '').strip()
    
    if not items:
        return jsonify({'success': False, 'error': 'No se seleccionaron movimientos para jornalizar.'}), 400
        
    if not fecha_jornalizacion:
        return jsonify({'success': False, 'error': 'Debe ingresar una fecha de jornalización.'}), 400
        
    if not boleta_jornalizacion:
        return jsonify({'success': False, 'error': 'Debe ingresar el número de boleta.'}), 400
        
    conn = get_db()
    try:
        for item in items:
            id_mov = item.get('id')
            tipo_origen = item.get('tipo_origen')
            
            if tipo_origen == 'transaccion':
                db_execute(
                    conn,
                    '''
                    UPDATE transacciones 
                    SET jornalizado = 1, fecha_jornalizado = ?, boleta_jornalizado = ? 
                    WHERE id = ?
                    ''',
                    [fecha_jornalizacion, boleta_jornalizacion, id_mov]
                )
            elif tipo_origen == 'prestamo':
                db_execute(
                    conn,
                    '''
                    UPDATE prestamos 
                    SET jornalizado = 1, fecha_jornalizado = ?, boleta_jornalizado = ? 
                    WHERE id = ?
                    ''',
                    [fecha_jornalizacion, boleta_jornalizacion, id_mov]
                )
            elif tipo_origen == 'pago_prestamo':
                db_execute(
                    conn,
                    '''
                    UPDATE pagos_prestamo 
                    SET jornalizado = 1, fecha_jornalizado = ?, boleta_jornalizado = ? 
                    WHERE id = ?
                    ''',
                    [fecha_jornalizacion, boleta_jornalizacion, id_mov]
                )
            else:
                raise ValueError(f'Tipo de origen desconocido: {tipo_origen}')
                
        conn.commit()
        
        # Registrar en la bitácora
        log_auditoria_evento(
            modulo='movimientos',
            entidad='jornalizacion',
            accion='procesar',
            entidad_id=None,
            descripcion=f'Jornalización masiva procesada para {len(items)} movimientos. Boleta: {boleta_jornalizacion}, Fecha: {fecha_jornalizacion}',
            datos={'cantidad_items': len(items), 'boleta': boleta_jornalizacion, 'fecha': fecha_jornalizacion}
        )
        
        return jsonify({'success': True, 'message': f'Se han jornalizado {len(items)} movimientos correctamente.'})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()
