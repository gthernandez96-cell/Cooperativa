"""
blueprints/pos.py — Módulo de Punto de Venta (POS)
Gestiona la terminal de ventas, catálogo de productos, historial de ventas,
cajas y turnos, cotizaciones, devoluciones, proveedores, compras, ajustes de inventario,
y la integración con cuentas de ahorro, contabilidad y puntos de fidelidad.
"""
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, session, jsonify, current_app
)
import os
from werkzeug.utils import secure_filename
from datetime import date, datetime, timedelta
from functools import wraps
from utils.db import (
    get_db, db_execute, db_fetchone, db_fetchall, db_insert_and_get_id
)
from utils.decorators import login_required, permission_required
from utils.helpers import log_auditoria_evento

bp = Blueprint('pos', __name__, url_prefix='/pos')


# ── Helpers internos ───────────────────────────────────────────────────────────

def _generar_numero_venta(conn):
    """Genera el siguiente número de venta correlativo."""
    row = db_fetchone(conn, "SELECT COUNT(*) FROM pos_ventas")
    n = (row[0] if row else 0) + 1
    return f"VTA-{n:06d}"


def obtener_sesion_caja_activa():
    """Retorna la sesión de caja activa del usuario logueado, o None."""
    usuario = session.get('username')
    if not usuario:
        return None
    conn = get_db()
    sesion = db_fetchone(conn, """
        SELECT s.*, c.nombre AS caja_nombre
        FROM pos_caja_sesiones s
        JOIN pos_cajas c ON s.caja_id = c.id
        WHERE s.estado = 'abierta' AND s.usuario_apertura = ?
        LIMIT 1
    """, (usuario,))
    return sesion


def caja_abierta_required():
    """Decorador que exige una sesión de caja abierta antes de continuar."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            sesion = obtener_sesion_caja_activa()
            if not sesion:
                flash('Debe abrir una sesión de caja para acceder a esta función.', 'warning')
                return redirect(url_for('pos.cajas'))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def _registrar_asiento_pos(conn, venta_id, numero, total, pagos, usuario):
    """Crea un asiento contable en borrador para la venta POS con soporte de pagos múltiples y costo de ventas."""
    hoy = date.today().isoformat()
    
    # Obtener cuentas configurables desde ajustes_sistema, con valores por defecto
    def _get_cta(clave, default_codigo):
        row = db_fetchone(conn, "SELECT valor FROM ajustes_sistema WHERE clave=?", (clave,))
        codigo = row['valor'] if row and row['valor'] else default_codigo
        cta = db_fetchone(conn, "SELECT id FROM cont_cuentas WHERE codigo=? LIMIT 1", (codigo,))
        return cta

    cuenta_caja = _get_cta('cuenta_pos_caja', '110101')
    cuenta_ahorro = _get_cta('cuenta_pos_ahorro', '210101')
    cuenta_cxc = _get_cta('cuenta_pos_cxc', '110401')
    cuenta_cxp = _get_cta('cuenta_pos_cxp', '210201')
    cuenta_ingresos = _get_cta('cuenta_pos_ingresos', '430101')
    cuenta_inventario = _get_cta('cuenta_pos_inventario', '110501')
    cuenta_costo_ventas = _get_cta('cuenta_pos_costo_ventas', '510101')

    if not cuenta_caja or not cuenta_ingresos:
        return  # Contabilidad no configurada o incompleta

    try:
        # Calcular Costo Total de los productos vendidos (excluye servicios)
        row_cost = db_fetchone(conn, """
            SELECT SUM(d.cantidad * p.costo) as costo_total
            FROM pos_venta_detalles d
            JOIN pos_productos p ON d.producto_id = p.id
            WHERE d.venta_id = ? AND p.unidad != 'servicio'
        """, (venta_id,))
        costo_total = float(row_cost['costo_total'] or 0.0)

        n_partidas = db_fetchone(conn, "SELECT COUNT(*) FROM cont_partidas")[0] + 1
        num_partida = f"PART-{n_partidas:06d}"

        partida_id = db_insert_and_get_id(
            conn,
            """INSERT INTO cont_partidas
               (numero, fecha, descripcion, estado, origen_tipo, origen_id, usuario, fecha_creacion)
               VALUES (?,?,?,?,?,?,?,?)""",
            (num_partida, hoy, f"Venta POS {numero}", 'borrador', 'pos', venta_id, usuario, datetime.now().isoformat())
        )

        # Haber: Ingresos por Ventas
        db_execute(conn,
            "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
            (partida_id, cuenta_ingresos['id'], f"Venta {numero}", 0, total))

        # Debe: según el método de pago
        for pago in pagos:
            monto = float(pago['monto'])
            metodo = pago['metodo']
            
            if metodo == 'efectivo':
                db_execute(conn,
                    "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                    (partida_id, cuenta_caja['id'], f"Cobro efectivo venta {numero}", monto, 0))
            elif metodo == 'debito_ahorro':
                acc = cuenta_ahorro['id'] if cuenta_ahorro else cuenta_caja['id']
                db_execute(conn,
                    "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                    (partida_id, acc, f"Débito ahorro venta {numero}", monto, 0))
            elif metodo == 'credito_interno':
                acc = cuenta_cxc['id'] if cuenta_cxc else cuenta_caja['id']
                db_execute(conn,
                    "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                    (partida_id, acc, f"Crédito interno venta {numero}", monto, 0))
            elif metodo == 'nota_credito':
                acc = cuenta_cxp['id'] if cuenta_cxp else cuenta_caja['id']
                db_execute(conn,
                    "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                    (partida_id, acc, f"Consumo nota crédito venta {numero}", monto, 0))
            else: # tarjeta / etc
                db_execute(conn,
                    "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                    (partida_id, cuenta_caja['id'], f"Cobro tarjeta venta {numero}", monto, 0))

        # Costo de Ventas vs Inventario
        if costo_total > 0 and cuenta_inventario and cuenta_costo_ventas:
            # Debe: Costo de Ventas
            db_execute(conn,
                "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                (partida_id, cuenta_costo_ventas['id'], f"Costo de Venta {numero}", costo_total, 0))
            # Haber: Inventario
            db_execute(conn,
                "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                (partida_id, cuenta_inventario['id'], f"Salida Inventario {numero}", 0, costo_total))

    except Exception as e:
        import logging
        logging.getLogger('cooperativa.pos').error(f"Error registrando asiento POS: {e}")
        pass


# ── Dashboard ──────────────────────────────────────────────────────────────────

@bp.route('/')
@login_required()
def dashboard():
    conn = get_db()
    hoy_str = date.today().isoformat()

    # Parámetros de filtro
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()
    cajero = request.args.get('cajero', '').strip()
    caja_id = request.args.get('caja_id', '').strip()

    # Construir WHERE para filtros
    where_clauses = ["v.estado='completada'"]
    params = []

    if fecha_desde:
        where_clauses.append("date(v.fecha) >= date(?)")
        params.append(fecha_desde)
    if fecha_hasta:
        where_clauses.append("date(v.fecha) <= date(?)")
        params.append(fecha_hasta)
    if not fecha_desde and not fecha_hasta:
        where_clauses.append("date(v.fecha) = date(?)")
        params.append(hoy_str)

    if cajero:
        where_clauses.append("v.usuario = ?")
        params.append(cajero)

    if caja_id:
        where_clauses.append("""
            EXISTS (
                SELECT 1 FROM pos_caja_sesiones s 
                WHERE s.caja_id = ? 
                  AND v.usuario = s.usuario_apertura 
                  AND v.fecha >= s.fecha_apertura 
                  AND (s.fecha_cierre IS NULL OR v.fecha <= s.fecha_cierre)
            )
        """)
        params.append(caja_id)

    where_str = " AND ".join(where_clauses)

    # Ventas filtradas (del día o rango)
    ventas_hoy = db_fetchone(conn, f"""
        SELECT COUNT(*) AS cant, COALESCE(SUM(total), 0) AS monto
        FROM pos_ventas v WHERE {where_str}
    """, tuple(params))

    # Ventas del mes (filtradas por cajero/caja)
    where_clauses_mes = ["v.estado='completada'"]
    params_mes = []
    if cajero:
        where_clauses_mes.append("v.usuario = ?")
        params_mes.append(cajero)
    if caja_id:
        where_clauses_mes.append("""
            EXISTS (
                SELECT 1 FROM pos_caja_sesiones s 
                WHERE s.caja_id = ? 
                  AND v.usuario = s.usuario_apertura 
                  AND v.fecha >= s.fecha_apertura 
                  AND (s.fecha_cierre IS NULL OR v.fecha <= s.fecha_cierre)
            )
        """)
        params_mes.append(caja_id)
    where_clauses_mes.append("SUBSTR(v.fecha, 1, 7) = SUBSTR(?, 1, 7)")
    params_mes.append(fecha_hasta if fecha_hasta else hoy_str)
    
    where_str_mes = " AND ".join(where_clauses_mes)
    ventas_mes = db_fetchone(conn, f"""
        SELECT COUNT(*) AS cant, COALESCE(SUM(total), 0) AS monto
        FROM pos_ventas v WHERE {where_str_mes}
    """, tuple(params_mes))

    total_productos = db_fetchone(conn, "SELECT COUNT(*) FROM pos_productos WHERE estado='activo'")[0]
    productos_sin_stock = db_fetchone(conn, "SELECT COUNT(*) FROM pos_productos WHERE estado='activo' AND stock <= stock_minimo")[0]

    productos_bajo_stock = db_fetchall(conn, """
        SELECT id, nombre, stock, stock_minimo 
        FROM pos_productos 
        WHERE estado='activo' AND stock <= stock_minimo 
        ORDER BY stock ASC LIMIT 10
    """)

    # Últimas 10 ventas filtradas
    ultimas_ventas = db_fetchall(conn, f"""
        SELECT v.*, COALESCE(s.nombre || ' ' || s.apellido, v.cliente_nombre, 'Cliente General') AS nombre_cliente
        FROM pos_ventas v
        LEFT JOIN socios s ON v.socio_id = s.id
        WHERE {where_str}
        ORDER BY v.id DESC LIMIT 10
    """, tuple(params))

    # Productos más vendidos filtrados
    top_productos = db_fetchall(conn, f"""
        SELECT p.nombre, SUM(d.cantidad) AS total_vendido, SUM(d.subtotal) AS ingresos
        FROM pos_venta_detalles d
        JOIN pos_productos p ON d.producto_id = p.id
        JOIN pos_ventas v ON d.venta_id = v.id
        WHERE {where_str}
        GROUP BY p.id ORDER BY total_vendido DESC LIMIT 5
    """, tuple(params))

    # Listas para cargar los selectores de filtros
    cajeros = db_fetchall(conn, "SELECT DISTINCT usuario FROM pos_ventas WHERE usuario IS NOT NULL AND usuario != '' ORDER BY usuario")
    cajas = db_fetchall(conn, "SELECT id, nombre FROM pos_cajas WHERE estado='activo' ORDER BY nombre")

    conn.close()
    return render_template('pos_dashboard.html',
        ventas_hoy=ventas_hoy,
        ventas_mes=ventas_mes,
        total_productos=total_productos,
        productos_sin_stock=productos_sin_stock,
        productos_bajo_stock=productos_bajo_stock,
        ultimas_ventas=ultimas_ventas,
        top_productos=top_productos,
        cajeros=cajeros,
        cajas=cajas,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        cajero_sel=cajero,
        caja_sel=caja_id
    )


# ── Terminal y Ventas ───────────────────────────────────────────────────────────

@bp.route('/terminal')
@login_required()
def terminal():
    sesion = obtener_sesion_caja_activa()
    conn = get_db()
    cajas = []
    if not sesion:
        # Si no hay sesion, obtener las cajas disponibles para el modal
        cajas = db_fetchall(conn, "SELECT id, nombre FROM pos_cajas WHERE estado='activo'")
        usuario_db = db_fetchone(conn, "SELECT u.bodega_id, b.nombre as bodega_nombre FROM usuarios u LEFT JOIN pos_bodegas b ON u.bodega_id = b.id WHERE u.username=?", (session.get('username'),))
        bodega_asignada = usuario_db['bodega_nombre'] if usuario_db and usuario_db['bodega_nombre'] else "Ninguna (Configure en Ajustes)"
    else:
        bodega_asignada = None
    productos = db_fetchall(conn, """
        SELECT p.*, c.nombre AS categoria_nombre
        FROM pos_productos p
        LEFT JOIN pos_categorias c ON p.categoria_id = c.id
        WHERE p.estado = 'activo' AND p.disponible_pos = 1
        ORDER BY c.nombre, p.nombre
    """)
    categorias = db_fetchall(conn, "SELECT * FROM pos_categorias WHERE estado='activa' ORDER BY nombre")
    conn.close()
    return render_template('pos_terminal.html',
        productos=productos,
        categorias=categorias,
        sesion_caja=sesion,
        cajas_disponibles=cajas,
        bodega_asignada=bodega_asignada
    )


@bp.route('/guardar_venta', methods=['POST'])
@login_required()
@caja_abierta_required()
def guardar_venta():
    data = request.get_json() or {}
    items = data.get('items', [])
    socio_id = data.get('socio_id') or None
    cliente_nombre = (data.get('cliente_nombre') or '').strip() or 'Cliente General'
    cliente_nit = (data.get('cliente_nit') or '').strip() or 'CF'
    cliente_direccion = (data.get('cliente_direccion') or '').strip() or 'Ciudad'
    notas = (data.get('notas') or '').strip()
    descuento_global = float(data.get('descuento', 0) or 0)
    
    # Soporte de pagos múltiples
    pagos = data.get('pagos', [])
    if not pagos:
        # Retro-compatibilidad con single payment
        metodo_pago = data.get('metodo_pago', 'efectivo')
        cuenta_id = data.get('cuenta_id') or None
        pagos = [{'metodo': metodo_pago, 'monto': None, 'cuenta_id': cuenta_id}]

    puntos_a_canjear = int(data.get('puntos_canjear', 0) or 0)

    if not items:
        return jsonify({'success': False, 'error': 'El carrito está vacío.'}), 400

    conn = get_db()
    sesion = obtener_sesion_caja_activa()
    if not sesion or not dict(sesion).get('bodega_id'):
        return jsonify({'success': False, 'error': 'No hay sesión de caja activa o bodega configurada.'}), 400
    bodega_id = sesion['bodega_id']

    try:
        # 1. Validar stock de todos los productos en la bodega activa
        for item in items:
            prod = db_fetchone(conn, "SELECT * FROM pos_productos WHERE id=? AND estado='activo'", (item['id'],))
            if not prod:
                return jsonify({'success': False, 'error': f"Producto ID {item['id']} no encontrado."}), 400
            cantidad = float(item.get('cantidad', 1))
            if prod['unidad'] != 'servicio':
                stock_bodega = db_fetchone(conn, "SELECT stock FROM pos_producto_bodegas WHERE producto_id=? AND bodega_id=?", (item['id'], bodega_id))
                stock_disp = float(stock_bodega['stock']) if stock_bodega else 0.0
                if stock_disp < cantidad:
                    return jsonify({'success': False,
                        'error': f"Stock insuficiente para '{prod['nombre']}' en esta bodega. Disponible: {stock_disp}"}), 400

        # 2. Calcular Totales
        subtotal = sum(float(i.get('precio', 0)) * float(i.get('cantidad', 1)) for i in items)
        
        # Descuento por puntos de fidelidad (Q0.10 por punto)
        descuento_puntos = round(puntos_a_canjear * 0.10, 2)
        descuento_total = round(descuento_global + descuento_puntos, 2)
        total = round(max(0, subtotal - descuento_total), 2)

        # 3. Completar montos en pagos y validar saldo/crédito
        suma_pagos = 0.0
        for p in pagos:
            if p.get('monto') is None or len(pagos) == 1:
                p['monto'] = total - suma_pagos
            p['monto'] = round(float(p['monto']), 2)
            suma_pagos += p['monto']
            
            # Validar según método
            if p['metodo'] == 'debito_ahorro':
                cta_id = p.get('cuenta_id')
                if not cta_id:
                    return jsonify({'success': False, 'error': 'Debe seleccionar una cuenta para el débito de ahorro.'}), 400
                cta = db_fetchone(conn, "SELECT saldo FROM cuentas WHERE id=? AND estado='activa'", (cta_id,))
                if not cta or float(cta['saldo']) < p['monto']:
                    return jsonify({'success': False, 'error': f"Saldo de ahorros insuficiente para el débito de Q{p['monto']:.2f}"}), 400
            elif p['metodo'] == 'credito_interno':
                if not socio_id:
                    return jsonify({'success': False, 'error': 'Debe asociar un socio para utilizar crédito interno.'}), 400
                soc = db_fetchone(conn, "SELECT saldo_credito_pos, limite_credito_pos FROM socios WHERE id=?", (socio_id,))
                if not soc:
                    return jsonify({'success': False, 'error': 'Socio no encontrado.'}), 400
                soc_dict = dict(soc)
                limite_credito = float(soc_dict['limite_credito_pos'] if soc_dict.get('limite_credito_pos') is not None else 500.0)
                disponible = max(0, limite_credito - float(soc_dict.get('saldo_credito_pos', 0)))
                if disponible < p['monto']:
                    return jsonify({'success': False, 'error': f"Límite de crédito excedido. Disponible: Q{disponible:.2f}"}), 400
            elif p['metodo'] == 'nota_credito':
                nc_id = p.get('detalle_id')
                if not nc_id:
                    return jsonify({'success': False, 'error': 'Debe seleccionar una Nota de Crédito válida.'}), 400
                nc = db_fetchone(conn, "SELECT saldo_disponible FROM pos_notas_credito WHERE id=? AND estado='activo'", (nc_id,))
                if not nc or float(nc['saldo_disponible']) < p['monto']:
                    return jsonify({'success': False, 'error': f"Saldo insuficiente en la Nota de Crédito."}), 400

        if round(suma_pagos, 2) != total:
            return jsonify({'success': False, 'error': f"El total de los pagos (Q{suma_pagos:.2f}) no coincide con el total de la venta (Q{total:.2f})."}), 400

        # 4. Validar puntos de fidelidad a canjear
        if puntos_a_canjear > 0:
            if not socio_id:
                return jsonify({'success': False, 'error': 'Debe asociar un socio para redimir puntos.'}), 400
            pts_row = db_fetchone(conn, "SELECT puntos_acumulados FROM pos_puntos_fidelidad WHERE socio_id=?", (socio_id,))
            pts_disponibles = pts_row['puntos_acumulados'] if pts_row else 0
            if pts_disponibles < puntos_a_canjear:
                return jsonify({'success': False, 'error': f"Puntos insuficientes. Disponibles: {pts_disponibles}"}), 400

        numero = _generar_numero_venta(conn)
        usuario = session.get('username', 'sistema')
        fecha = datetime.now().isoformat()

        # 5. Insertar la venta
        # metodo_pago principal guardará el primer método de la lista por compatibilidad de visualización
        metodo_principal = pagos[0]['metodo']
        cuenta_principal = pagos[0].get('cuenta_id')
        
        venta_id = db_insert_and_get_id(conn, """
            INSERT INTO pos_ventas
            (numero, socio_id, cliente_nombre, cliente_nit, cliente_direccion, subtotal, descuento, total, metodo_pago, cuenta_id, estado, notas, usuario, fecha)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (numero, socio_id, cliente_nombre, cliente_nit, cliente_direccion, subtotal, descuento_total, total,
              metodo_principal, cuenta_principal, 'completada', notas, usuario, fecha))

        # 6. Guardar los detalles y descontar stock
        for item in items:
            cantidad = float(item.get('cantidad', 1))
            precio_u = float(item.get('precio', 0))
            subtotal_item = round(cantidad * precio_u, 2)
            db_execute(conn, """
                INSERT INTO pos_venta_detalles
                (venta_id, producto_id, cantidad, precio_unitario, descuento, subtotal)
                VALUES (?,?,?,?,?,?)
            """, (venta_id, item['id'], cantidad, precio_u, 0, subtotal_item))
            
            prod_row = db_fetchone(conn, "SELECT unidad FROM pos_productos WHERE id=?", (item['id'],))
            if prod_row and prod_row['unidad'] != 'servicio':
                componentes = db_fetchall(conn, "SELECT componente_id, cantidad FROM pos_producto_componentes WHERE producto_id=?", (item['id'],))
                if componentes:
                    for comp in componentes:
                        cant_comp = float(comp['cantidad']) * cantidad
                        db_execute(conn, "UPDATE pos_productos SET stock = stock - ? WHERE id=?", (cant_comp, comp['componente_id']))
                        db_execute(conn, "UPDATE pos_producto_bodegas SET stock = stock - ? WHERE producto_id=? AND bodega_id=?", (cant_comp, comp['componente_id'], bodega_id))
                else:
                    # Descontar del global y de la bodega específica
                    db_execute(conn, "UPDATE pos_productos SET stock = stock - ? WHERE id=?", (cantidad, item['id']))
                    db_execute(conn, "UPDATE pos_producto_bodegas SET stock = stock - ? WHERE producto_id=? AND bodega_id=?", (cantidad, item['id'], bodega_id))

        # 7. Registrar desglose de pagos y aplicar transacciones
        for p in pagos:
            monto_pago = p['monto']
            metodo = p['metodo']
            cta_id = p.get('cuenta_id')
            nc_id = p.get('detalle_id')
            
            db_execute(conn, """
                INSERT INTO pos_venta_pagos (venta_id, metodo_pago, cuenta_id, monto, detalle_id)
                VALUES (?,?,?,?,?)
            """, (venta_id, metodo, cta_id, monto_pago, nc_id))
            
            if metodo == 'debito_ahorro' and cta_id:
                cta = db_fetchone(conn, "SELECT saldo FROM cuentas WHERE id=?", (cta_id,))
                nuevo_saldo = round(float(cta['saldo']) - monto_pago, 2)
                db_execute(conn, "UPDATE cuentas SET saldo=? WHERE id=?", (nuevo_saldo, cta_id))
                db_execute(conn, """
                    INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha)
                    VALUES (?,?,?,?,?,?)
                """, (cta_id, 'retiro', monto_pago, nuevo_saldo, f"Pago POS {numero}", date.today().isoformat()))
                
            elif metodo == 'credito_interno':
                db_execute(conn, """
                    UPDATE socios
                    SET saldo_credito_pos = saldo_credito_pos + ?
                    WHERE id = ?
                """, (monto_pago, socio_id))
                
            elif metodo == 'nota_credito' and nc_id:
                nc = db_fetchone(conn, "SELECT saldo_disponible FROM pos_notas_credito WHERE id=?", (nc_id,))
                nuevo_saldo_nc = round(float(nc['saldo_disponible']) - monto_pago, 2)
                estado_nc = 'consumido' if nuevo_saldo_nc <= 0 else 'activo'
                db_execute(conn, """
                    UPDATE pos_notas_credito
                    SET saldo_disponible = ?, estado = ?
                    WHERE id = ?
                """, (nuevo_saldo_nc, estado_nc, nc_id))

        # 8. Procesar Fidelización (Puntos)
        if socio_id:
            # Crear registro de fidelización si no existe
            db_execute(conn, """
                INSERT OR IGNORE INTO pos_puntos_fidelidad (socio_id, puntos_acumulados, puntos_canjeados, fecha_actualizacion)
                VALUES (?, 0, 0, ?)
            """, (socio_id, date.today().isoformat()))
            
            # Redimir puntos
            if puntos_a_canjear > 0:
                db_execute(conn, """
                    UPDATE pos_puntos_fidelidad
                    SET puntos_acumulados = puntos_acumulados - ?,
                        puntos_canjeados = puntos_canjeados + ?,
                        fecha_actualizacion = ?
                    WHERE socio_id = ?
                """, (puntos_a_canjear, puntos_a_canjear, date.today().isoformat(), socio_id))
                
                db_execute(conn, """
                    INSERT INTO pos_puntos_historial (socio_id, venta_id, tipo, puntos, fecha)
                    VALUES (?, ?, 'redime', ?, ?)
                """, (socio_id, venta_id, puntos_a_canjear, date.today().isoformat()))
                
            # Acumular nuevos puntos (1 punto por cada Q10.00 de compra total)
            puntos_ganados = int(total // 10)
            if puntos_ganados > 0:
                db_execute(conn, """
                    UPDATE pos_puntos_fidelidad
                    SET puntos_acumulados = puntos_acumulados + ?,
                        fecha_actualizacion = ?
                    WHERE socio_id = ?
                """, (puntos_ganados, date.today().isoformat(), socio_id))
                
                db_execute(conn, """
                    INSERT INTO pos_puntos_historial (socio_id, venta_id, tipo, puntos, fecha)
                    VALUES (?, ?, 'acumula', ?, ?)
                """, (socio_id, venta_id, puntos_ganados, date.today().isoformat()))

        # 9. Asiento contable automático
        _registrar_asiento_pos(conn, venta_id, numero, total, pagos, usuario)

        # 10. Eliminar cotización de origen si fue facturada
        cotizacion_id = data.get('cotizacion_id')
        if cotizacion_id:
            db_execute(conn, "UPDATE pos_cotizaciones SET estado='convertida' WHERE id=?", (cotizacion_id,))

        # 11. Certificar Factura FEL (Mock)
        from utils.fel_api import certificar_factura
        
        venta_datos = {
            'numero': numero,
            'cliente_nombre': cliente_nombre,
            'cliente_nit': cliente_nit,
            'cliente_direccion': cliente_direccion,
            'total': total,
            'fecha': fecha
        }
        res_fel = certificar_factura(venta_datos, items)
        if res_fel.get('success'):
            db_execute(conn, """
                UPDATE pos_ventas
                SET fel_uuid = ?, fel_serie = ?, fel_numero = ?, fel_fecha_certificacion = ?
                WHERE id = ?
            """, (res_fel['autorizacion'], res_fel['serie'], res_fel['numero'], res_fel['fecha_certificacion'], venta_id))

        conn.commit()

        log_auditoria_evento(
            modulo='pos', entidad='venta', accion='crear',
            entidad_id=venta_id,
            descripcion=f"Venta {numero} por Q{total:.2f} — {metodo_principal}",
            datos={'numero': numero, 'total': total, 'items': len(items)}
        )

        return jsonify({'success': True, 'numero': numero, 'total': total, 'venta_id': venta_id})

    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


@bp.route('/ventas/<int:vid>/anular', methods=['POST'])
@login_required()
def anular_venta(vid):
    conn = get_db()
    try:
        venta = db_fetchone(conn, "SELECT * FROM pos_ventas WHERE id=?", (vid,))
        if not venta:
            flash('Venta no encontrada.', 'danger')
            return redirect(url_for('pos.historial'))
        if venta['estado'] == 'anulada':
            flash('Esta venta ya fue anulada.', 'warning')
            return redirect(url_for('pos.historial'))

        # Reintegrar stock
        detalles = db_fetchall(conn, "SELECT * FROM pos_venta_detalles WHERE venta_id=?", (vid,))
        for d in detalles:
            prod = db_fetchone(conn, "SELECT unidad FROM pos_productos WHERE id=?", (d['producto_id'],))
            if prod and prod['unidad'] != 'servicio':
                componentes = db_fetchall(conn, "SELECT componente_id, cantidad FROM pos_producto_componentes WHERE producto_id=?", (d['producto_id'],))
                if componentes:
                    for comp in componentes:
                        cant_comp = float(comp['cantidad']) * float(d['cantidad'])
                        db_execute(conn, "UPDATE pos_productos SET stock = stock + ? WHERE id=?", (cant_comp, comp['componente_id']))
                else:
                    db_execute(conn, "UPDATE pos_productos SET stock = stock + ? WHERE id=?",
                               (d['cantidad'], d['producto_id']))

        # Obtener los pagos realizados
        pagos = db_fetchall(conn, "SELECT * FROM pos_venta_pagos WHERE venta_id=?", (vid,))
        
        # Si no hay registros en pos_venta_pagos, usar los campos directos de pos_ventas por compatibilidad
        if not pagos:
            pagos = [{'metodo_pago': venta['metodo_pago'], 'monto': venta['total'], 'cuenta_id': venta['cuenta_id']}]

        for p in pagos:
            monto_pago = p['monto']
            metodo = p['metodo_pago']
            
            if metodo == 'debito_ahorro' and p['cuenta_id']:
                cuenta = db_fetchone(conn, "SELECT saldo FROM cuentas WHERE id=?", (p['cuenta_id'],))
                nuevo_saldo = round(float(cuenta['saldo']) + monto_pago, 2)
                db_execute(conn, "UPDATE cuentas SET saldo=? WHERE id=?", (nuevo_saldo, p['cuenta_id']))
                db_execute(conn, """
                    INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha)
                    VALUES (?,?,?,?,?,?)
                """, (p['cuenta_id'], 'deposito', monto_pago, nuevo_saldo,
                      f"Reversión POS {venta['numero']}", date.today().isoformat()))
                      
            elif metodo == 'credito_interno' and venta['socio_id']:
                db_execute(conn, """
                    UPDATE socios
                    SET saldo_credito_pos = max(0.0, saldo_credito_pos - ?)
                    WHERE id = ?
                """, (monto_pago, venta['socio_id']))
                
            elif metodo == 'nota_credito' and p['detalle_id']:
                db_execute(conn, """
                    UPDATE pos_notas_credito
                    SET saldo_disponible = saldo_disponible + ?, estado = 'activo'
                    WHERE id = ?
                """, (monto_pago, p['detalle_id']))

        # Reversar puntos otorgados y canjeados
        if venta['socio_id']:
            historial_puntos = db_fetchall(conn, "SELECT * FROM pos_puntos_historial WHERE venta_id=?", (vid,))
            for hp in historial_puntos:
                if hp['tipo'] == 'acumula':
                    # Descontar los puntos acumulados
                    db_execute(conn, """
                        UPDATE pos_puntos_fidelidad
                        SET puntos_acumulados = max(0, puntos_acumulados - ?),
                            fecha_actualizacion = ?
                        WHERE socio_id = ?
                    """, (hp['puntos'], date.today().isoformat(), venta['socio_id']))
                elif hp['tipo'] == 'redime':
                    # Devolver los puntos canjeados
                    db_execute(conn, """
                        UPDATE pos_puntos_fidelidad
                        SET puntos_acumulados = puntos_acumulados + ?,
                            puntos_canjeados = max(0, puntos_canjeados - ?),
                            fecha_actualizacion = ?
                        WHERE socio_id = ?
                    """, (hp['puntos'], hp['puntos'], date.today().isoformat(), venta['socio_id']))
                    
            # Eliminar historial de puntos de esta venta
            db_execute(conn, "DELETE FROM pos_puntos_historial WHERE venta_id=?", (vid,))

        # Anular asiento contable relacionado
        db_execute(conn,
            "UPDATE cont_partidas SET estado='anulado' WHERE (origen_tipo='pos' OR origen_tipo='pos_dev') AND origen_id=?", (vid,))

        db_execute(conn, "UPDATE pos_ventas SET estado='anulada' WHERE id=?", (vid,))
        conn.commit()

        log_auditoria_evento(
            modulo='pos', entidad='venta', accion='anular',
            entidad_id=vid,
            descripcion=f"Anulación de venta {venta['numero']}",
        )
        flash(f"Venta {venta['numero']} anulada correctamente.", 'success')

    except Exception as e:
        conn.rollback()
        flash(f'Error al anular: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.historial'))


@bp.route('/historial')
@login_required()
def historial():
    conn = get_db()
    fecha_desde = request.args.get('fecha_desde', date.today().isoformat())
    fecha_hasta = request.args.get('fecha_hasta', date.today().isoformat())
    estado_filtro = request.args.get('estado', 'todas')
    q = request.args.get('q', '').strip()

    params = [fecha_desde, fecha_hasta]
    where_extra = ""
    if estado_filtro != 'todas':
        where_extra += " AND v.estado = ?"
        params.append(estado_filtro)
    if q:
        where_extra += " AND (v.numero LIKE ? OR v.cliente_nombre LIKE ? OR s.nombre LIKE ?)"
        like = f'%{q}%'
        params += [like, like, like]

    ventas = db_fetchall(conn, f"""
        SELECT v.*,
               COALESCE(s.nombre || ' ' || s.apellido, v.cliente_nombre, 'Cliente General') AS nombre_cliente,
               (SELECT COUNT(*) FROM pos_venta_detalles WHERE venta_id=v.id) AS num_items
        FROM pos_ventas v
        LEFT JOIN socios s ON v.socio_id = s.id
        WHERE date(v.fecha) BETWEEN date(?) AND date(?) {where_extra}
        ORDER BY v.id DESC
    """, params)

    totales = db_fetchone(conn, f"""
        SELECT COUNT(*) AS cant, COALESCE(SUM(v.total), 0) AS monto
        FROM pos_ventas v
        LEFT JOIN socios s ON v.socio_id = s.id
        WHERE date(v.fecha) BETWEEN date(?) AND date(?) AND v.estado='completada' {where_extra.replace('v.estado = ?', '1=1') if estado_filtro == 'todas' else where_extra}
    """, params if estado_filtro == 'todas' else params)

    conn.close()
    return render_template('pos_historial.html',
        ventas=ventas,
        totales=totales,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        estado_filtro=estado_filtro,
        q=q,
    )


@bp.route('/historial/exportar')
@login_required()
def exportar_historial():
    import csv
    from io import StringIO
    from flask import Response
    
    conn = get_db()
    fecha_desde = request.args.get('fecha_desde', date.today().isoformat())
    fecha_hasta = request.args.get('fecha_hasta', date.today().isoformat())
    estado_filtro = request.args.get('estado', 'todas')
    q = request.args.get('q', '').strip()

    params = [fecha_desde, fecha_hasta]
    where_extra = ""
    if estado_filtro != 'todas':
        where_extra += " AND v.estado = ?"
        params.append(estado_filtro)
    if q:
        where_extra += " AND (v.numero LIKE ? OR v.cliente_nombre LIKE ? OR s.nombre LIKE ?)"
        like = f'%{q}%'
        params += [like, like, like]

    ventas = db_fetchall(conn, f"""
        SELECT v.*,
               COALESCE(s.nombre || ' ' || s.apellido, v.cliente_nombre, 'Cliente General') AS nombre_cliente,
               (SELECT COUNT(*) FROM pos_venta_detalles WHERE venta_id=v.id) AS num_items
        FROM pos_ventas v
        LEFT JOIN socios s ON v.socio_id = s.id
        WHERE date(v.fecha) BETWEEN date(?) AND date(?) {where_extra}
        ORDER BY v.id DESC
    """, params)
    conn.close()

    si = StringIO()
    cw = csv.writer(si)
    cw.writerow(['Numero Venta', 'Fecha', 'Cliente / Asociado', 'Items', 'Total', 'Metodo Pago', 'Estado', 'FEL Autorización'])
    
    for v in ventas:
        metodo = v['metodo_pago']
        if metodo == 'efectivo':
            metodo_str = "Efectivo"
        elif metodo == 'debito_ahorro':
            metodo_str = "Debito Ahorro"
        elif metodo == 'credito_interno':
            metodo_str = "Credito POS"
        elif metodo == 'nota_credito':
            metodo_str = "Nota Credito"
        else:
            metodo_str = "Tarjeta"
            
        cw.writerow([
            v['numero'],
            v['fecha'],
            v['nombre_cliente'],
            v['num_items'],
            f"Q{v['total']:.2f}",
            metodo_str,
            v['estado'].capitalize(),
            v['fel_uuid'] or 'N/A'
        ])
    
    output = si.getvalue()
    return Response(
        output,
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename=historial_ventas_{fecha_desde}_al_{fecha_hasta}.csv"}
    )


@bp.route('/ventas/<int:vid>')
@login_required()
def detalle_venta(vid):
    conn = get_db()
    venta = db_fetchone(conn, """
        SELECT v.*, COALESCE(s.nombre || ' ' || s.apellido, v.cliente_nombre, 'Cliente General') AS nombre_cliente,
               s.codigo AS socio_codigo
        FROM pos_ventas v LEFT JOIN socios s ON v.socio_id=s.id WHERE v.id=?
    """, (vid,))
    if not venta:
        conn.close()
        flash('Venta no encontrada.', 'danger')
        return redirect(url_for('pos.historial'))
    detalles = db_fetchall(conn, """
        SELECT d.*, p.nombre AS producto_nombre, p.codigo AS producto_codigo
        FROM pos_venta_detalles d JOIN pos_productos p ON d.producto_id=p.id WHERE d.venta_id=?
    """, (vid,))
    
    pagos = db_fetchall(conn, "SELECT * FROM pos_venta_pagos WHERE venta_id=?", (vid,))
    
    # Devoluciones hechas para esta venta
    devoluciones_venta = db_fetchall(conn, "SELECT * FROM pos_devoluciones WHERE venta_id=?", (vid,))

    conn.close()
    return render_template('pos_detalle_venta.html', venta=venta, detalles=detalles, pagos=pagos, devoluciones=devoluciones_venta)

@bp.route('/api/ultimas_ventas')
@login_required()
def ultimas_ventas():
    conn = get_db()
    # Traemos las ultimas 20 ventas del día o caja activa
    ventas = db_fetchall(conn, """
        SELECT v.id, v.numero, v.fecha, v.total, 
               COALESCE(s.nombre || ' ' || s.apellido, v.cliente_nombre, 'Cliente General') AS cliente
        FROM pos_ventas v
        LEFT JOIN socios s ON v.socio_id=s.id
        ORDER BY v.id DESC LIMIT 20
    """)
    conn.close()
    
    # Convert sqlite3.Row to dict
    lista_ventas = []
    for v in ventas:
        lista_ventas.append({
            'id': v['id'],
            'numero': v['numero'],
            'fecha': v['fecha'],
            'total': float(v['total']),
            'cliente': v['cliente']
        })
    
    return jsonify({'success': True, 'ventas': lista_ventas})


@bp.route('/ventas/<int:vid>/ticket_json')
@login_required()
def ticket_json(vid):
    conn = get_db()
    venta = db_fetchone(conn, """
        SELECT v.*, COALESCE(s.nombre || ' ' || s.apellido, v.cliente_nombre, 'Cliente General') AS nombre_cliente,
               s.codigo AS socio_codigo, s.saldo_credito_pos, s.limite_credito_pos
        FROM pos_ventas v LEFT JOIN socios s ON v.socio_id=s.id WHERE v.id=?
    """, (vid,))
    if not venta:
        conn.close()
        return jsonify({'success': False, 'error': 'Venta no encontrada.'}), 404
        
    detalles = db_fetchall(conn, """
        SELECT d.*, p.nombre AS producto_nombre, p.codigo AS producto_codigo
        FROM pos_venta_detalles d JOIN pos_productos p ON d.producto_id=p.id WHERE d.venta_id=?
    """, (vid,))
    
    pagos = db_fetchall(conn, "SELECT * FROM pos_venta_pagos WHERE venta_id=?", (vid,))
    conn.close()
    
    pagos_desglose = []
    if pagos:
        for p in pagos:
            metodo = p['metodo_pago']
            if metodo == 'efectivo':
                label = "💵 Efectivo"
            elif metodo == 'debito_ahorro':
                label = "🏦 Débito de Ahorro"
            elif metodo == 'credito_interno':
                label = "📝 Crédito Interno"
            elif metodo == 'nota_credito':
                label = "🔄 Nota de Crédito"
            else:
                label = "💳 Tarjeta"
            pagos_desglose.append({
                'label': label,
                'metodo': metodo,
                'monto': p['monto']
            })
    else:
        metodo = venta['metodo_pago']
        if metodo == 'efectivo':
            label = "💵 Efectivo"
        elif metodo == 'debito_ahorro':
            label = "🏦 Débito de Ahorro"
        elif metodo == 'credito_interno':
            label = "📝 Crédito Interno"
        elif metodo == 'nota_credito':
            label = "🔄 Nota de Crédito"
        else:
            label = "💳 Tarjeta"
        pagos_desglose.append({
            'label': label,
            'metodo': metodo,
            'monto': venta['total']
        })

    return jsonify({
        'success': True,
        'venta': {
            'id': venta['id'],
            'numero': venta['numero'],
            'fecha': venta['fecha'],
            'total': venta['total'],
            'subtotal': venta['subtotal'],
            'descuento': venta['descuento'],
            'nombre_cliente': venta['nombre_cliente'],
            'socio_codigo': venta['socio_codigo'],
            'cliente_nit': venta['cliente_nit'] or 'CF',
            'cliente_direccion': venta['cliente_direccion'] or 'Ciudad',
            'usuario': venta['usuario'] or 'Sistema',
            'fel_uuid': venta['fel_uuid'],
            'fel_serie': venta['fel_serie'],
            'fel_numero': venta['fel_numero'],
            'fel_fecha_certificacion': venta['fel_fecha_certificacion'],
            'saldo_credito': dict(venta).get('saldo_credito_pos', 0),
            'limite_credito': dict(venta).get('limite_credito_pos', 500)
        },
        'detalles': [{
            'cantidad': d['cantidad'],
            'producto_nombre': d['producto_nombre'],
            'producto_codigo': d['producto_codigo'],
            'precio_unitario': d['precio_unitario'],
            'subtotal': d['subtotal']
        } for d in detalles],
        'pagos': pagos_desglose
    })


# ── Productos ──────────────────────────────────────────────────────────────────


@bp.route('/productos')
@login_required()
def productos():
    conn = get_db()
    q = request.args.get('q', '').strip()
    cat_filtro = request.args.get('categoria', '')
    params, where = [], ""
    if q:
        where += " AND (p.nombre LIKE ? OR p.codigo LIKE ? OR p.codigo_barras LIKE ?)"
        params += [f'%{q}%', f'%{q}%', f'%{q}%']
    if cat_filtro:
        where += " AND p.categoria_id = ?"
        params.append(cat_filtro)
    prods = db_fetchall(conn, f"""
        SELECT p.*, c.nombre AS categoria_nombre, prov.nombre AS proveedor_nombre
        FROM pos_productos p 
        LEFT JOIN pos_categorias c ON p.categoria_id=c.id
        LEFT JOIN pos_proveedores prov ON p.proveedor_id=prov.id
        WHERE 1=1 {where} ORDER BY p.nombre
    """, params)
    productos_dict = [dict(row) for row in prods]
    categorias = [dict(row) for row in db_fetchall(conn, "SELECT * FROM pos_categorias ORDER BY nombre")]
    proveedores = db_fetchall(conn, "SELECT * FROM pos_proveedores WHERE estado='activo' ORDER BY nombre")
    conn.close()
    return render_template('pos_productos.html',
        productos=productos_dict, categorias=categorias, proveedores=proveedores, q=q, cat_filtro=cat_filtro)

@bp.route('/producto/<int:id>/stock_bodegas', methods=['GET'])
@login_required()
def producto_stock_bodegas(id):
    conn = get_db()
    stocks = db_fetchall(conn, """
        SELECT b.nombre, pb.stock 
        FROM pos_producto_bodegas pb
        JOIN pos_bodegas b ON pb.bodega_id = b.id
        WHERE pb.producto_id = ?
        ORDER BY b.nombre
    """, (id,))
    conn.close()
    return jsonify([dict(s) for s in stocks])


@bp.route('/productos/form', methods=['GET'])
@login_required()
def producto_form():
    conn = get_db()
    pid = request.args.get('id')
    producto = None
    if pid:
        producto = db_fetchone(conn, "SELECT * FROM pos_productos WHERE id=?", (pid,))
        if not producto:
            flash('Producto no encontrado.', 'warning')
            conn.close()
            return redirect(url_for('pos.productos'))
            
    categorias = db_fetchall(conn, "SELECT * FROM pos_categorias WHERE estado='activa' ORDER BY nombre")
    proveedores = db_fetchall(conn, "SELECT id, nombre FROM pos_proveedores WHERE estado='activo' ORDER BY nombre")
    conn.close()
    return render_template('pos_producto_form.html', producto=producto, categorias=categorias, proveedores=proveedores)

@bp.route('/productos/guardar', methods=['POST'])
@login_required()
def guardar_producto():
    conn = get_db()
    try:
        pid = request.form.get('id') or None
        codigo = request.form.get('codigo', '').strip().upper()
        nombre = request.form.get('nombre', '').strip()
        descripcion = request.form.get('descripcion', '').strip()
        categoria_id = request.form.get('categoria_id') or None
        precio_venta = float(request.form.get('precio_venta', 0) or 0)
        precio_socio = float(request.form.get('precio_socio', 0) or 0)
        costo = float(request.form.get('costo', 0) or 0)
        stock = float(request.form.get('stock', 0) or 0)
        stock_minimo = float(request.form.get('stock_minimo', 0) or 0)
        stock_maximo = float(request.form.get('stock_maximo', 0) or 0)
        unidad = request.form.get('unidad', 'unidad').strip()
        estado = request.form.get('estado', 'activo')
        codigo_barras = request.form.get('codigo_barras', '').strip() or None
        referencia_proveedor = request.form.get('referencia_proveedor', '').strip() or None
        proveedor_id = request.form.get('proveedor_id') or None
        venta_rapida = 1 if request.form.get('venta_rapida') == '1' else 0
        puede_venderse = 1 if request.form.get('puede_venderse') == '1' else 0
        puede_comprarse = 1 if request.form.get('puede_comprarse') == '1' else 0
        disponible_pos = 1 if request.form.get('disponible_pos') == '1' else 0

        if proveedor_id:
            proveedor_id = int(proveedor_id)

        imagen_filename = None
        if 'imagen' in request.files:
            file = request.files['imagen']
            if file and file.filename != '':
                filename = secure_filename(file.filename)
                upload_folder = os.path.join(current_app.root_path, 'static', 'uploads', 'pos_productos')
                os.makedirs(upload_folder, exist_ok=True)
                file_path = os.path.join(upload_folder, filename)
                file.save(file_path)
                imagen_filename = f'uploads/pos_productos/{filename}'

        if not codigo or not nombre:
            flash('Código y nombre son obligatorios.', 'danger')
            return redirect(url_for('pos.productos'))

        if pid:
            if imagen_filename:
                db_execute(conn, """
                    UPDATE pos_productos SET codigo=?,nombre=?,descripcion=?,categoria_id=?,
                    precio_venta=?,precio_socio=?,costo=?,stock=?,stock_minimo=?,stock_maximo=?,unidad=?,estado=?,codigo_barras=?,referencia_proveedor=?,proveedor_id=?,imagen=?,venta_rapida=?,puede_venderse=?,puede_comprarse=?,disponible_pos=? WHERE id=?
                """, (codigo, nombre, descripcion, categoria_id, precio_venta, precio_socio,
                      costo, stock, stock_minimo, stock_maximo, unidad, estado, codigo_barras, referencia_proveedor, proveedor_id, imagen_filename, venta_rapida, puede_venderse, puede_comprarse, disponible_pos, pid))
            else:
                db_execute(conn, """
                    UPDATE pos_productos SET codigo=?,nombre=?,descripcion=?,categoria_id=?,
                    precio_venta=?,precio_socio=?,costo=?,stock=?,stock_minimo=?,stock_maximo=?,unidad=?,estado=?,codigo_barras=?,referencia_proveedor=?,proveedor_id=?,venta_rapida=?,puede_venderse=?,puede_comprarse=?,disponible_pos=? WHERE id=?
                """, (codigo, nombre, descripcion, categoria_id, precio_venta, precio_socio,
                      costo, stock, stock_minimo, stock_maximo, unidad, estado, codigo_barras, referencia_proveedor, proveedor_id, venta_rapida, puede_venderse, puede_comprarse, disponible_pos, pid))
            flash('Producto actualizado.', 'success')
            redirect_id = pid
        else:
            redirect_id = db_insert_and_get_id(conn, """
                INSERT INTO pos_productos
                (codigo,nombre,descripcion,categoria_id,precio_venta,precio_socio,costo,stock,stock_minimo,stock_maximo,unidad,estado,codigo_barras,referencia_proveedor,proveedor_id,imagen,venta_rapida,puede_venderse,puede_comprarse,disponible_pos,fecha_creacion)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (codigo, nombre, descripcion, categoria_id, precio_venta, precio_socio,
                  costo, stock, stock_minimo, stock_maximo, unidad, estado, codigo_barras, referencia_proveedor, proveedor_id, imagen_filename, venta_rapida, puede_venderse, puede_comprarse, disponible_pos, date.today().isoformat()))
            flash('Producto creado exitosamente.', 'success')
        conn.commit()
    except Exception as e:
        flash(f'Error: {e}', 'danger')
        return redirect(url_for('pos.productos'))
    finally:
        conn.close()
    return redirect(url_for('pos.producto_form', id=redirect_id))


# ── Categorías de Productos ───────────────────────────────────────────────────

@bp.route('/categorias/guardar', methods=['POST'])
@login_required()
def guardar_categoria():
    conn = get_db()
    try:
        cid = request.form.get('id') or None
        nombre = request.form.get('nombre', '').strip()
        descripcion = request.form.get('descripcion', '').strip()
        if not nombre:
            flash('El nombre de la categoría es obligatorio.', 'danger')
            return redirect(url_for('pos.productos'))
            
        if cid:
            db_execute(conn, "UPDATE pos_categorias SET nombre=?, descripcion=? WHERE id=?", (nombre, descripcion, cid))
            flash('Categoría actualizada.', 'success')
        else:
            db_execute(conn, "INSERT INTO pos_categorias (nombre, descripcion) VALUES (?, ?)", (nombre, descripcion))
            flash('Categoría creada exitosamente.', 'success')
        conn.commit()
    except Exception as e:
        flash(f'Error: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.productos'))

@bp.route('/categorias/eliminar/<int:id>', methods=['POST'])
@login_required()
def eliminar_categoria(id):
    conn = get_db()
    try:
        prod_count = db_fetchone(conn, "SELECT COUNT(*) as cnt FROM pos_productos WHERE categoria_id = ?", (id,))
        if prod_count and prod_count['cnt'] > 0:
            flash('No se puede eliminar la categoría porque tiene productos asociados.', 'danger')
        else:
            db_execute(conn, "DELETE FROM pos_categorias WHERE id = ?", (id,))
            conn.commit()
            flash('Categoría eliminada.', 'success')
    except Exception as e:
        flash(f'Error: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.productos'))


# ── Cajas y Turnos ─────────────────────────────────────────────────────────────

@bp.route('/cajas')
@login_required()
def cajas():
    conn = get_db()
    cajas_disponibles = db_fetchall(conn, "SELECT * FROM pos_cajas WHERE estado='activo'")
    bodegas_disponibles = db_fetchall(conn, "SELECT id, nombre FROM pos_bodegas WHERE estado='activo'")
    usuario = session.get('username')
    usuario_db = db_fetchone(conn, "SELECT u.bodega_id, b.nombre as bodega_nombre FROM usuarios u LEFT JOIN pos_bodegas b ON u.bodega_id = b.id WHERE u.username=?", (usuario,))
    bodega_usuario_id = usuario_db['bodega_id'] if usuario_db else None
    bodega_asignada = usuario_db['bodega_nombre'] if usuario_db and usuario_db['bodega_nombre'] else "Ninguna (Configure en Ajustes)"
    
    sesion_activa = db_fetchone(conn, """
        SELECT s.*, c.nombre AS caja_nombre
        FROM pos_caja_sesiones s
        JOIN pos_cajas c ON s.caja_id = c.id
        WHERE s.estado = 'abierta' AND s.usuario_apertura = ?
    """, (usuario,))
    
    historial_sesiones = db_fetchall(conn, """
        SELECT s.*, c.nombre AS caja_nombre
        FROM pos_caja_sesiones s
        JOIN pos_cajas c ON s.caja_id = c.id
        ORDER BY s.id DESC LIMIT 15
    """)
    
    saldo_esperado = 0
    movimientos = []
    if sesion_activa:
        ventas_efectivo = db_fetchone(conn, """
            SELECT COALESCE(SUM(vp.monto), 0) AS total
            FROM pos_venta_pagos vp
            JOIN pos_ventas v ON vp.venta_id = v.id
            WHERE v.estado = 'completada' AND vp.metodo_pago = 'efectivo'
              AND v.fecha >= ?
        """, (sesion_activa['fecha_apertura'],))[0]
        
        ventas_efectivo_viejas = db_fetchone(conn, """
            SELECT COALESCE(SUM(total), 0) AS total
            FROM pos_ventas
            WHERE estado = 'completada' AND metodo_pago = 'efectivo' AND fecha >= ?
              AND id NOT IN (SELECT DISTINCT venta_id FROM pos_venta_pagos)
        """, (sesion_activa['fecha_apertura'],))[0]
        
        ventas_efectivo += ventas_efectivo_viejas
        
        movs = db_fetchall(conn, "SELECT * FROM pos_caja_movimientos WHERE sesion_id=? ORDER BY id DESC", (sesion_activa['id'],))
        total_entradas = sum(m['monto'] for m in movs if m['tipo'] == 'entrada')
        total_salidas = sum(m['monto'] for m in movs if m['tipo'] == 'salida')
        
        saldo_esperado = sesion_activa['saldo_apertura'] + ventas_efectivo + total_entradas - total_salidas
        movimientos = movs

    conn.close()
    return render_template('pos_cajas.html',
        cajas=cajas_disponibles,
        bodegas=bodegas_disponibles,
        bodega_usuario_id=bodega_usuario_id,
        bodega_asignada=bodega_asignada,
        sesion_activa=sesion_activa,
        historial=historial_sesiones,
        saldo_esperado=saldo_esperado,
        movimientos=movimientos
    )


@bp.route('/cajas/abrir', methods=['POST'])
@login_required()
def cajas_abrir():
    caja_id = request.form.get('caja_id')
    saldo_apertura = float(request.form.get('saldo_apertura', 0) or 0)
    notas = request.form.get('notes', '').strip() or request.form.get('notas', '').strip()
    usuario = session.get('username')
    
    next_url = request.form.get('next') or url_for('pos.cajas')
    if not caja_id:
        flash('Debe seleccionar una caja.', 'danger')
        return redirect(next_url)
        
    conn = get_db()
    try:
        usuario_db = db_fetchone(conn, "SELECT bodega_id FROM usuarios WHERE username=?", (usuario,))
        bodega_id = usuario_db['bodega_id'] if usuario_db else None
        
        if not bodega_id:
            flash('Su usuario no tiene una bodega de inventario asignada. Por favor, configure una bodega en los Ajustes.', 'danger')
            return redirect(next_url)
            
        caja_ocupada = db_fetchone(conn, "SELECT id FROM pos_caja_sesiones WHERE caja_id=? AND estado='abierta'", (caja_id,))
        if caja_ocupada:
            flash('Esta caja ya está abierta en otra sesión activa.', 'danger')
            return redirect(next_url)
            
        usuario_ocupado = db_fetchone(conn, "SELECT id FROM pos_caja_sesiones WHERE usuario_apertura=? AND estado='abierta'", (usuario,))
        if usuario_ocupado:
            flash('Usted ya tiene una sesión de caja abierta.', 'danger')
            return redirect(next_url)
            
        fecha_apertura = datetime.now().isoformat()
        db_execute(conn, """
            INSERT INTO pos_caja_sesiones (caja_id, bodega_id, usuario_apertura, fecha_apertura, saldo_apertura, estado, notas)
            VALUES (?,?,?,?,?,?,?)
        """, (caja_id, bodega_id, usuario, fecha_apertura, saldo_apertura, 'abierta', notas))
        conn.commit()
        flash('Caja abierta correctamente.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error al abrir caja: {e}', 'danger')
    finally:
        conn.close()
        
    next_url = request.form.get('next') or url_for('pos.cajas')
    return redirect(next_url)


@bp.route('/cajas/cerrar', methods=['POST'])
@login_required()
def cajas_cerrar():
    saldo_cierre = float(request.form.get('saldo_cierre', 0) or 0)
    notas = request.form.get('notes', '').strip() or request.form.get('notas', '').strip()
    usuario = session.get('username')
    
    conn = get_db()
    try:
        sesion = db_fetchone(conn, "SELECT * FROM pos_caja_sesiones WHERE usuario_apertura=? AND estado='abierta'", (usuario,))
        if not sesion:
            flash('No tiene ninguna sesión de caja abierta.', 'danger')
            return redirect(url_for('pos.cajas'))
            
        ventas_efectivo = db_fetchone(conn, """
            SELECT COALESCE(SUM(vp.monto), 0) AS total
            FROM pos_venta_pagos vp
            JOIN pos_ventas v ON vp.venta_id = v.id
            WHERE v.estado = 'completada' AND vp.metodo_pago = 'efectivo'
              AND v.fecha >= ?
        """, (sesion['fecha_apertura'],))[0]
        
        ventas_efectivo_viejas = db_fetchone(conn, """
            SELECT COALESCE(SUM(total), 0) AS total
            FROM pos_ventas
            WHERE estado = 'completada' AND metodo_pago = 'efectivo' AND fecha >= ?
              AND id NOT IN (SELECT DISTINCT venta_id FROM pos_venta_pagos)
        """, (sesion['fecha_apertura'],))[0]
        
        ventas_efectivo += ventas_efectivo_viejas
        
        movs = db_fetchall(conn, "SELECT * FROM pos_caja_movimientos WHERE sesion_id=?", (sesion['id'],))
        total_entradas = sum(m['monto'] for m in movs if m['tipo'] == 'entrada')
        total_salidas = sum(m['monto'] for m in movs if m['tipo'] == 'salida')
        
        saldo_esperado = sesion['saldo_apertura'] + ventas_efectivo + total_entradas - total_salidas
        fecha_cierre = datetime.now().isoformat()
        
        db_execute(conn, """
            UPDATE pos_caja_sesiones
            SET usuario_cierre = ?, fecha_cierre = ?, saldo_cierre = ?, saldo_esperado = ?, estado = ?, notas = ?
            WHERE id = ?
        """, (usuario, fecha_cierre, saldo_cierre, saldo_esperado, 'cerrada', notas, sesion['id']))
        
        conn.commit()
        flash('Caja cerrada y arqueo completado exitosamente.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error al cerrar caja: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.cajas'))


@bp.route('/cajas/sesion/<int:sid>/reporte')
@login_required()
def cajas_sesion_reporte(sid):
    conn = get_db()
    sesion = db_fetchone(conn, """
        SELECT s.*, c.nombre AS caja_nombre
        FROM pos_caja_sesiones s
        JOIN pos_cajas c ON s.caja_id = c.id
        WHERE s.id = ?
    """, (sid,))
    
    if not sesion:
        conn.close()
        flash("Sesión no encontrada.", "danger")
        return redirect(url_for('pos.cajas'))
        
    fecha_limite = sesion['fecha_cierre'] if sesion['fecha_cierre'] else datetime.now().isoformat()
    
    ventas = db_fetchall(conn, """
        SELECT v.total, v.metodo_pago, v.id, v.fecha
        FROM pos_ventas v
        WHERE v.estado = 'completada' 
          AND v.usuario = ?
          AND v.fecha >= ? AND v.fecha <= ?
    """, (sesion['usuario_apertura'], sesion['fecha_apertura'], fecha_limite))
    
    totals_por_metodo = {
        'efectivo': 0.0,
        'debito_ahorro': 0.0,
        'credito_interno': 0.0,
        'nota_credito': 0.0,
        'tarjeta': 0.0
    }
    
    for v in ventas:
        pagos = db_fetchall(conn, "SELECT metodo_pago, monto FROM pos_venta_pagos WHERE venta_id=?", (v['id'],))
        if pagos:
            for p in pagos:
                metodo = p['metodo_pago']
                if metodo in totals_por_metodo:
                    totals_por_metodo[metodo] += p['monto']
        else:
            metodo = v['metodo_pago']
            if metodo in totals_por_metodo:
                totals_por_metodo[metodo] += v['total']
                
    movimientos = db_fetchall(conn, """
        SELECT * FROM pos_caja_movimientos 
        WHERE sesion_id = ? 
        ORDER BY id ASC
    """, (sid,))
    
    # Ventas por hora
    ventas_por_hora = {}
    top_productos = {}
    for v in ventas:
        hora = v['fecha'][11:13]
        ventas_por_hora[hora] = ventas_por_hora.get(hora, 0.0) + float(v['total'])
        
        detalles = db_fetchall(conn, "SELECT p.nombre, d.cantidad, d.subtotal FROM pos_venta_detalles d JOIN pos_productos p ON d.producto_id = p.id WHERE d.venta_id=?", (v['id'],))
        for d in detalles:
            nombre = d['nombre'][:25]
            if nombre not in top_productos:
                top_productos[nombre] = {'cantidad': 0, 'subtotal': 0.0}
            top_productos[nombre]['cantidad'] += d['cantidad']
            top_productos[nombre]['subtotal'] += d['subtotal']
            
    ventas_por_hora_list = [{'hora': f"{k}:00", 'total': v} for k, v in sorted(ventas_por_hora.items())]
    top_productos_list = sorted(
        [{'nombre': k, 'cantidad': v['cantidad'], 'subtotal': v['subtotal']} for k, v in top_productos.items()],
        key=lambda x: x['subtotal'], reverse=True
    )[:5]
    
    conn.close()
    
    return render_template('pos_corte_caja.html', 
                           sesion=sesion, 
                           totals=totals_por_metodo, 
                           movimientos=movimientos,
                           ventas_por_hora=ventas_por_hora_list,
                           top_productos=top_productos_list)


@bp.route('/cajas/movimiento', methods=['POST'])
@login_required()
def cajas_movimiento():
    tipo = request.form.get('tipo')
    monto = float(request.form.get('monto', 0) or 0)
    motivo = request.form.get('motivo', '').strip()
    usuario = session.get('username')
    
    if tipo not in ('entrada', 'salida') or monto <= 0 or not motivo:
        flash('Datos de movimiento inválidos.', 'danger')
        return redirect(url_for('pos.cajas'))
        
    conn = get_db()
    try:
        sesion = db_fetchone(conn, "SELECT id FROM pos_caja_sesiones WHERE usuario_apertura=? AND estado='abierta'", (usuario,))
        if not sesion:
            flash('Debe tener una caja abierta para registrar movimientos de efectivo.', 'danger')
            return redirect(url_for('pos.cajas'))
            
        fecha = datetime.now().isoformat()
        db_execute(conn, """
            INSERT INTO pos_caja_movimientos (sesion_id, tipo, monto, motivo, fecha, usuario)
            VALUES (?,?,?,?,?,?)
        """, (sesion['id'], tipo, monto, motivo, fecha, usuario))
        
        conn.commit()
        flash(f'Movimiento de {tipo} registrado correctamente.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error al registrar movimiento: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.cajas'))


# ── Cotizaciones ───────────────────────────────────────────────────────────────

@bp.route('/cotizaciones')
@login_required()
def cotizaciones():
    conn = get_db()
    cots = db_fetchall(conn, """
        SELECT c.*, COALESCE(s.nombre || ' ' || s.apellido, c.cliente_nombre, 'Cliente General') AS nombre_cliente
        FROM pos_cotizaciones c
        LEFT JOIN socios s ON c.socio_id = s.id
        ORDER BY c.id DESC
    """)
    conn.close()
    return render_template('pos_cotizaciones.html', cotizaciones=cots)


@bp.route('/cotizaciones/guardar', methods=['POST'])
@login_required()
def cotizaciones_guardar():
    data = request.get_json() or {}
    items = data.get('items', [])
    socio_id = data.get('socio_id') or None
    cliente_nombre = (data.get('cliente_nombre') or '').strip() or 'Cliente General'
    notas = (data.get('notas') or '').strip()
    descuento_global = float(data.get('descuento', 0) or 0)
    
    if not items:
        return jsonify({'success': False, 'error': 'El carrito está vacío.'}), 400
        
    conn = get_db()
    try:
        n_cots = db_fetchone(conn, "SELECT COUNT(*) FROM pos_cotizaciones")[0] + 1
        numero = f"COT-{n_cots:06d}"
        
        subtotal = sum(float(i.get('precio', 0)) * float(i.get('cantidad', 1)) for i in items)
        total = round(max(0, subtotal - descuento_global), 2)
        usuario = session.get('username', 'sistema')
        fecha = date.today().isoformat()
        fecha_vencimiento = (datetime.now() + timedelta(days=15)).date().isoformat()
        
        cot_id = db_insert_and_get_id(conn, """
            INSERT INTO pos_cotizaciones
            (numero, socio_id, cliente_nombre, subtotal, descuento, total, fecha, fecha_vencimiento, estado, notas, usuario)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (numero, socio_id, cliente_nombre, subtotal, descuento_global, total, fecha, fecha_vencimiento, 'borrador', notas, usuario))
        
        for item in items:
            sub = round(float(item.get('cantidad', 1)) * float(item.get('precio', 0)), 2)
            db_execute(conn, """
                INSERT INTO pos_cotizacion_detalles
                (cotizacion_id, producto_id, cantidad, precio_unitario, descuento, subtotal)
                VALUES (?,?,?,?,?,?)
            """, (cot_id, item['id'], float(item.get('cantidad', 1)), float(item.get('precio', 0)), 0, sub))
            
        conn.commit()
        return jsonify({'success': True, 'numero': numero, 'cotizacion_id': cot_id})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


@bp.route('/cotizaciones/<int:cid>/cargar')
@login_required()
def cotizaciones_cargar(cid):
    conn = get_db()
    cot = db_fetchone(conn, "SELECT * FROM pos_cotizaciones WHERE id=?", (cid,))
    if not cot:
        conn.close()
        return jsonify({'success': False, 'error': 'Cotización no encontrada'}), 404
        
    detalles = db_fetchall(conn, """
        SELECT d.*, p.nombre, p.stock, p.unidad, p.codigo
        FROM pos_cotizacion_detalles d
        JOIN pos_productos p ON d.producto_id = p.id
        WHERE d.cotizacion_id = ?
    """, (cid,))
    
    socio = None
    if cot['socio_id']:
        s_row = db_fetchone(conn, """
            SELECT s.id, s.codigo, s.nombre, s.apellido,
                   (SELECT c.id FROM cuentas c WHERE c.socio_id=s.id AND c.tipo='ahorro'
                    AND c.producto_ahorro='ahorro_corriente' AND c.estado='activa' LIMIT 1) AS cuenta_id,
                   (SELECT c.saldo FROM cuentas c WHERE c.socio_id=s.id AND c.tipo='ahorro'
                    AND c.producto_ahorro='ahorro_corriente' AND c.estado='activa' LIMIT 1) AS saldo
            FROM socios s WHERE s.id = ?
        """, (cot['socio_id'],))
        if s_row:
            socio = dict(s_row)
            
    conn.close()
    
    items = []
    for d in detalles:
        items.append({
            'id': d['producto_id'],
            'codigo': d['codigo'],
            'nombre': d['nombre'],
            'precio': d['precio_unitario'],
            'qty': d['cantidad'],
            'stock': d['stock'],
            'unidad': d['unidad']
        })
        
    return jsonify({
        'success': True,
        'cotizacion': dict(cot),
        'items': items,
        'socio': socio
    })


@bp.route('/cotizaciones/<int:cid>/eliminar', methods=['POST'])
@login_required()
def cotizaciones_eliminar(cid):
    conn = get_db()
    try:
        db_execute(conn, "DELETE FROM pos_cotizacion_detalles WHERE cotizacion_id=?", (cid,))
        db_execute(conn, "DELETE FROM pos_cotizaciones WHERE id=?", (cid,))
        conn.commit()
        flash('Cotización eliminada correctamente.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error al eliminar cotización: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.cotizaciones'))


# ── Devoluciones y Notas de Crédito ─────────────────────────────────────────────

@bp.route('/devoluciones')
@login_required()
def devoluciones():
    conn = get_db()
    devs = db_fetchall(conn, """
        SELECT d.*, v.numero AS venta_numero, COALESCE(s.nombre || ' ' || s.apellido, 'Cliente General') AS nombre_socio
        FROM pos_devoluciones d
        JOIN pos_ventas v ON d.venta_id = v.id
        LEFT JOIN socios s ON d.socio_id = s.id
        ORDER BY d.id DESC
    """)
    conn.close()
    return render_template('pos_devoluciones.html', devoluciones=devs)


@bp.route('/ventas/<int:vid>/devolver', methods=['POST'])
@login_required()
def procesar_devolucion(vid):
    data = request.get_json() or {}
    items_devolver = data.get('items', [])
    motivo = data.get('motivo', '').strip() or 'Devolución de mercadería'
    usuario = session.get('username', 'sistema')
    
    if not items_devolver:
        return jsonify({'success': False, 'error': 'No se especificaron productos para devolver.'}), 400
        
    conn = get_db()
    sesion = obtener_sesion_caja_activa()
    bodega_id = sesion['bodega_id'] if (sesion and dict(sesion).get('bodega_id')) else None
    
    try:
        venta = db_fetchone(conn, "SELECT * FROM pos_ventas WHERE id=?", (vid,))
        if not venta:
            return jsonify({'success': False, 'error': 'Venta no encontrada.'}), 404
        if venta['estado'] == 'anulada':
            return jsonify({'success': False, 'error': 'No se puede devolver una venta anulada.'}), 400
            
        detalles_originales = {d['producto_id']: d for d in db_fetchall(conn, "SELECT * FROM pos_venta_detalles WHERE venta_id=?", (vid,))}
        
        devueltos_previos = {}
        prev_devs = db_fetchall(conn, """
            SELECT dd.producto_id, SUM(dd.cantidad) as total_cant
            FROM pos_devolucion_detalles dd
            JOIN pos_devoluciones d ON dd.devolucion_id = d.id
            WHERE d.venta_id = ?
            GROUP BY dd.producto_id
        """, (vid,))
        for p in prev_devs:
            devueltos_previos[p['producto_id']] = p['total_cant']
            
        total_reembolsado = 0
        lineas_devolucion = []
        
        for item in items_devolver:
            prod_id = int(item['producto_id'])
            cant_a_dev = float(item['cantidad'])
            if cant_a_dev <= 0:
                continue
                
            if prod_id not in detalles_originales:
                return jsonify({'success': False, 'error': f'El producto ID {prod_id} no pertenece a esta venta.'}), 400
                
            orig = detalles_originales[prod_id]
            ya_devuelto = devueltos_previos.get(prod_id, 0)
            disponible_para_dev = orig['cantidad'] - ya_devuelto
            
            if cant_a_dev > disponible_para_dev:
                return jsonify({'success': False, 'error': f"Cantidad excedida para devolver. Disponible: {disponible_para_dev}"}), 400
                
            subtotal_item = round(cant_a_dev * orig['precio_unitario'], 2)
            total_reembolsado += subtotal_item
            lineas_devolucion.append({
                'producto_id': prod_id,
                'cantidad': cant_a_dev,
                'precio_unitario': orig['precio_unitario'],
                'subtotal': subtotal_item
            })
            
        if not lineas_devolucion:
            return jsonify({'success': False, 'error': 'Ningún producto válido para devolver.'}), 400
            
        n_devs = db_fetchone(conn, "SELECT COUNT(*) FROM pos_devoluciones")[0] + 1
        num_dev = f"DEV-{n_devs:06d}"
        fecha = datetime.now().isoformat()
        
        dev_id = db_insert_and_get_id(conn, """
            INSERT INTO pos_devoluciones (numero, venta_id, socio_id, total_reembolsado, fecha, usuario, motivo)
            VALUES (?,?,?,?,?,?,?)
        """, (num_dev, vid, venta['socio_id'], total_reembolsado, fecha, usuario, motivo))
        
        for l in lineas_devolucion:
            db_execute(conn, """
                INSERT INTO pos_devolucion_detalles (devolucion_id, producto_id, cantidad, precio_unitario, subtotal)
                VALUES (?,?,?,?,?)
            """, (dev_id, l['producto_id'], l['cantidad'], l['precio_unitario'], l['subtotal']))
            
            prod = db_fetchone(conn, "SELECT unidad FROM pos_productos WHERE id=?", (l['producto_id'],))
            if prod and prod['unidad'] != 'servicio':
                componentes = db_fetchall(conn, "SELECT componente_id, cantidad FROM pos_producto_componentes WHERE producto_id=?", (l['producto_id'],))
                if componentes:
                    for comp in componentes:
                        cant_comp = float(comp['cantidad']) * float(l['cantidad'])
                        db_execute(conn, "UPDATE pos_productos SET stock = stock + ? WHERE id=?", (cant_comp, comp['componente_id']))
                        if bodega_id:
                            row_stock = db_fetchone(conn, "SELECT stock FROM pos_producto_bodegas WHERE producto_id=? AND bodega_id=?", (comp['componente_id'], bodega_id))
                            if row_stock:
                                db_execute(conn, "UPDATE pos_producto_bodegas SET stock = stock + ? WHERE producto_id=? AND bodega_id=?", (cant_comp, comp['componente_id'], bodega_id))
                            else:
                                db_execute(conn, "INSERT INTO pos_producto_bodegas (producto_id, bodega_id, stock) VALUES (?,?,?)", (comp['componente_id'], bodega_id, cant_comp))
                else:
                    db_execute(conn, "UPDATE pos_productos SET stock = stock + ? WHERE id=?", (l['cantidad'], l['producto_id']))
                    if bodega_id:
                        row_stock = db_fetchone(conn, "SELECT stock FROM pos_producto_bodegas WHERE producto_id=? AND bodega_id=?", (l['producto_id'], bodega_id))
                        if row_stock:
                            db_execute(conn, "UPDATE pos_producto_bodegas SET stock = stock + ? WHERE producto_id=? AND bodega_id=?", (l['cantidad'], l['producto_id'], bodega_id))
                        else:
                            db_execute(conn, "INSERT INTO pos_producto_bodegas (producto_id, bodega_id, stock) VALUES (?,?,?)", (l['producto_id'], bodega_id, l['cantidad']))
                
        if venta['socio_id']:
            n_nc = db_fetchone(conn, "SELECT COUNT(*) FROM pos_notas_credito")[0] + 1
            num_nc = f"NC-{n_nc:06d}"
            db_execute(conn, """
                INSERT INTO pos_notas_credito (numero, socio_id, monto_original, saldo_disponible, fecha, estado, venta_origen_id)
                VALUES (?,?,?,?,?,?,?)
            """, (num_nc, venta['socio_id'], total_reembolsado, total_reembolsado, date.today().isoformat(), 'activo', vid))
            reembolso_metodo = f"Nota de Crédito {num_nc}"
        else:
            reembolso_metodo = "Efectivo"
            
        # Integración contable del reembolso
        try:
            cuenta_ingreso = db_fetchone(conn, "SELECT id FROM cont_cuentas WHERE codigo='430101' LIMIT 1")
            cuenta_nc = db_fetchone(conn, "SELECT id FROM cont_cuentas WHERE codigo='210201' LIMIT 1")
            cuenta_caja = db_fetchone(conn, "SELECT id FROM cont_cuentas WHERE codigo='110101' LIMIT 1")
            
            if cuenta_ingreso:
                n_partidas = db_fetchone(conn, "SELECT COUNT(*) FROM cont_partidas")[0] + 1
                num_partida = f"PART-{n_partidas:06d}"
                partida_id = db_insert_and_get_id(conn, """
                    INSERT INTO cont_partidas (numero, fecha, descripcion, estado, origen_tipo, origen_id, usuario, fecha_creacion)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (num_partida, date.today().isoformat(), f"Devolución {num_dev} de venta {venta['numero']}", 'borrador', 'pos_dev', dev_id, usuario, datetime.now().isoformat()))
                
                db_execute(conn, "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                           (partida_id, cuenta_ingreso['id'], f"Devolución {num_dev}", total_reembolsado, 0))
                
                acc_dest = cuenta_nc['id'] if (venta['socio_id'] and cuenta_nc) else cuenta_caja['id']
                db_execute(conn, "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                           (partida_id, acc_dest, f"Reembolso devolución {num_dev}", 0, total_reembolsado))
        except Exception:
            pass
            
        conn.commit()
        
        log_auditoria_evento(
            modulo='pos', entidad='devolucion', accion='crear',
            entidad_id=dev_id,
            descripcion=f"Devolución {num_dev} de venta {venta['numero']} por Q{total_reembolsado:.2f}",
        )
        
        return jsonify({'success': True, 'numero': num_dev, 'total': total_reembolsado, 'reembolso_metodo': reembolso_metodo})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


# ── Proveedores y Compras ───────────────────────────────────────────────────────

@bp.route('/proveedores')
@login_required()
def proveedores():
    conn = get_db()
    provs = db_fetchall(conn, "SELECT * FROM pos_proveedores ORDER BY nombre")
    proveedores_dict = [dict(row) for row in provs]
    conn.close()
    return render_template('pos_proveedores.html', proveedores=proveedores_dict)

@bp.route('/proveedores/nuevo')
@login_required()
def proveedores_nuevo():
    return render_template('pos_proveedor_form.html', p={})

@bp.route('/proveedores/<int:pid>/editar')
@login_required()
def proveedores_editar(pid):
    conn = get_db()
    prov = db_fetchone(conn, "SELECT * FROM pos_proveedores WHERE id=?", (pid,))
    conn.close()
    if not prov:
        flash('Proveedor no encontrado.', 'warning')
        return redirect(url_for('pos.proveedores'))
    return render_template('pos_proveedor_form.html', p=dict(prov))


@bp.route('/proveedores/guardar', methods=['POST'])
@login_required()
def proveedores_guardar():
    pid = request.form.get('id')
    nombre = request.form.get('nombre', '').strip()
    vendedor_nombre = request.form.get('vendedor_nombre', '').strip()
    nit = request.form.get('nit', '').strip()
    telefono = request.form.get('telefono', '').strip()
    email = request.form.get('email', '').strip()
    direccion = request.form.get('direccion', '').strip()
    estado = request.form.get('estado', 'activo')
    terminos_pago = request.form.get('terminos_pago', 'Pago inmediato').strip()
    metodo_pago = request.form.get('metodo_pago', 'Cheque').strip()
    banco_nombre = request.form.get('banco_nombre', '').strip()
    banco_tipo_cuenta = request.form.get('banco_tipo_cuenta', '').strip()
    banco_numero_cuenta = request.form.get('banco_numero_cuenta', '').strip()
    try:
        dias_credito = int(request.form.get('dias_credito', 0))
    except ValueError:
        dias_credito = 0
    
    if not nombre:
        flash('El nombre del proveedor es obligatorio.', 'danger')
        return redirect(url_for('pos.proveedores'))
        
    conn = get_db()
    try:
        if pid:
            db_execute(conn, """
                UPDATE pos_proveedores
                SET nombre=?, vendedor_nombre=?, nit=?, telefono=?, email=?, direccion=?, dias_credito=?, terminos_pago=?, metodo_pago=?, banco_nombre=?, banco_tipo_cuenta=?, banco_numero_cuenta=?, estado=?
                WHERE id=?
            """, (nombre, vendedor_nombre, nit, telefono, email, direccion, dias_credito, terminos_pago, metodo_pago, banco_nombre, banco_tipo_cuenta, banco_numero_cuenta, estado, pid))
            flash('Proveedor actualizado correctamente.', 'success')
        else:
            db_execute(conn, """
                INSERT INTO pos_proveedores (nombre, vendedor_nombre, nit, telefono, email, direccion, dias_credito, terminos_pago, metodo_pago, banco_nombre, banco_tipo_cuenta, banco_numero_cuenta, estado)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (nombre, vendedor_nombre, nit, telefono, email, direccion, dias_credito, terminos_pago, metodo_pago, banco_nombre, banco_tipo_cuenta, banco_numero_cuenta, 'activo'))
            flash('Proveedor creado correctamente.', 'success')
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f'Error al guardar proveedor: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.proveedores'))


@bp.route('/compras')
@login_required()
def compras():
    import csv
    from io import StringIO
    from flask import Response

    estado_filter = request.args.get('estado', 'todos')
    fecha_filter = request.args.get('fecha', 'todos')
    q_filter = request.args.get('q', '').strip()
    sort_by = request.args.get('sort', 'fecha')
    sort_dir = request.args.get('dir', 'desc').lower()
    page = int(request.args.get('page', 1))
    per_page = 20

    if sort_dir not in ['asc', 'desc']:
        sort_dir = 'desc'

    order_map = {
        'referencia': 'c.numero',
        'proveedor': 'p.nombre',
        'fecha': 'c.fecha',
        'total': 'c.total',
        'estado': 'c.estado',
        'pago': 'c.estado_pago'
    }
    order_col = order_map.get(sort_by, 'c.id')

    query_base = """
        FROM pos_compras c
        JOIN pos_proveedores p ON c.proveedor_id = p.id
        LEFT JOIN pos_bodegas b ON c.bodega_id = b.id
        WHERE 1=1
    """
    params = []

    if estado_filter != 'todos':
        query_base += " AND c.estado = ?"
        params.append(estado_filter)
        
    if fecha_filter == 'este_mes':
        query_base += " AND strftime('%Y-%m', c.fecha) = strftime('%Y-%m', 'now')"
    elif fecha_filter == 'este_anio':
        query_base += " AND strftime('%Y', c.fecha) = strftime('%Y', 'now')"
        
    if q_filter:
        query_base += " AND (c.numero LIKE ? OR p.nombre LIKE ?)"
        params.extend([f"%{q_filter}%", f"%{q_filter}%"])

    conn = get_db()
    
    # Total General
    total_general = db_fetchone(conn, f"SELECT SUM(c.total) {query_base}", params)[0] or 0.0
    
    # Total items for pagination
    total_items = db_fetchone(conn, f"SELECT COUNT(*) {query_base}", params)[0] or 0
    total_pages = (total_items + per_page - 1) // per_page
    if page < 1: page = 1
    if page > total_pages and total_pages > 0: page = total_pages
    
    offset = (page - 1) * per_page

    cmps = db_fetchall(conn, f"""
        SELECT c.*, p.nombre AS proveedor_nombre, b.nombre AS bodega_nombre
        {query_base}
        ORDER BY {order_col} {sort_dir}
        LIMIT ? OFFSET ?
    """, params + [per_page, offset])

    provs = db_fetchall(conn, "SELECT id, nombre FROM pos_proveedores WHERE estado='activo' ORDER BY nombre")
    bodegas = db_fetchall(conn, "SELECT id, nombre FROM pos_bodegas WHERE estado='activo' ORDER BY nombre")
    prods = db_fetchall(conn, "SELECT id, nombre, precio_venta, costo, stock FROM pos_productos WHERE estado='activo' ORDER BY nombre")
    conn.close()
    
    return render_template('pos_compras.html', 
        compras=cmps, 
        proveedores=provs, 
        bodegas=bodegas, 
        productos=prods,
        total_general=total_general,
        page=page,
        total_pages=total_pages,
        estado_filter=estado_filter,
        fecha_filter=fecha_filter,
        q_filter=q_filter,
        sort_by=sort_by,
        sort_dir=sort_dir
    )

@bp.route('/compras/exportar')
@login_required()
def compras_exportar():
    import csv
    from io import StringIO
    from flask import Response
    
    estado_filter = request.args.get('estado', 'todos')
    fecha_filter = request.args.get('fecha', 'todos')
    q_filter = request.args.get('q', '').strip()
    sort_by = request.args.get('sort', 'fecha')
    sort_dir = request.args.get('dir', 'desc').lower()

    if sort_dir not in ['asc', 'desc']:
        sort_dir = 'desc'

    order_map = {
        'referencia': 'c.numero',
        'proveedor': 'p.nombre',
        'fecha': 'c.fecha',
        'total': 'c.total',
        'estado': 'c.estado',
        'pago': 'c.estado_pago'
    }
    order_col = order_map.get(sort_by, 'c.id')

    query_base = """
        FROM pos_compras c
        JOIN pos_proveedores p ON c.proveedor_id = p.id
        LEFT JOIN pos_bodegas b ON c.bodega_id = b.id
        WHERE 1=1
    """
    params = []

    if estado_filter != 'todos':
        query_base += " AND c.estado = ?"
        params.append(estado_filter)
        
    if fecha_filter == 'este_mes':
        query_base += " AND strftime('%Y-%m', c.fecha) = strftime('%Y-%m', 'now')"
    elif fecha_filter == 'este_anio':
        query_base += " AND strftime('%Y', c.fecha) = strftime('%Y', 'now')"
        
    if q_filter:
        query_base += " AND (c.numero LIKE ? OR p.nombre LIKE ?)"
        params.extend([f"%{q_filter}%", f"%{q_filter}%"])

    conn = get_db()
    cmps = db_fetchall(conn, f"""
        SELECT c.numero, p.nombre AS proveedor, c.fecha, c.total, c.estado, c.estado_pago
        {query_base}
        ORDER BY {order_col} {sort_dir}
    """, params)
    conn.close()

    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['Referencia', 'Proveedor', 'Fecha', 'Total', 'Estado', 'Pago'])
    for c in cmps:
        writer.writerow([c['numero'], c['proveedor'], c['fecha'], f"{c['total']:.2f}", c['estado'], c['estado_pago']])
    
    output = si.getvalue()
    return Response(
        output,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename=compras_export.csv"}
    )

@bp.route('/compras/<int:cid>/pagar', methods=['POST'])
@login_required()
def compras_pagar(cid):
    conn = get_db()
    db_execute(conn, "UPDATE pos_compras SET estado_pago = 'Pagado' WHERE id = ?", (cid,))
    conn.commit()
    conn.close()
    flash('Compra marcada como pagada.', 'success')
    return redirect(request.referrer or url_for('pos.compras'))


@bp.route('/compras/nueva')
@login_required()
def compras_nueva():
    conn = get_db()
    provs = db_fetchall(conn, "SELECT id, nombre FROM pos_proveedores WHERE estado='activo' ORDER BY nombre")
    bodegas = db_fetchall(conn, "SELECT id, nombre FROM pos_bodegas WHERE estado='activo' ORDER BY nombre")
    prods = db_fetchall(conn, "SELECT id, nombre, precio_venta, costo, stock, proveedor_id, stock_maximo FROM pos_productos WHERE estado='activo' AND puede_comprarse = 1 ORDER BY nombre")
    conn.close()
    
    fecha_actual = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    
    return render_template('pos_nueva_compra.html', 
        proveedores=provs, 
        productos=prods, 
        bodegas=bodegas,
        fecha_actual=fecha_actual
    )



@bp.route('/compras/autogenerar', methods=['POST'])
@login_required()
def compras_autogenerar():
    conn = get_db()
    try:
        productos_bajos = db_fetchall(conn, """
            SELECT id, costo, stock_minimo, stock
            FROM pos_productos 
            WHERE estado='activo' AND stock <= stock_minimo
        """)
        
        if not productos_bajos:
            flash('No hay productos con stock mínimo para comprar.', 'info')
            return redirect(url_for('pos.dashboard'))
            
        prov = db_fetchone(conn, "SELECT id FROM pos_proveedores LIMIT 1")
        if not prov:
            flash('No hay proveedores registrados. Debe crear uno primero para autogenerar cotizaciones.', 'danger')
            return redirect(url_for('pos.proveedores'))
            
        proveedor_id = prov['id']
        total = 0
        
        n_compras = db_fetchone(conn, "SELECT COUNT(*) FROM pos_compras")[0] + 1
        num_compra = f"CMP-{n_compras:06d}"
        fecha = datetime.now().isoformat()
        usuario = session.get('username', 'sistema')
        notas = "Cotización Autogenerada por Alerta de Stock Mínimo"
        
        comp_id = db_insert_and_get_id(conn, """
            INSERT INTO pos_compras (numero, proveedor_id, total, fecha, estado, usuario, notas)
            VALUES (?,?,?,?,?,?,?)
        """, (num_compra, proveedor_id, 0, fecha, 'cotizacion', usuario, notas))
        
        for p in productos_bajos:
            cant = 1.0  # Se pide al menos 1 por defecto, el usuario debe editar
            costo_u = float(p['costo'])
            sub = cant * costo_u
            total += sub
            
            db_execute(conn, """
                INSERT INTO pos_compra_detalles (compra_id, producto_id, cantidad, costo_unitario, subtotal)
                VALUES (?,?,?,?,?)
            """, (comp_id, p['id'], cant, costo_u, sub))
            
        db_execute(conn, "UPDATE pos_compras SET total=? WHERE id=?", (total, comp_id))
        conn.commit()
        
        flash(f'Cotización {num_compra} autogenerada exitosamente. Ajuste las cantidades según necesite.', 'success')
        return redirect(url_for('pos.compras'))
        
    except Exception as e:
        conn.rollback()
        flash(f'Error al autogenerar: {str(e)}', 'danger')
        return redirect(url_for('pos.dashboard'))
    finally:
        conn.close()

@bp.route('/compras/<int:cid>/editar')
@login_required()
def compras_editar(cid):
    conn = get_db()
    compra = db_fetchone(conn, "SELECT * FROM pos_compras WHERE id=?", (cid,))
    if not compra or compra['estado'] != 'cotizacion':
        conn.close()
        flash('La orden de compra no existe o no puede ser modificada en este estado.', 'warning')
        return redirect(url_for('pos.compras'))
        
    detalles = db_fetchall(conn, "SELECT * FROM pos_compra_detalles WHERE compra_id=?", (cid,))
    provs = db_fetchall(conn, "SELECT id, nombre FROM pos_proveedores WHERE estado='activo' ORDER BY nombre")
    bodegas = db_fetchall(conn, "SELECT id, nombre FROM pos_bodegas WHERE estado='activo' ORDER BY nombre")
    prods = db_fetchall(conn, "SELECT id, nombre, precio_venta, costo, stock, proveedor_id, stock_maximo FROM pos_productos WHERE estado='activo' AND puede_comprarse = 1 ORDER BY nombre")
    conn.close()
    
    compra_dict = dict(compra)
    compra_dict['items'] = [dict(d) for d in detalles]
    
    fecha_actual = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    
    return render_template('pos_nueva_compra.html', 
        proveedores=provs, 
        productos=prods, 
        bodegas=bodegas,
        fecha_actual=fecha_actual,
        compra=compra_dict
    )

@bp.route('/compras/guardar', methods=['POST'])
@login_required()
def compras_guardar():
    data = request.get_json() or {}
    proveedor_id = data.get('proveedor_id')
    items = data.get('items', [])
    notas = data.get('notas', '').strip()
    compra_id = data.get('compra_id')
    usuario = session.get('username', 'sistema')
    
    if not proveedor_id or not items:
        return jsonify({'success': False, 'error': 'Proveedor e ítems son obligatorios.'}), 400
        
    conn = get_db()
    try:
        total = round(sum(float(i['cantidad']) * float(i['costo']) for i in items), 2)
        
        if compra_id:
            # Modo edición
            compra = db_fetchone(conn, "SELECT id, numero, estado FROM pos_compras WHERE id=?", (compra_id,))
            if not compra or compra['estado'] != 'cotizacion':
                return jsonify({'success': False, 'error': 'La compra no existe o ya no puede ser modificada.'}), 400
                
            db_execute(conn, "UPDATE pos_compras SET proveedor_id=?, notas=?, total=? WHERE id=?", 
                       (proveedor_id, notas, total, compra_id))
            db_execute(conn, "DELETE FROM pos_compra_detalles WHERE compra_id=?", (compra_id,))
            num_compra = compra['numero']
            comp_id = compra_id
        else:
            # Modo creación
            n_compras = db_fetchone(conn, "SELECT COUNT(*) FROM pos_compras")[0] + 1
            num_compra = f"CMP-{n_compras:06d}"
            fecha = datetime.now().isoformat()
            
            comp_id = db_insert_and_get_id(conn, """
                INSERT INTO pos_compras (numero, proveedor_id, total, fecha, estado, usuario, notas)
                VALUES (?,?,?,?,?,?,?)
            """, (num_compra, proveedor_id, total, fecha, 'cotizacion', usuario, notas))
        
        for item in items:
            prod_id = int(item['producto_id'])
            cant = float(item['cantidad'])
            costo_u = float(item['costo'])
            sub = round(cant * costo_u, 2)
            
            db_execute(conn, """
                INSERT INTO pos_compra_detalles (compra_id, producto_id, cantidad, costo_unitario, subtotal)
                VALUES (?,?,?,?,?)
            """, (comp_id, prod_id, cant, costo_u, sub))
            
        conn.commit()
        return jsonify({'success': True, 'numero': num_compra, 'total': total, 'compra_id': comp_id})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()

@bp.route('/compras/<int:cid>/factura', methods=['GET'])
@login_required()
def compras_factura(cid):
    conn = get_db()
    compra = db_fetchone(conn, """
        SELECT c.*, p.nombre as proveedor_nombre, p.terminos_pago as terminos_pago
        FROM pos_compras c
        LEFT JOIN pos_proveedores p ON c.proveedor_id = p.id
        WHERE c.id=?
    """, (cid,))
    if not compra or compra['estado'] != 'cotizacion':
        conn.close()
        flash('La compra no es una cotización válida o ya fue procesada.', 'warning')
        return redirect(url_for('pos.compras'))
        
    detalles = db_fetchall(conn, """
        SELECT d.*, p.nombre as producto_nombre
        FROM pos_compra_detalles d
        LEFT JOIN pos_productos p ON d.producto_id = p.id
        WHERE d.compra_id=?
    """, (cid,))
    bodegas = db_fetchall(conn, "SELECT id, nombre FROM pos_bodegas WHERE estado='activo' ORDER BY nombre")
    prods = db_fetchall(conn, "SELECT id, nombre, precio_venta, costo, stock, proveedor_id, stock_maximo FROM pos_productos WHERE estado='activo' AND puede_comprarse = 1 ORDER BY nombre")
    conn.close()
    
    fecha_actual = datetime.now()
    terminos = compra['terminos_pago'] or 'Pago inmediato'
    
    if terminos == '15 días':
        fv = fecha_actual + timedelta(days=15)
    elif terminos == '21 días':
        fv = fecha_actual + timedelta(days=21)
    elif terminos == '30 días':
        fv = fecha_actual + timedelta(days=30)
    elif terminos == '45 días':
        fv = fecha_actual + timedelta(days=45)
    elif terminos == 'Fin del siguiente mes':
        next_month = fecha_actual.month % 12 + 1
        year = fecha_actual.year + (fecha_actual.month // 12)
        import calendar
        last_day = calendar.monthrange(year, next_month)[1]
        fv = datetime(year, next_month, last_day)
    elif terminos == '10 días después del fin del siguiente mes':
        next_month = fecha_actual.month % 12 + 1
        year = fecha_actual.year + (fecha_actual.month // 12)
        import calendar
        last_day = calendar.monthrange(year, next_month)[1]
        fv = datetime(year, next_month, last_day) + timedelta(days=10)
    else:
        fv = fecha_actual

    return render_template('pos_ingreso_factura.html', 
        compra=compra, 
        detalles=detalles, 
        bodegas=bodegas,
        productos=prods,
        fecha_actual_iso=fecha_actual.strftime('%Y-%m-%d'),
        fecha_actual=fecha_actual.strftime('%d/%m/%Y'),
        fecha_vencimiento=fv.strftime('%d/%m/%Y')
    )

@bp.route('/compras/<int:cid>/ingresar_factura', methods=['POST'])
@login_required()
def compras_ingresar_factura(cid):
    data = request.get_json() or {}
    numero_factura = data.get('numero_factura', '').strip()
    bodega_id = data.get('bodega_id')
    items = data.get('items', [])
    usuario = session.get('username', 'sistema')

    if not numero_factura or not bodega_id or not items:
        return jsonify({'success': False, 'error': 'Debe ingresar el número de factura, bodega destino y al menos un producto.'}), 400

    conn = get_db()
    try:
        compra = db_fetchone(conn, "SELECT * FROM pos_compras WHERE id=?", (cid,))
        if not compra or compra['estado'] != 'cotizacion':
            return jsonify({'success': False, 'error': 'La compra no es una cotización válida.'}), 400

        total = round(sum(float(i['cantidad']) * float(i['costo']) for i in items), 2)

        # Actualizar estado y datos de factura
        db_execute(conn, """
            UPDATE pos_compras 
            SET estado='recibida', numero_factura=?, bodega_id=?, total=?
            WHERE id=?
        """, (numero_factura, bodega_id, total, cid))

        # Reemplazar detalles
        db_execute(conn, "DELETE FROM pos_compra_detalles WHERE compra_id=?", (cid,))
        for item in items:
            prod_id = int(item['producto_id'])
            cant = float(item['cantidad'])
            costo_u = float(item['costo'])
            sub = round(cant * costo_u, 2)
            db_execute(conn, """
                INSERT INTO pos_compra_detalles (compra_id, producto_id, cantidad, costo_unitario, subtotal)
                VALUES (?,?,?,?,?)
            """, (cid, prod_id, cant, costo_u, sub))

        # Aumentar stock de productos y actualizar costo promedio ponderado
        detalles = db_fetchall(conn, "SELECT * FROM pos_compra_detalles WHERE compra_id=?", (cid,))
        for d in detalles:
            pid = d['producto_id']
            cant = float(d['cantidad'])
            costo_u = float(d['costo_unitario'])
            
            prod = db_fetchone(conn, "SELECT stock, costo FROM pos_productos WHERE id = ?", (pid,))
            stock_actual = float(prod['stock'] or 0.0)
            costo_actual = float(prod['costo'] or 0.0)
            
            nuevo_stock = stock_actual + cant
            nuevo_costo = costo_u
            if nuevo_stock > 0:
                nuevo_costo = ((stock_actual * costo_actual) + (cant * costo_u)) / nuevo_stock
            
            # Global stock y costo
            db_execute(conn, "UPDATE pos_productos SET stock = ?, costo = ? WHERE id = ?", (nuevo_stock, round(nuevo_costo, 4), pid))
            
            # Stock por bodega
            row_dest = db_fetchone(conn, "SELECT stock FROM pos_producto_bodegas WHERE producto_id=? AND bodega_id=?", (pid, bodega_id))
            if row_dest:
                db_execute(conn, "UPDATE pos_producto_bodegas SET stock = stock + ? WHERE producto_id=? AND bodega_id=?", (cant, pid, bodega_id))
            else:
                db_execute(conn, "INSERT INTO pos_producto_bodegas (producto_id, bodega_id, stock) VALUES (?,?,?)", (pid, bodega_id, cant))

        # Asiento contable
        try:
            def _get_cta(clave, default_codigo):
                row = db_fetchone(conn, "SELECT valor FROM ajustes_sistema WHERE clave=?", (clave,))
                codigo = row['valor'] if row and row['valor'] else default_codigo
                cta = db_fetchone(conn, "SELECT id FROM cont_cuentas WHERE codigo=? LIMIT 1", (codigo,))
                return cta

            cuenta_inventario = _get_cta('cuenta_pos_inventario', '110501')
            cuenta_cxp = _get_cta('cuenta_pos_cxp', '210201')
            cuenta_caja = _get_cta('cuenta_pos_caja', '110101') # fallback
            cuenta_credito = cuenta_cxp if cuenta_cxp else cuenta_caja
            
            if cuenta_inventario and cuenta_credito:
                n_partidas = db_fetchone(conn, "SELECT COUNT(*) FROM cont_partidas")[0] + 1
                num_partida = f"PART-{n_partidas:06d}"
                partida_id = db_insert_and_get_id(conn, """
                    INSERT INTO cont_partidas (numero, fecha, descripcion, estado, origen_tipo, origen_id, usuario, fecha_creacion)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (num_partida, date.today().isoformat(), f"Factura {numero_factura} Compra {compra['numero']}", 'borrador', 'pos_compra', cid, usuario, datetime.now().isoformat()))
                
                db_execute(conn, "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                           (partida_id, cuenta_inventario['id'], f"Ingreso inventario F.{numero_factura}", total, 0))
                db_execute(conn, "INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber) VALUES (?,?,?,?,?)",
                           (partida_id, cuenta_credito['id'], f"Pago/Pasivo compra F.{numero_factura}", 0, total))
        except Exception as e:
            import logging
            logging.getLogger('cooperativa.pos').error(f"Error registrando asiento COMPRA: {e}")
            pass

        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()
# ── Ajustes de Inventario ──────────────────────────────────────────────────────

@bp.route('/ajustes')
@login_required()
def ajustes():
    conn = get_db()
    from utils.db import get_system_setting
    ajustes_list = db_fetchall(conn, "SELECT * FROM pos_ajustes_inventario ORDER BY id DESC")
    prods = db_fetchall(conn, "SELECT id, nombre, stock FROM pos_productos WHERE estado='activo' ORDER BY nombre")
    bodegas = db_fetchall(conn, "SELECT * FROM pos_bodegas ORDER BY id")
    usuarios = db_fetchall(conn, """
        SELECT u.id, u.username, u.bodega_id, b.nombre AS bodega_nombre
        FROM usuarios u
        LEFT JOIN pos_bodegas b ON u.bodega_id = b.id
        WHERE u.activo='si'
        ORDER BY u.username
    """)
    
    fel_certificador = get_system_setting(conn, 'fel_certificador', 'MOCK')
    fel_user = get_system_setting(conn, 'fel_user', '')
    fel_password = get_system_setting(conn, 'fel_password', '')
    fel_nit = get_system_setting(conn, 'fel_nit', '')
    fel_api_url = get_system_setting(conn, 'fel_api_url', '')
    
    conn.close()
    return render_template('pos_ajustes.html', 
                           ajustes=ajustes_list, 
                           productos=prods, 
                           bodegas=bodegas, 
                           usuarios=usuarios,
                           fel_certificador=fel_certificador,
                           fel_user=fel_user,
                           fel_password=fel_password,
                           fel_nit=fel_nit,
                           fel_api_url=fel_api_url)


@bp.route('/ajustes/guardar_fel', methods=['POST'])
@login_required()
def ajustes_guardar_fel():
    from utils.db import set_system_setting
    certificador = request.form.get('fel_certificador', 'MOCK').strip()
    user = request.form.get('fel_user', '').strip()
    password = request.form.get('fel_password', '').strip()
    nit = request.form.get('fel_nit', '').strip()
    api_url = request.form.get('fel_api_url', '').strip()
    
    usuario = session.get('username')
    conn = get_db()
    try:
        set_system_setting(conn, 'fel_certificador', certificador, usuario)
        set_system_setting(conn, 'fel_user', user, usuario)
        set_system_setting(conn, 'fel_password', password, usuario)
        set_system_setting(conn, 'fel_nit', nit, usuario)
        set_system_setting(conn, 'fel_api_url', api_url, usuario)
        conn.commit()
        flash('Configuración FEL guardada exitosamente.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error al guardar configuración: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.ajustes') + '?tab=fel')

@bp.route('/ajustes/asignar_bodega', methods=['POST'])
@login_required()
def ajustes_asignar_bodega():
    usuario_id = request.form.get('usuario_id')
    bodega_id = request.form.get('bodega_id')
    
    if not usuario_id:
        flash('Seleccione un usuario.', 'danger')
        return redirect(url_for('pos.ajustes'))
        
    conn = get_db()
    try:
        db_execute(conn, "UPDATE usuarios SET bodega_id=? WHERE id=?", (bodega_id or None, usuario_id))
        conn.commit()
        flash('Bodega asignada al usuario correctamente.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error al asignar: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.ajustes'))


@bp.route('/ajustes/guardar', methods=['POST'])
@login_required()
def ajustes_guardar():
    data = request.get_json() or {}
    tipo = data.get('tipo', 'fisico')
    notas = data.get('notas', '').strip()
    items = data.get('items', [])
    usuario = session.get('username', 'sistema')
    
    if not items:
        return jsonify({'success': False, 'error': 'Debe especificar al menos un producto para ajustar.'}), 400
        
    conn = get_db()
    try:
        n_ajustes = db_fetchone(conn, "SELECT COUNT(*) FROM pos_ajustes_inventario")[0] + 1
        num_ajuste = f"AJU-{n_ajustes:06d}"
        fecha = datetime.now().isoformat()
        
        ajuste_id = db_insert_and_get_id(conn, """
            INSERT INTO pos_ajustes_inventario (numero, tipo, fecha, usuario, notas)
            VALUES (?,?,?,?,?)
        """, (num_ajuste, tipo, fecha, usuario, notas))
        
        for item in items:
            prod_id = int(item['producto_id'])
            cant_nueva = float(item['cantidad_nueva'])
            
            prod = db_fetchone(conn, "SELECT stock FROM pos_productos WHERE id=?", (prod_id,))
            cant_anterior = float(prod['stock']) if prod else 0.0
            diferencia = round(cant_nueva - cant_anterior, 2)
            
            db_execute(conn, """
                INSERT INTO pos_ajuste_detalles (ajuste_id, producto_id, cantidad_anterior, cantidad_nueva, diferencia)
                VALUES (?,?,?,?,?)
            """, (ajuste_id, prod_id, cant_anterior, cant_nueva, diferencia))
            
            db_execute(conn, "UPDATE pos_productos SET stock = ? WHERE id = ?", (cant_nueva, prod_id))
            
        conn.commit()
        return jsonify({'success': True, 'numero': num_ajuste, 'ajuste_id': ajuste_id})
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


# ── Bodegas y Traslados ───────────────────────────────────────────────────────

@bp.route('/bodegas')
@login_required()
def bodegas():
    conn = get_db()
    lista_bodegas = db_fetchall(conn, "SELECT * FROM pos_bodegas ORDER BY id")
    traslados = db_fetchall(conn, """
        SELECT t.*, o.nombre AS origen_nombre, d.nombre AS destino_nombre 
        FROM pos_traslados t
        JOIN pos_bodegas o ON t.origen_bodega_id = o.id
        JOIN pos_bodegas d ON t.destino_bodega_id = d.id
        ORDER BY t.id DESC LIMIT 50
    """)
    productos = db_fetchall(conn, "SELECT id, nombre, codigo FROM pos_productos WHERE estado='activo' ORDER BY nombre")
    conn.close()
    return render_template('pos_bodegas.html', bodegas=lista_bodegas, traslados=traslados, productos=productos)

@bp.route('/bodegas/guardar', methods=['POST'])
@login_required()
def bodegas_guardar():
    nombre = request.form.get('nombre', '').strip()
    ubicacion = request.form.get('ubicacion', '').strip()
    estado = request.form.get('estado', 'activo')
    bid = request.form.get('id')
    
    if not nombre:
        flash('El nombre de la bodega es obligatorio.', 'danger')
        return redirect(url_for('pos.bodegas'))
        
    conn = get_db()
    try:
        if bid:
            db_execute(conn, "UPDATE pos_bodegas SET nombre=?, ubicacion=?, estado=? WHERE id=?", (nombre, ubicacion, estado, bid))
            flash('Bodega actualizada correctamente.', 'success')
        else:
            new_id = db_insert_and_get_id(conn, "INSERT INTO pos_bodegas (nombre, ubicacion, estado) VALUES (?,?,?)", (nombre, ubicacion, estado))
            # Insertar registro de inventario base 0
            prods = db_fetchall(conn, "SELECT id FROM pos_productos")
            for p in prods:
                db_execute(conn, "INSERT INTO pos_producto_bodegas (producto_id, bodega_id, stock) VALUES (?,?,0)", (p['id'], new_id))
            flash('Bodega creada correctamente.', 'success')
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f'Error al guardar bodega: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('pos.bodegas'))

@bp.route('/traslados/guardar', methods=['POST'])
@login_required()
def traslados_guardar():
    data = request.get_json() or {}
    origen_id = data.get('origen_id')
    destino_id = data.get('destino_id')
    notas = data.get('notas', '').strip()
    items = data.get('items', [])
    usuario = session.get('username', 'sistema')

    from services.pos_service import realizar_traslado
    success, result = realizar_traslado(origen_id, destino_id, notas, items, usuario)
    
    if success:
        return jsonify({'success': True, 'numero': result['numero']})
    else:
        status_code = 400 if 'inválidos' in result['error'] or 'insuficiente' in result['error'] else 500
        return jsonify({'success': False, 'error': result['error']}), status_code

@bp.route('/api/traslados/<int:traslado_id>')
@login_required()
def api_traslado_detalle(traslado_id):
    conn = get_db()
    traslado = db_fetchone(conn, """
        SELECT t.*, o.nombre AS origen_nombre, d.nombre AS destino_nombre
        FROM pos_traslados t
        JOIN pos_bodegas o ON t.origen_bodega_id = o.id
        JOIN pos_bodegas d ON t.destino_bodega_id = d.id
        WHERE t.id = ?
    """, (traslado_id,))
    
    if not traslado:
        conn.close()
        return jsonify({'error': 'Traslado no encontrado'}), 404
        
    items = db_fetchall(conn, """
        SELECT td.cantidad, p.nombre, p.codigo
        FROM pos_traslado_detalles td
        JOIN pos_productos p ON td.producto_id = p.id
        WHERE td.traslado_id = ?
    """, (traslado_id,))
    conn.close()
    
    return jsonify({
        'traslado': dict(traslado),
        'items': [dict(i) for i in items]
    })

@bp.route('/reportes/rentabilidad')
@login_required()
@permission_required('Administrador')
def reporte_rentabilidad():
    fecha_inicio = request.args.get('fecha_inicio', (datetime.now().replace(day=1)).strftime('%Y-%m-%d'))
    fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    
    conn = get_db()
    
    # Reporte de rentabilidad por producto basado en ventas completadas
    query = """
        SELECT 
            p.codigo,
            p.nombre,
            p.categoria_id,
            SUM(vd.cantidad) as total_vendido,
            SUM(vd.subtotal) as ingresos_totales,
            SUM(vd.cantidad * COALESCE(p.costo, 0)) as costo_total
        FROM pos_venta_detalles vd
        JOIN pos_ventas v ON vd.venta_id = v.id
        JOIN pos_productos p ON vd.producto_id = p.id
        WHERE v.estado = 'completada'
          AND date(v.fecha) >= ?
          AND date(v.fecha) <= ?
        GROUP BY p.id
        ORDER BY ingresos_totales DESC
    """
    
    resultados_raw = db_fetchall(conn, query, (fecha_inicio, fecha_fin))
    
    # Procesar resultados para calcular ganancia y margen
    resultados = []
    totales = {'ingresos': 0, 'costos': 0, 'ganancia': 0}
    
    for r in resultados_raw:
        item = dict(r)
        item['ganancia'] = item['ingresos_totales'] - item['costo_total']
        item['margen_porcentaje'] = (item['ganancia'] / item['ingresos_totales'] * 100) if item['ingresos_totales'] > 0 else 0
        
        totales['ingresos'] += item['ingresos_totales']
        totales['costos'] += item['costo_total']
        totales['ganancia'] += item['ganancia']
        
        resultados.append(item)
        
    totales['margen_porcentaje'] = (totales['ganancia'] / totales['ingresos'] * 100) if totales['ingresos'] > 0 else 0
    
    conn.close()
    
    return render_template('pos_reporte_rentabilidad.html', 
                           resultados=resultados, 
                           totales=totales,
                           fecha_inicio=fecha_inicio, 
                           fecha_fin=fecha_fin)

# ── APIs JSON ──────────────────────────────────────────────────────────────────

@bp.route('/api/buscar_socio')
@login_required()
def api_buscar_socio():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    conn = get_db()
    like = f'%{q}%'
    socios = db_fetchall(conn, """
        SELECT s.id, s.codigo, s.nombre, s.apellido, s.nit, s.dpi, s.direccion,
               (SELECT c.id FROM cuentas c WHERE c.socio_id=s.id AND c.tipo='ahorro'
                AND c.producto_ahorro='ahorro_corriente' AND c.estado='activa' LIMIT 1) AS cuenta_id,
               (SELECT c.saldo FROM cuentas c WHERE c.socio_id=s.id AND c.tipo='ahorro'
                AND c.producto_ahorro='ahorro_corriente' AND c.estado='activa' LIMIT 1) AS saldo
        FROM socios s
        WHERE s.estado='activo' AND (s.codigo LIKE ? OR s.nombre LIKE ? OR s.apellido LIKE ?)
        LIMIT 10
    """, (like, like, like))
    conn.close()
    return jsonify([dict(s) for s in socios])


@bp.route('/api/cuentas_socio/<int:socio_id>')
@login_required()
def api_cuentas_socio(socio_id):
    conn = get_db()
    cuentas = db_fetchall(conn, """
        SELECT id, numero, tipo, producto_ahorro, saldo
        FROM cuentas WHERE socio_id=? AND estado='activa' AND saldo > 0
        ORDER BY saldo DESC
    """, (socio_id,))
    conn.close()
    return jsonify([dict(c) for c in cuentas])


@bp.route('/api/buscar_producto_barras')
@login_required()
def api_buscar_producto_barras():
    barras = request.args.get('barras', '').strip()
    if not barras:
        return jsonify(None)
    conn = get_db()
    prod = db_fetchone(conn, """
        SELECT p.*, c.nombre AS categoria_nombre
        FROM pos_productos p
        LEFT JOIN pos_categorias c ON p.categoria_id = c.id
        WHERE p.estado='activo' AND p.codigo_barras = ?
        LIMIT 1
    """, (barras,))
    conn.close()
    return jsonify(dict(prod) if prod else None)


@bp.route('/api/historial_asociado/<int:socio_id>')
@login_required()
def api_historial_asociado(socio_id):
    conn = get_db()
    ventas = db_fetchall(conn, """
        SELECT id, numero, total, fecha, metodo_pago, estado
        FROM pos_ventas WHERE socio_id=?
        ORDER BY id DESC LIMIT 10
    """, (socio_id,))
    conn.close()
    return jsonify([dict(v) for v in ventas])


@bp.route('/api/puntos_socio/<int:socio_id>')
@login_required()
def api_puntos_socio(socio_id):
    conn = get_db()
    puntos_row = db_fetchone(conn, "SELECT puntos_acumulados FROM pos_puntos_fidelidad WHERE socio_id=?", (socio_id,))
    puntos = puntos_row['puntos_acumulados'] if puntos_row else 0
    
    socio_row = db_fetchone(conn, "SELECT saldo_credito_pos, limite_credito_pos FROM socios WHERE id=?", (socio_id,))
    saldo_credito = float(socio_row['saldo_credito_pos']) if socio_row and socio_row['saldo_credito_pos'] is not None else 0.0
    limite_credito = float(socio_row['limite_credito_pos']) if socio_row and socio_row['limite_credito_pos'] is not None else 0.0
    credito_disponible = max(0.0, limite_credito - saldo_credito)
    
    ncs = db_fetchall(conn, "SELECT id, numero, saldo_disponible FROM pos_notas_credito WHERE socio_id=? AND estado='activo' AND saldo_disponible > 0", (socio_id,))
    notas_credito = [dict(nc) for nc in ncs]
    
    # Calcular cuánto se le mandó a cobrar (que aún no han aplicado/pagado en caja)
    cobro_row = db_fetchone(conn, "SELECT SUM(monto) as cobro FROM planilla_masiva_detalles WHERE referencia_tipo='socio_pos' AND referencia_id=? AND estado='pendiente'", (socio_id,))
    cobro_en_planilla = float(cobro_row['cobro'] or 0.0) if cobro_row else 0.0
    
    conn.close()
    return jsonify({
        'puntos': puntos,
        'credito_disponible': credito_disponible,
        'limite_credito': limite_credito,
        'saldo_credito_usado': saldo_credito,
        'cobro_en_planilla': cobro_en_planilla,
        'notas_credito': notas_credito
    })


@bp.route('/api/kardex/<int:producto_id>')
@login_required()
def api_kardex(producto_id):
    conn = get_db()
    movimientos = db_fetchall(conn, """
        SELECT 
            'Venta' AS tipo,
            v.numero AS referencia,
            v.fecha AS fecha,
            -vd.cantidad AS cantidad,
            vd.precio_unitario AS precio_valor,
            v.usuario_creacion AS usuario
        FROM pos_venta_detalles vd
        JOIN pos_ventas v ON vd.venta_id = v.id
        WHERE vd.producto_id = ? AND v.estado = 'completada'

        UNION ALL

        SELECT 
            'Compra' AS tipo,
            c.numero AS referencia,
            c.fecha AS fecha,
            cd.cantidad AS cantidad,
            cd.costo_unitario AS precio_valor,
            c.usuario AS usuario
        FROM pos_compra_detalles cd
        JOIN pos_compras c ON cd.compra_id = c.id
        WHERE cd.producto_id = ? AND c.estado = 'recibido'

        UNION ALL

        SELECT 
            'Devolución' AS tipo,
            dev.numero AS referencia,
            dev.fecha AS fecha,
            dd.cantidad AS cantidad,
            dd.precio_unitario AS precio_valor,
            dev.usuario AS usuario
        FROM pos_devolucion_detalles dd
        JOIN pos_devoluciones dev ON dd.devolucion_id = dev.id
        WHERE dd.producto_id = ?

        UNION ALL

        SELECT 
            CASE WHEN a.tipo = 'ingreso' THEN 'Ajuste de Entrada' ELSE 'Ajuste de Salida' END AS tipo,
            a.numero AS referencia,
            a.fecha AS fecha,
            CASE WHEN a.tipo = 'ingreso' THEN ad.cantidad ELSE -ad.cantidad END AS cantidad,
            p.costo AS precio_valor,
            a.usuario AS usuario
        FROM pos_ajuste_detalles ad
        JOIN pos_ajustes_inventario a ON ad.ajuste_id = a.id
        JOIN pos_productos p ON ad.producto_id = p.id
        WHERE ad.producto_id = ?

        ORDER BY fecha DESC, referencia DESC
    """, (producto_id, producto_id, producto_id, producto_id))
    conn.close()
    return jsonify([dict(m) for m in movimientos])


# ── Reportes ──────────────────────────────────────────────────────────────────

@bp.route('/reportes')
@login_required()
def reportes():
    conn = get_db()
    fecha_desde = request.args.get('fecha_desde', date.today().isoformat())
    fecha_hasta = request.args.get('fecha_hasta', date.today().isoformat())
    
    # Corte de Caja (Ventas por método de pago)
    corte_caja = db_fetchall(conn, """
        SELECT vp.metodo_pago, COUNT(DISTINCT v.id) as num_ventas, SUM(vp.monto) as total
        FROM pos_venta_pagos vp
        JOIN pos_ventas v ON vp.venta_id = v.id
        WHERE date(v.fecha) BETWEEN date(?) AND date(?) AND v.estado = 'completada'
        GROUP BY vp.metodo_pago
    """, (fecha_desde, fecha_hasta))
    
    # Ventas por Categoría
    ventas_categoria = db_fetchall(conn, """
        SELECT c.nombre as categoria, SUM(vd.cantidad) as total_cantidad, SUM(vd.subtotal) as total_monto
        FROM pos_venta_detalles vd
        JOIN pos_ventas v ON vd.venta_id = v.id
        JOIN pos_productos p ON vd.producto_id = p.id
        LEFT JOIN pos_categorias c ON p.categoria_id = c.id
        WHERE date(v.fecha) BETWEEN date(?) AND date(?) AND v.estado = 'completada'
        GROUP BY c.id
        ORDER BY total_monto DESC
    """, (fecha_desde, fecha_hasta))
    
    # Utilidades y Márgenes por Producto
    utilidades_producto = db_fetchall(conn, """
        SELECT 
            p.nombre as producto, 
            c.nombre as categoria,
            SUM(vd.cantidad) as total_cantidad, 
            SUM(vd.subtotal) as ingreso_total,
            SUM(vd.cantidad * COALESCE(p.costo, 0)) as costo_total
        FROM pos_venta_detalles vd
        JOIN pos_ventas v ON vd.venta_id = v.id
        JOIN pos_productos p ON vd.producto_id = p.id
        LEFT JOIN pos_categorias c ON p.categoria_id = c.id
        WHERE date(v.fecha) BETWEEN date(?) AND date(?) AND v.estado = 'completada'
        GROUP BY p.id
        ORDER BY ingreso_total DESC
    """, (fecha_desde, fecha_hasta))
    
    utilidades_procesadas = []
    total_ingresos_utilidad = 0
    total_costos_utilidad = 0
    total_utilidad_neta = 0

    for row in utilidades_producto:
        ingreso = float(row['ingreso_total'] or 0)
        costo = float(row['costo_total'] or 0)
        utilidad = ingreso - costo
        margen = (utilidad / ingreso * 100) if ingreso > 0 else 0
        
        total_ingresos_utilidad += ingreso
        total_costos_utilidad += costo
        total_utilidad_neta += utilidad

        utilidades_procesadas.append({
            'producto': row['producto'],
            'categoria': row['categoria'] or 'Sin Categoría',
            'cantidad': row['total_cantidad'],
            'ingreso': ingreso,
            'costo': costo,
            'utilidad': utilidad,
            'margen': margen
        })

    margen_global = (total_utilidad_neta / total_ingresos_utilidad * 100) if total_ingresos_utilidad > 0 else 0

    # Productos con stock bajo (stock <= stock_minimo, excluyendo servicios)
    bajo_stock = db_fetchall(conn, """
        SELECT p.id, p.codigo, p.nombre, p.stock, p.stock_minimo, c.nombre as categoria
        FROM pos_productos p
        LEFT JOIN pos_categorias c ON p.categoria_id = c.id
        WHERE p.estado = 'activo' AND p.unidad != 'servicio' AND p.stock <= p.stock_minimo
        ORDER BY p.nombre
    """)

    conn.close()
    
    totales_caja = sum(float(r['total'] or 0) for r in corte_caja)
    totales_cat = sum(float(r['total_monto'] or 0) for r in ventas_categoria)

    return render_template('pos_reportes.html', 
        fecha_desde=fecha_desde, 
        fecha_hasta=fecha_hasta,
        corte_caja=corte_caja,
        ventas_categoria=ventas_categoria,
        totales_caja=totales_caja,
        totales_cat=totales_cat,
        utilidades=utilidades_procesadas,
        total_ingresos_utilidad=total_ingresos_utilidad,
        total_costos_utilidad=total_costos_utilidad,
        total_utilidad_neta=total_utilidad_neta,
        margen_global=margen_global,
        bajo_stock=bajo_stock
    )


# ── Planillas Crédito POS ──────────────────────────────────────────────────

@bp.route('/planillas')
@login_required()
def planillas_credito_pos():
    import math as _math
    conn = get_db()
    nombre = request.args.get('nombre', '').strip()
    frecuencia = request.args.get('frecuencia', '').strip()
    estado = request.args.get('estado', '').strip().lower()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()
    page = max(1, int(request.args.get('page', 1) or 1))
    per_page = min(100, max(10, int(request.args.get('per_page', 50) or 50)))

    base_query = "FROM planillas_masivas WHERE tipo = 'pos_creditos'"
    params = []

    if nombre:
        base_query += ' AND nombre LIKE ?'
        params.append(f'%{nombre}%')
    if frecuencia:
        base_query += ' AND frecuencia = ?'
        params.append(frecuencia)
    if estado:
        base_query += ' AND estado = ?'
        params.append(estado)
    if fecha_desde:
        base_query += ' AND date(fecha_pago) >= date(?)'
        params.append(fecha_desde)
    if fecha_hasta:
        base_query += ' AND date(fecha_pago) <= date(?)'
        params.append(fecha_hasta)

    order_sql = '''
        ORDER BY CASE estado
            WHEN 'pendiente' THEN 1
            WHEN 'parcial' THEN 2
            WHEN 'aplicada' THEN 3
            ELSE 4
        END, fecha_creacion DESC, id DESC
    '''

    total_planillas = db_fetchone(conn, f'SELECT COUNT(*) {base_query}', params)[0]
    total_pages = max(1, _math.ceil(total_planillas / per_page))
    offset = (page - 1) * per_page

    planillas = db_fetchall(
        conn,
        f'SELECT * {base_query} {order_sql} LIMIT ? OFFSET ?',
        params + [per_page, offset]
    )
    total_monto_row = db_fetchone(conn, f'SELECT COALESCE(SUM(total_monto),0) {base_query}', params)
    total_monto = float(total_monto_row[0] or 0)
    conn.close()

    return render_template(
        'pos_planillas.html',
        planillas=planillas,
        filtros={
            'nombre': nombre,
            'frecuencia': frecuencia,
            'estado': estado,
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta
        },
        total_planillas=total_planillas,
        total_monto=total_monto,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )

@bp.route('/generar_planilla', methods=['GET', 'POST'])
@login_required()
def generar_planilla_pos():
    form_data = {
        'nombre_planilla': '',
        'fecha_pago': date.today().isoformat(),
        'frecuencia': 'Quincenal',
    }

    if request.method == 'POST':
        nombre_planilla = request.form.get('nombre_planilla', '').strip()
        fecha_pago = request.form.get('fecha_pago', '').strip()
        frecuencia = request.form.get('frecuencia', '').strip()

        form_data = {
            'nombre_planilla': nombre_planilla,
            'fecha_pago': fecha_pago or date.today().isoformat(),
            'frecuencia': frecuencia or 'Quincenal',
        }

        if not nombre_planilla or not fecha_pago or not frecuencia:
            flash('Todos los campos son obligatorios.', 'danger')
            return render_template('pos_generar_planilla.html', form_data=form_data)

        if frecuencia not in ('Quincenal', 'Catorcenal'):
            flash('Frecuencia no valida.', 'danger')
            return render_template('pos_generar_planilla.html', form_data=form_data)

        conn = get_db()
        socios_credito = db_fetchall(
            conn,
            '''
            SELECT id, codigo, nombre, apellido, saldo_credito_pos, cuota_credito_pos, frecuencia_credito_pos
            FROM socios
            WHERE estado = 'activo'
              AND saldo_credito_pos > 0
              AND frecuencia_credito_pos = ?
            ORDER BY apellido, nombre
            ''',
            (frecuencia,)
        )

        if not socios_credito:
            conn.close()
            flash('No se encontraron socios con saldo de crédito POS para esta frecuencia.', 'warning')
            return render_template('pos_generar_planilla.html', form_data=form_data)

        total_planilla = 0
        detalles_a_insertar = []

        for socio in socios_credito:
            saldo = float(socio['saldo_credito_pos'] or 0)
            cuota = float(socio['cuota_credito_pos'] or 0)
            if cuota <= 0: cuota = saldo # Si no configuró cuota, cobra todo el saldo
            monto_programado = min(saldo, cuota)
            if monto_programado > 0:
                total_planilla += monto_programado
                detalles_a_insertar.append({
                    'socio_id': socio['id'],
                    'codigo': socio['codigo'],
                    'nombre': f"{socio['nombre']} {socio['apellido']}",
                    'monto': monto_programado
                })

        if not detalles_a_insertar:
            conn.close()
            flash('No hay cuotas que cobrar.', 'warning')
            return render_template('pos_generar_planilla.html', form_data=form_data)

        planilla_id = db_insert_and_get_id(
            conn,
            '''
            INSERT INTO planillas_masivas
            (tipo, nombre, fecha_pago, frecuencia, estado, total_monto, total_registros, fecha_creacion, usuario_creacion)
            VALUES (?, ?, ?, ?, 'pendiente', ?, ?, ?, ?)
            ''',
            (
                'pos_creditos', nombre_planilla, fecha_pago, frecuencia,
                total_planilla, len(detalles_a_insertar), date.today().isoformat(), session.get('username')
            )
        )

        for d in detalles_a_insertar:
            db_execute(
                conn,
                '''
                INSERT INTO planilla_masiva_detalles
                (planilla_id, referencia_tipo, referencia_id, numero_referencia, socio_codigo, socio_nombre, monto, estado)
                VALUES (?, 'socio_pos', ?, ?, ?, ?, ?, 'pendiente')
                ''',
                (planilla_id, d['socio_id'], d['codigo'], d['codigo'], d['nombre'], d['monto'])
            )
            # Rebajar inmediatamente el saldo del asociado
            db_execute(
                conn,
                "UPDATE socios SET saldo_credito_pos = MAX(0, saldo_credito_pos - ?) WHERE id = ?",
                (d['monto'], d['socio_id'])
            )

        conn.commit()
        conn.close()
        flash('Planilla de crédito POS generada y guardada como pendiente.', 'success')
        return redirect(url_for('pos.detalle_planilla_pos', planilla_id=planilla_id))

    return render_template('pos_generar_planilla.html', form_data=form_data)

@bp.route('/planillas/<int:planilla_id>')
@login_required()
def detalle_planilla_pos(planilla_id):
    conn = get_db()
    planilla = db_fetchone(conn, "SELECT * FROM planillas_masivas WHERE id=? AND tipo='pos_creditos'", (planilla_id,))
    
    if not planilla:
        conn.close()
        flash('Planilla POS no encontrada.', 'danger')
        return redirect(url_for('pos.planillas_credito_pos'))

    detalles = db_fetchall(conn, '''
        SELECT d.*, s.saldo_credito_pos as saldo_actual
        FROM planilla_masiva_detalles d
        LEFT JOIN socios s ON d.referencia_id = s.id
        WHERE d.planilla_id=?
        ORDER BY d.socio_nombre, d.numero_referencia
    ''', (planilla_id,))
    conn.close()

    return render_template(
        'pos_detalle_planilla.html',
        planilla=planilla,
        detalles=detalles,
        nombre_planilla=planilla['nombre'],
        fecha_pago=planilla['fecha_pago'],
        boleta_deposito=planilla['boleta_deposito'],
        frecuencia=planilla['frecuencia']
    )

@bp.route('/procesar_planilla_pos', methods=['POST'])
@login_required()
def procesar_planilla_pos():
    data = request.get_json()
    planilla_id = data.get('planilla_id')
    pagos = data.get('pagos', [])
    boleta_deposito = data.get('boleta_deposito', '').strip()
    
    if not boleta_deposito:
        return jsonify({'error': 'Debe indicar número de boleta o documento de pago para aplicar la planilla.'}), 400

    conn = get_db()
    planilla = db_fetchone(conn, "SELECT * FROM planillas_masivas WHERE id=? AND tipo='pos_creditos'", (planilla_id,))
    
    if not planilla:
        conn.close()
        return jsonify({'error': 'Planilla no encontrada.'}), 404

    if planilla['estado'] == 'aplicada':
        conn.close()
        return jsonify({'error': 'Esta planilla ya fue aplicada.'}), 400

    procesados = 0
    errores = []

    for pago in pagos:
        try:
            socio_id = pago['prestamo_id'] # referenciado como prestamo_id genérico en el frontend
            monto = float(pago['monto'])
            detalle_id = pago.get('detalle_id')

            if monto <= 0: continue

            if detalle_id:
                db_execute(conn, "UPDATE planilla_masiva_detalles SET estado='aplicado', monto=? WHERE id=?", (monto, detalle_id))
            
            procesados += 1
        except Exception as e:
            errores.append(f"Error procesando socio_id {pago.get('prestamo_id')}: {str(e)}")

    pendientes = db_fetchone(
        conn,
        "SELECT COUNT(*) FROM planilla_masiva_detalles WHERE planilla_id=? AND estado='pendiente'",
        (planilla_id,)
    )[0]
    
    estado_final = 'aplicada' if pendientes == 0 and procesados > 0 else ('parcial' if procesados > 0 else 'pendiente')
    
    db_execute(conn, '''
        UPDATE planillas_masivas
        SET estado=?, boleta_deposito=?, fecha_aplicacion=?, usuario_aplicacion=?
        WHERE id=?
    ''', (estado_final, boleta_deposito, date.today().isoformat(), session.get('username'), planilla_id))
    
    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Se procesaron {procesados} descuentos de crédito POS correctamente.',
        'procesados': procesados,
        'total': len(pagos),
        'errores': errores
    })


# ── Estado de Cuenta ──────────────────────────────────────────────────────────

@bp.route('/socio/<int:socio_id>/estado_cuenta/imprimir')
@login_required()
def imprimir_estado_cuenta(socio_id):
    conn = get_db()
    socio = db_fetchone(conn, "SELECT * FROM socios WHERE id=?", (socio_id,))
    if not socio:
        conn.close()
        flash('Socio no encontrado', 'error')
        return redirect(url_for('pos.terminal'))
        
    ventas_credito = db_fetchall(conn, """
        SELECT v.id, v.numero, v.fecha, p.monto 
        FROM pos_ventas v
        JOIN pos_venta_pagos p ON v.id = p.venta_id
        WHERE v.socio_id=? AND p.metodo_pago='credito_interno'
        ORDER BY v.fecha DESC
    """, (socio_id,))
    
    conn.close()
    
    return render_template('pos_estado_cuenta_print.html', socio=socio, ventas=ventas_credito)
