"""
tests/test_pos_completo.py — Pruebas para las funcionalidades avanzadas del POS.
"""
import json
import pytest
from datetime import date
from utils.db import get_db, db_execute, db_fetchone

def test_flujo_caja_turno(admin_client):
    # 1. Intentar acceder a terminal sin caja abierta redirige
    resp = admin_client.get('/pos/terminal', follow_redirects=True)
    assert b'Control de apertura' in resp.data

    # 2. Abrir caja registradora
    resp = admin_client.post('/pos/cajas/abrir', data={
        'caja_id': 1,
        'saldo_apertura': 150.00,
        'notas': 'Apertura de prueba'
    }, follow_redirects=True)
    assert b'Caja abierta correctamente' in resp.data

    # 3. Registrar un movimiento de egreso manual
    resp = admin_client.post('/pos/cajas/movimiento', data={
        'tipo': 'salida',
        'monto': 25.00,
        'motivo': 'Compra de café'
    }, follow_redirects=True)
    assert b'Movimiento de salida registrado' in resp.data

    # 4. Cerrar caja registradora
    resp = admin_client.post('/pos/cajas/cerrar', data={
        'saldo_cierre': 125.00,
        'notas': 'Cierre de prueba'
    }, follow_redirects=True)
    assert b'Caja cerrada y arqueo completado' in resp.data


def test_cotizacion_guardar_y_cargar(admin_client):
    # Abrir caja primero
    admin_client.post('/pos/cajas/abrir', data={'caja_id': 1, 'saldo_apertura': 100.0})

    # Guardar una cotización
    payload = {
        'items': [{'id': 1, 'nombre': 'Cuaderno', 'precio': 18.00, 'cantidad': 3}],
        'socio_id': None,
        'cliente_nombre': 'Gustavo Hernandez',
        'descuento': 0.0,
        'notas': 'Proforma de cuadernos'
    }
    resp = admin_client.post('/pos/cotizaciones/guardar', 
                             data=json.dumps(payload),
                             content_type='application/json')
    data = json.loads(resp.data)
    assert data['success'] is True
    assert 'COT-' in data['numero']
    cot_id = data['cotizacion_id']

    # Cargar la cotización
    resp_load = admin_client.get(f'/pos/cotizaciones/{cot_id}/cargar')
    data_load = json.loads(resp_load.data)
    assert data_load['success'] is True
    assert data_load['cotizacion']['cliente_nombre'] == 'Gustavo Hernandez'
    assert len(data_load['items']) == 1
    assert data_load['items'][0]['nombre'] == 'Cuaderno universitario' # cargado de la BD por ID


def test_venta_pagos_mixtos_y_fidelidad(admin_client):
    # Abrir caja primero
    admin_client.post('/pos/cajas/abrir', data={'caja_id': 1, 'saldo_apertura': 100.0})

    # Busquemos un socio activo
    conn = get_db()
    socio = db_fetchone(conn, "SELECT * FROM socios WHERE estado='activo' LIMIT 1")
    cuenta = db_fetchone(conn, "SELECT * FROM cuentas WHERE socio_id=? AND estado='activa' LIMIT 1", (socio['id'],))
    
    # Asegurar saldo y puntos
    db_execute(conn, "UPDATE cuentas SET saldo=500.00 WHERE id=?", (cuenta['id'],))
    db_execute(conn, "INSERT OR IGNORE INTO pos_puntos_fidelidad (socio_id, puntos_acumulados, puntos_canjeados, fecha_actualizacion) VALUES (?, 100, 0, ?)", (socio['id'], date.today().isoformat()))
    conn.commit()
    conn.close()

    # Ejecutar una venta con pago mixto: Q30 efectivo, Q24 débito a ahorro (usando 60 puntos = Q6.00 de descuento)
    # Total original: 3 cuadernos * Q18.00 = Q54.00.
    # Descuento puntos (60 pts * 0.10) = Q6.00.
    # Total a cobrar = Q48.00.
    # Pagos: Efectivo Q20.00 + Ahorros Q28.00.
    payload = {
        'items': [{'id': 1, 'nombre': 'Cuaderno', 'precio': 18.00, 'cantidad': 3}],
        'socio_id': socio['id'],
        'cliente_nombre': f"{socio['nombre']} {socio['apellido']}",
        'descuento': 0.0,
        'puntos_canjear': 60,
        'pagos': [
            {'metodo': 'efectivo', 'monto': 20.00},
            {'metodo': 'debito_ahorro', 'monto': 28.00, 'cuenta_id': cuenta['id']}
        ]
    }
    
    resp = admin_client.post('/pos/guardar_venta',
                             data=json.dumps(payload),
                             content_type='application/json')
    data = json.loads(resp.data)
    assert data['success'] is True
    assert data['total'] == 48.00
    venta_id = data['venta_id']

    # Verificar saldos
    conn = get_db()
    cta_despues = db_fetchone(conn, "SELECT saldo FROM cuentas WHERE id=?", (cuenta['id'],))
    assert float(cta_despues['saldo']) == 500.00 - 28.00

    pts_despues = db_fetchone(conn, "SELECT puntos_acumulados FROM pos_puntos_fidelidad WHERE socio_id=?", (socio['id'],))
    # Puntos originales: 100.
    # Redimidos: 60 (quedan 40).
    # Acumulados por venta de Q48.00: 4 puntos (1 punto por cada Q10.00).
    # Total esperado: 40 + 4 = 44 puntos.
    assert pts_despues['puntos_acumulados'] == 44
    conn.close()

    # Registrar una devolución de 1 cuaderno (reembolsa Q18.00)
    dev_payload = {
        'items': [{'producto_id': 1, 'cantidad': 1}],
        'motivo': 'Defectuoso'
    }
    resp_dev = admin_client.post(f'/pos/ventas/{venta_id}/devolver',
                                 data=json.dumps(dev_payload),
                                 content_type='application/json')
    data_dev = json.loads(resp_dev.data)
    assert data_dev['success'] is True
    assert data_dev['total'] == 18.00
    
    # Comprobar nota de crédito emitida
    conn = get_db()
    nc = db_fetchone(conn, "SELECT * FROM pos_notas_credito WHERE socio_id=? ORDER BY id DESC LIMIT 1", (socio['id'],))
    assert nc is not None
    assert nc['saldo_disponible'] == 18.00

    conn.close()
