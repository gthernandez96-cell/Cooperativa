"""
tests/test_ahorro.py — Pruebas del módulo de ahorro (cuentas y transacciones).
"""
import pytest
import app as app_module
import utils.db as db_module


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "cooperativa_test.db"
    monkeypatch.setattr(app_module, 'DB', str(db_path))
    monkeypatch.setattr(db_module, 'DB', str(db_path))
    app_module.app.config['TESTING'] = True
    app_module.app.config['WTF_CSRF_ENABLED'] = False
    app_module.init_db()
    with app_module.app.test_client() as c:
        c.post('/login', data={'username': 'admin', 'password': 'admin123'})
        yield c


def test_listado_cuentas_accesible(client):
    resp = client.get('/cuentas')
    assert resp.status_code == 200


def test_nueva_cuenta_get_accesible(client):
    resp = client.get('/cuentas/nueva')
    assert resp.status_code == 200


def test_detalle_cuenta_existente(client):
    conn = app_module.get_db()
    cuenta = conn.execute("SELECT id FROM cuentas LIMIT 1").fetchone()
    conn.close()
    if cuenta:
        resp = client.get(f'/cuentas/{cuenta["id"]}')
        assert resp.status_code == 200


def test_detalle_cuenta_inexistente_redirige(client):
    # Una cuenta inexistente debe redirigir con mensaje de error, no lanzar excepción
    resp = client.get('/cuentas/99999', follow_redirects=True)
    assert resp.status_code == 200
    assert b'Cuenta no encontrada' in resp.data


def test_crear_cuenta_nueva(client):
    conn = app_module.get_db()
    socio = conn.execute("SELECT id FROM socios LIMIT 1").fetchone()
    conn.close()
    assert socio is not None

    resp = client.post('/cuentas/nueva', data={
        'socio_id': socio['id'],
        'tipo': 'ahorro',
        'producto_ahorro': 'ahorro_corriente',
        'tasa_interes': '5.0',
        'fecha_apertura': '2025-01-01',
    }, follow_redirects=True)
    # No debe haber error 500
    assert resp.status_code == 200


def test_deposito_en_cuenta(client):
    conn = app_module.get_db()
    cuenta = conn.execute("SELECT id, saldo FROM cuentas WHERE estado='activa' LIMIT 1").fetchone()
    conn.close()
    if not cuenta:
        pytest.skip("No hay cuentas activas en la BD de prueba")

    saldo_anterior = cuenta['saldo']
    resp = client.post(f'/cuentas/{cuenta["id"]}/transaccion', data={
        'tipo': 'deposito',
        'monto': '500',
        'descripcion': 'Test depósito',
        'fecha': '2025-01-15',
    }, follow_redirects=True)
    assert resp.status_code == 200

    conn = app_module.get_db()
    saldo_nuevo = conn.execute("SELECT saldo FROM cuentas WHERE id=?", (cuenta['id'],)).fetchone()['saldo']
    conn.close()
    assert saldo_nuevo == saldo_anterior + 500


def test_retiro_excede_saldo_muestra_error(client):
    conn = app_module.get_db()
    # Crear cuenta con saldo conocido
    socio = conn.execute("SELECT id FROM socios LIMIT 1").fetchone()
    conn.execute(
        "INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('AHO-TEST-001', socio['id'], 'ahorro', 100.0, 5.0, '2025-01-01', 'activa')
    )
    conn.commit()
    cuenta = conn.execute("SELECT id FROM cuentas WHERE numero='AHO-TEST-001'").fetchone()
    conn.close()

    resp = client.post(f'/cuentas/{cuenta["id"]}/transaccion', data={
        'tipo': 'retiro',
        'monto': '9999',
        'descripcion': 'Retiro excesivo',
        'fecha': '2025-01-15',
    }, follow_redirects=True)
    assert resp.status_code == 200
    # El saldo no debe haber cambiado
    conn = app_module.get_db()
    saldo = conn.execute("SELECT saldo FROM cuentas WHERE id=?", (cuenta['id'],)).fetchone()['saldo']
    conn.close()
    assert saldo == 100.0


def test_reportes_ahorro_accesible(client):
    resp = client.get('/reportes_ahorro')
    assert resp.status_code == 200


def test_gestion_retiro_pagina_accesible(client):
    resp = client.get('/gestiones/retiro')
    assert resp.status_code == 200
    assert b'Nueva solicitud de retiro' in resp.data


def test_crear_solicitud_retiro_exito(client):
    conn = app_module.get_db()
    # Create a mock active socio
    conn.execute(
        "INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado, frecuencia) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('SOC-AHO-102', 'Juan', 'Perez', '2000000000102', '2026-01-01', 'activo', 'Quincenal')
    )
    socio = conn.execute("SELECT id FROM socios WHERE codigo='SOC-AHO-102'").fetchone()

    # Create a cuenta with positive balance
    conn.execute(
        "INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('AHO-TEST-102', socio['id'], 'ahorro', 1500.0, 3.0, '2026-01-01', 'activa')
    )
    cuenta = conn.execute("SELECT id FROM cuentas WHERE numero='AHO-TEST-102'").fetchone()
    conn.commit()
    conn.close()

    resp = client.post('/gestiones/retiro/nuevo', data={
        'cuenta_id': str(cuenta['id']),
        'monto': '500.00',
        'descripcion': 'Retiro de prueba',
        'metodo_retiro': 'cheque',
        'destino': 'retiro'
    }, follow_redirects=True)
    assert resp.status_code == 200

    # Verify database has the pending withdrawal request
    conn = app_module.get_db()
    solicitud = conn.execute("SELECT * FROM solicitudes_retiro WHERE cuenta_id=?", (cuenta['id'],)).fetchone()
    conn.close()
    assert solicitud is not None
    assert solicitud['monto'] == 500.0
    assert solicitud['estado'] == 'pendiente'


def test_crear_solicitud_retiro_saldo_insuficiente(client):
    conn = app_module.get_db()
    # Create a mock active socio
    conn.execute(
        "INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado, frecuencia) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('SOC-AHO-103', 'Pedro', 'Gomez', '2000000000103', '2026-01-01', 'activo', 'Quincenal')
    )
    socio = conn.execute("SELECT id FROM socios WHERE codigo='SOC-AHO-103'").fetchone()

    # Create a cuenta with small balance
    conn.execute(
        "INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('AHO-TEST-103', socio['id'], 'ahorro', 100.0, 3.0, '2026-01-01', 'activa')
    )
    cuenta = conn.execute("SELECT id FROM cuentas WHERE numero='AHO-TEST-103'").fetchone()
    conn.commit()
    conn.close()

    # Attempt to withdraw 1000.0 (exceeds 100.0)
    resp = client.post('/gestiones/retiro/nuevo', data={
        'cuenta_id': str(cuenta['id']),
        'monto': '1000.00',
        'descripcion': 'Retiro excesivo',
        'metodo_retiro': 'cheque',
        'destino': 'retiro'
    }, follow_redirects=True)
    assert resp.status_code == 200
    assert b'excede el saldo disponible' in resp.data

    # Verify no withdrawal request was created in database
    conn = app_module.get_db()
    solicitud = conn.execute("SELECT * FROM solicitudes_retiro WHERE cuenta_id=?", (cuenta['id'],)).fetchone()
    conn.close()
    assert solicitud is None


def test_procesar_abonos_masivos_con_cero(client):
    conn = app_module.get_db()
    # Crear dos socios y cuentas para pruebas
    conn.execute(
        "INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado, frecuencia) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('SOC-AHO-TEST-1', 'Juan', 'Test', '2000000000120', '2026-01-01', 'activo', 'Quincenal')
    )
    socio1 = conn.execute("SELECT id FROM socios WHERE codigo='SOC-AHO-TEST-1'").fetchone()

    conn.execute(
        "INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado, frecuencia) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('SOC-AHO-TEST-2', 'Pedro', 'Test', '2000000000121', '2026-01-01', 'activo', 'Quincenal')
    )
    socio2 = conn.execute("SELECT id FROM socios WHERE codigo='SOC-AHO-TEST-2'").fetchone()

    conn.execute(
        "INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('AHO-TEST-M1', socio1['id'], 'ahorro', 100.0, 3.0, '2026-01-01', 'activa')
    )
    cuenta1 = conn.execute("SELECT id FROM cuentas WHERE numero='AHO-TEST-M1'").fetchone()

    conn.execute(
        "INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('AHO-TEST-M2', socio2['id'], 'ahorro', 100.0, 3.0, '2026-01-01', 'activa')
    )
    cuenta2 = conn.execute("SELECT id FROM cuentas WHERE numero='AHO-TEST-M2'").fetchone()

    # Crear planilla masiva
    conn.execute(
        "INSERT INTO planillas_masivas (tipo, estado, nombre, fecha_pago, frecuencia, total_registros, total_monto, fecha_creacion) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ('ahorro_cuotas', 'pendiente', 'Planilla Ahorro Test', '2026-05-30', 'Quincenal', 2, 100.0, '2026-05-30')
    )
    planilla = conn.execute("SELECT id FROM planillas_masivas WHERE nombre='Planilla Ahorro Test'").fetchone()

    # Crear detalles
    conn.execute(
        "INSERT INTO planilla_masiva_detalles (planilla_id, referencia_tipo, referencia_id, numero_referencia, monto, estado, socio_codigo, socio_nombre) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (planilla['id'], 'cuenta', cuenta1['id'], 'AHO-TEST-M1', 50.0, 'pendiente', 'SOC-AHO-TEST-1', 'Juan Test')
    )
    det1 = conn.execute("SELECT id FROM planilla_masiva_detalles WHERE numero_referencia='AHO-TEST-M1'").fetchone()

    conn.execute(
        "INSERT INTO planilla_masiva_detalles (planilla_id, referencia_tipo, referencia_id, numero_referencia, monto, estado, socio_codigo, socio_nombre) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (planilla['id'], 'cuenta', cuenta2['id'], 'AHO-TEST-M2', 50.0, 'pendiente', 'SOC-AHO-TEST-2', 'Pedro Test')
    )
    det2 = conn.execute("SELECT id FROM planilla_masiva_detalles WHERE numero_referencia='AHO-TEST-M2'").fetchone()

    conn.commit()
    conn.close()

    # Simular la petición con abono de 50.0 para cuenta1 y 0.0 para cuenta2
    resp = client.post('/procesar_abonos_masivos', json={
        'planilla_id': planilla['id'],
        'boleta_deposito': 'BOL-0002',
        'fecha': '2026-05-30',
        'frecuencia': 'Quincenal',
        'abonos': [
            {'cuenta_id': cuenta1['id'], 'monto': 50.0, 'numero': 'AHO-TEST-M1', 'detalle_id': det1['id']},
            {'cuenta_id': cuenta2['id'], 'monto': 0.0, 'numero': 'AHO-TEST-M2', 'detalle_id': det2['id']}
        ]
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data['procesados'] == 2
    assert len(data['errores']) == 0

    conn = app_module.get_db()
    # Cuenta 1 debe aumentar en 50
    bal1 = conn.execute("SELECT saldo FROM cuentas WHERE id=?", (cuenta1['id'],)).fetchone()['saldo']
    # Cuenta 2 debe quedar intacta (100)
    bal2 = conn.execute("SELECT saldo FROM cuentas WHERE id=?", (cuenta2['id'],)).fetchone()['saldo']
    assert bal1 == 150.0
    assert bal2 == 100.0

    # Cuenta 1 debe registrar transaccion de deposito, Cuenta 2 ninguna
    tx1 = conn.execute("SELECT COUNT(*) as cnt FROM transacciones WHERE cuenta_id=?", (cuenta1['id'],)).fetchone()['cnt']
    tx2 = conn.execute("SELECT COUNT(*) as cnt FROM transacciones WHERE cuenta_id=?", (cuenta2['id'],)).fetchone()['cnt']
    assert tx1 == 1
    assert tx2 == 0

    # Ambos detalles deben estar aplicados
    st1 = conn.execute("SELECT estado, monto FROM planilla_masiva_detalles WHERE id=?", (det1['id'],)).fetchone()
    st2 = conn.execute("SELECT estado, monto FROM planilla_masiva_detalles WHERE id=?", (det2['id'],)).fetchone()
    assert st1['estado'] == 'aplicado'
    assert st2['estado'] == 'aplicado'
    # El monto de la segunda debe haberse guardado como 0.0
    assert st2['monto'] == 0.0

    conn.close()


def test_exportar_planilla_ahorro(client):
    conn = app_module.get_db()
    # Create a mock spreadsheet
    conn.execute(
        "INSERT INTO planillas_masivas (tipo, nombre, estado, total_monto, total_registros, fecha_creacion, fecha_pago, frecuencia) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ('ahorro_cuotas', 'Planilla Ahorro Test Export', 'pendiente', 150.00, 2, '2026-05-30', '2026-05-31', 'Quincenal')
    )
    planilla = conn.execute("SELECT id FROM planillas_masivas WHERE nombre='Planilla Ahorro Test Export'").fetchone()

    # Create details
    conn.execute(
        "INSERT INTO planilla_masiva_detalles (planilla_id, referencia_tipo, referencia_id, numero_referencia, monto, estado, socio_codigo, socio_nombre) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (planilla['id'], 'cuenta', 1, 'AHO-EXPORT-1', 100.0, 'pendiente', 'SOC-EXP-1', 'Juan Export')
    )
    conn.commit()
    conn.close()

    resp = client.get(f'/planillas_ahorro/{planilla["id"]}/exportar')
    assert resp.status_code == 200
    assert 'text/csv' in resp.content_type
    assert b'\xef\xbb\xbf' in resp.data  # UTF-8 BOM
    assert b'Juan Export' in resp.data
    assert b'AHO-EXPORT-1' in resp.data
    assert b'SOC-EXP-1' in resp.data


# ── Nuevos tests — cierres de período y paginación ───────────────────────────

from datetime import date as _date


def test_deposito_bloqueado_en_periodo_cerrado(client):
    """No se permite depositar cuando el período está cerrado."""
    conn = app_module.get_db()
    socio = conn.execute("SELECT id FROM socios LIMIT 1").fetchone()
    conn.execute(
        "INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('AHO-CIE-001', socio['id'], 'ahorro', 500.0, 5.0, '2025-01-01', 'activa')
    )
    cuenta_id = conn.execute("SELECT id FROM cuentas WHERE numero='AHO-CIE-001'").fetchone()['id']
    hoy = _date.today().isoformat()
    conn.execute(
        "INSERT INTO cierres_periodo (modulo, fecha_inicio, fecha_fin, estado, observaciones, usuario, fecha_creacion) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ('ahorro', '2000-01-01', '2099-12-31', 'cerrado', 'Bloqueo total de test', 'admin', hoy)
    )
    conn.commit()
    conn.close()

    resp = client.post(
        f'/cuentas/{cuenta_id}/transaccion',
        data={'tipo': 'deposito', 'monto': '100', 'descripcion': 'Bloqueado', 'fecha': hoy},
        follow_redirects=True
    )
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert (
        'período' in html.lower()
        or 'cerrado' in html.lower()
        or 'bloqueado' in html.lower()
        or 'no se puede' in html.lower()
    )


def test_planillas_ahorro_paginacion(client):
    """La lista de planillas debe responder correctamente a los parámetros de paginación."""
    resp_p1 = client.get('/planillas_ahorro_pendientes?page=1&per_page=10')
    assert resp_p1.status_code == 200
    resp_p2 = client.get('/planillas_ahorro_pendientes?page=2&per_page=10')
    assert resp_p2.status_code == 200


def test_planillas_prestamos_paginacion(client):
    """La lista de planillas de préstamos debe responder a los parámetros de paginación."""
    resp = client.get('/planillas_prestamos_pendientes?page=1&per_page=10')
    assert resp.status_code == 200
