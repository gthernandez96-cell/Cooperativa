"""
tests/test_ahorro.py — Pruebas del módulo de ahorro (cuentas y transacciones).
"""
import pytest
import app as app_module


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "cooperativa_test.db"
    monkeypatch.setattr(app_module, 'DB', str(db_path))
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
        'tasa_interes': '3.5',
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
        ('AHO-TEST-001', socio['id'], 'ahorro', 100.0, 3.5, '2025-01-01', 'activa')
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
