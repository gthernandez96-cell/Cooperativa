import pytest
import app as app_module
from datetime import date, datetime

@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "cooperativa_test.db"
    monkeypatch.setattr(app_module, 'DB', str(db_path))
    from utils import db as db_module
    monkeypatch.setattr(db_module, 'DB', str(db_path))
    app_module.app.config['TESTING'] = True
    app_module.app.config['WTF_CSRF_ENABLED'] = False  # Desactivar CSRF en tests
    app_module.init_db()

    with app_module.app.test_client() as client:
        yield client

def login_as_admin(client):
    return client.post('/login', data={'username': 'admin', 'password': 'admin123'}, follow_redirects=True)

def test_movimientos_diarios_y_jornalizacion(client):
    login_as_admin(client)

    conn = app_module.get_db()
    hoy = date.today().isoformat()

    # 1. Crear datos de prueba (Socio, Cuenta, Préstamo, Transacciones, Pagos)
    # Crear socio
    conn.execute(
        "INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado) VALUES (?, ?, ?, ?, ?, ?)",
        ('SOC-TEST-MOV', 'Juan', 'Perez', '7777777777777', hoy, 'activo')
    )
    socio_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Crear cuenta
    conn.execute(
        "INSERT INTO cuentas (numero, socio_id, tipo, producto_ahorro, saldo, tasa_interes, fecha_apertura, estado) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ('COR-TEST-MOV', socio_id, 'ahorro', 'ahorro_corriente', 1000.0, 5.0, hoy, 'activa')
    )
    cuenta_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Registrar transacción de ahorro (retiro)
    conn.execute(
        "INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha, metodo_pago, jornalizado) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (cuenta_id, 'retiro', 200.0, 800.0, 'Retiro de prueba', datetime.now().isoformat(), 'cheque', 0)
    )
    txn_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Registrar transacción de depósito de ahorro (debe ser ignorada/excluida)
    conn.execute(
        "INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha, metodo_pago, jornalizado) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (cuenta_id, 'deposito', 150.0, 950.0, 'Deposito excluido', datetime.now().isoformat(), 'deposito', 0)
    )
    dep_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Registrar interés generado
    conn.execute(
        "INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha, metodo_pago, jornalizado) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (cuenta_id, 'interes', 10.0, 960.0, 'Interes de prueba', datetime.now().isoformat(), 'deposito', 0)
    )
    int_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Registrar IPF retenido
    conn.execute(
        "INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha, metodo_pago, jornalizado) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (cuenta_id, 'ipf', 1.0, 959.0, 'IPF de prueba', datetime.now().isoformat(), 'debito', 0)
    )
    ipf_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Registrar préstamo aprobado hoy
    conn.execute(
        "INSERT INTO prestamos (numero, socio_id, monto_solicitado, monto_aprobado, tasa_interes, plazo_meses, fecha_solicitud, fecha_aprobacion, estado, desembolso_tipo, jornalizado) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ('PRE-TEST-MOV', socio_id, 5000.0, 5000.0, 18.0, 12, hoy, hoy, 'aprobado', 'deposito', 0)
    )
    prestamo_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Registrar pago de préstamo (amortización)
    conn.execute(
        "INSERT INTO pagos_prestamo (prestamo_id, monto, capital, interes, saldo_restante, fecha, numero_comprobante, boleta_deposito, descripcion, jornalizado, metodo_pago) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (prestamo_id, 500.0, 450.0, 50.0, 4550.0, hoy, 'REC-999999', 'BOL-PAG-TEST', 'Pago de prueba', 0, 'deposito')
    )
    pago_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.commit()
    conn.close()

    # 2. Verificar que los movimientos aparezcan en la vista diarios
    response = client.get(f'/movimientos/diarios?fecha_desde={hoy}&fecha_hasta={hoy}')
    assert response.status_code == 200
    assert b'SOC-TEST-MOV' in response.data
    assert b'COR-TEST-MOV' in response.data
    assert b'PRE-TEST-MOV' in response.data
    assert b'BOL-PAG-TEST' in response.data
    
    # Verificar que el interés y el IPF aparezcan
    assert b'Interes de prueba' in response.data
    assert b'IPF de prueba' in response.data
    
    # Verificar que el depósito de ahorro esté EXCLUIDO
    assert b'Deposito excluido' not in response.data

    # Test filtrar por intereses
    response_int = client.get(f'/movimientos/diarios?fecha_desde={hoy}&fecha_hasta={hoy}&tipo=intereses')
    assert b'Interes de prueba' in response_int.data
    assert b'IPF de prueba' not in response_int.data
    assert b'PRE-TEST-MOV' not in response_int.data

    # Test filtrar por IPF
    response_isr = client.get(f'/movimientos/diarios?fecha_desde={hoy}&fecha_hasta={hoy}&tipo=ipf')
    assert b'IPF de prueba' in response_isr.data
    assert b'Interes de prueba' not in response_isr.data

    # Test filtrar por retiros
    response_retiros = client.get(f'/movimientos/diarios?fecha_desde={hoy}&fecha_hasta={hoy}&tipo=retiros')
    assert b'COR-TEST-MOV' in response_retiros.data
    assert b'PRE-TEST-MOV' not in response_retiros.data

    # Test filtrar por cheques
    response_cheques = client.get(f'/movimientos/diarios?fecha_desde={hoy}&fecha_hasta={hoy}&tipo=cheques')
    assert b'COR-TEST-MOV' in response_cheques.data # El retiro de ahorro se hizo con cheque
    assert b'PRE-TEST-MOV' not in response_cheques.data # El prestamo desembolsado fue deposito

    # 3. Jornalizar los movimientos vía POST
    jornalizar_payload = {
        'items': [
            {'id': txn_id, 'tipo_origen': 'transaccion'},
            {'id': int_id, 'tipo_origen': 'transaccion'},
            {'id': ipf_id, 'tipo_origen': 'transaccion'},
            {'id': prestamo_id, 'tipo_origen': 'prestamo'},
            {'id': pago_id, 'tipo_origen': 'pago_prestamo'}
        ],
        'fecha_jornalizacion': hoy,
        'boleta_jornalizacion': 'BOL-JORNAL-999'
    }

    resp_jornalizar = client.post('/movimientos/jornalizar', json=jornalizar_payload)
    assert resp_jornalizar.status_code == 200
    data_json = resp_jornalizar.get_json()
    assert data_json['success'] is True

    # 4. Validar en base de datos que quedaron jornalizados
    conn = app_module.get_db()
    txn = conn.execute("SELECT jornalizado, fecha_jornalizado, boleta_jornalizado FROM transacciones WHERE id=?", (txn_id,)).fetchone()
    txn_int = conn.execute("SELECT jornalizado, fecha_jornalizado, boleta_jornalizado FROM transacciones WHERE id=?", (int_id,)).fetchone()
    txn_isr = conn.execute("SELECT jornalizado, fecha_jornalizado, boleta_jornalizado FROM transacciones WHERE id=?", (ipf_id,)).fetchone()
    loan = conn.execute("SELECT jornalizado, fecha_jornalizado, boleta_jornalizado FROM prestamos WHERE id=?", (prestamo_id,)).fetchone()
    pago = conn.execute("SELECT jornalizado, fecha_jornalizado, boleta_jornalizado FROM pagos_prestamo WHERE id=?", (pago_id,)).fetchone()
    conn.close()

    assert txn['jornalizado'] == 1
    assert txn['fecha_jornalizado'] == hoy
    assert txn['boleta_jornalizado'] == 'BOL-JORNAL-999'

    assert txn_int['jornalizado'] == 1
    assert txn_int['fecha_jornalizado'] == hoy
    assert txn_int['boleta_jornalizado'] == 'BOL-JORNAL-999'

    assert txn_isr['jornalizado'] == 1
    assert txn_isr['fecha_jornalizado'] == hoy
    assert txn_isr['boleta_jornalizado'] == 'BOL-JORNAL-999'

    assert loan['jornalizado'] == 1
    assert loan['fecha_jornalizado'] == hoy
    assert loan['boleta_jornalizado'] == 'BOL-JORNAL-999'

    assert pago['jornalizado'] == 1
    assert pago['fecha_jornalizado'] == hoy
    assert pago['boleta_jornalizado'] == 'BOL-JORNAL-999'
