"""
tests/test_prestamos.py — Pruebas de cálculos financieros de préstamos.
"""
import pytest
import app as app_module
from app import (
    calcular_resumen_prestamo,
    calcular_total_cuotas_prestamo,
    obtener_dias_frecuencia,
    generar_calendario_prestamo,
)


# ── Cálculos financieros (sin BD) ─────────────────────────────────────────────

class TestObtenerDiasFrecuencia:
    def test_catorcenal_retorna_14(self):
        assert obtener_dias_frecuencia('Catorcenal') == 14

    def test_quincenal_retorna_15(self):
        assert obtener_dias_frecuencia('Quincenal') == 15

    def test_valor_invalido_retorna_15(self):
        assert obtener_dias_frecuencia('Mensual') == 15

    def test_none_retorna_15(self):
        assert obtener_dias_frecuencia(None) == 15


class TestCalcularTotalCuotas:
    def test_12_meses_quincenal(self):
        # 12 meses * 30 días / 15 = 24 cuotas
        assert calcular_total_cuotas_prestamo(12, 'Quincenal') == 24

    def test_12_meses_catorcenal(self):
        # 12 * 30 / 14 = 25.71 → ceil = 26
        assert calcular_total_cuotas_prestamo(12, 'Catorcenal') == 26

    def test_plazo_0_retorna_0(self):
        assert calcular_total_cuotas_prestamo(0, 'Quincenal') == 0

    def test_plazo_negativo_retorna_0(self):
        assert calcular_total_cuotas_prestamo(-1, 'Quincenal') == 0


class TestCalcularResumenPrestamo:
    def test_monto_cero_retorna_cuota_cero(self):
        result = calcular_resumen_prestamo(0, 18, 12, 'Quincenal')
        assert result['cuota'] == 0.0
        assert result['total'] == 0.0

    def test_sin_interes_cuota_igual_capital_dividido_cuotas(self):
        result = calcular_resumen_prestamo(12000, 0, 12, 'Quincenal')
        # 24 cuotas sin interés = 500 cada una
        assert result['cuota'] == 500.0
        assert result['intereses'] == 0.0

    def test_con_interes_cuota_mayor_que_sin_interes(self):
        sin_interes = calcular_resumen_prestamo(10000, 0, 12, 'Quincenal')
        con_interes = calcular_resumen_prestamo(10000, 18, 12, 'Quincenal')
        assert con_interes['cuota'] > sin_interes['cuota']

    def test_resultado_incluye_frecuencia(self):
        result = calcular_resumen_prestamo(10000, 18, 12, 'Catorcenal')
        assert result['frecuencia'] == 'Catorcenal'
        assert result['dias_frecuencia'] == 14


class TestGenerarCalendarioPrestamo:
    def test_calendario_tiene_cuotas_correctas(self):
        cal = generar_calendario_prestamo('2025-01-15', 6, 500, 'Quincenal')
        assert len(cal) == 6
        assert cal[0]['numero_cuota'] == 1
        assert cal[0]['fecha_programada'] == '2025-01-15'
        assert cal[0]['monto_programado'] == 500.0

    def test_fechas_separadas_15_dias(self):
        cal = generar_calendario_prestamo('2025-01-01', 3, 1000, 'Quincenal')
        from datetime import date
        fecha1 = date.fromisoformat(cal[0]['fecha_programada'])
        fecha2 = date.fromisoformat(cal[1]['fecha_programada'])
        assert (fecha2 - fecha1).days == 15

    def test_fechas_catorcenal_separadas_14_dias(self):
        cal = generar_calendario_prestamo('2025-01-01', 3, 1000, 'Catorcenal')
        from datetime import date
        fecha1 = date.fromisoformat(cal[0]['fecha_programada'])
        fecha2 = date.fromisoformat(cal[1]['fecha_programada'])
        assert (fecha2 - fecha1).days == 14

    def test_calendario_vacio_con_cero_cuotas(self):
        cal = generar_calendario_prestamo('2025-01-01', 0, 500, 'Quincenal')
        assert cal == []


# ── Rutas de préstamos (con BD) ───────────────────────────────────────────────

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


def test_lista_prestamos_accesible(client):
    resp = client.get('/prestamos')
    assert resp.status_code == 200


def test_api_cuota_retorna_json(client):
    resp = client.get('/api/cuota?monto=10000&tasa=18&plazo=12&frecuencia=Quincenal')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'cuota' in data
    assert data['cuota'] > 0


def test_nuevo_prestamo_get_accesible(client):
    resp = client.get('/prestamos/nuevo')
    assert resp.status_code == 200


def test_detalle_prestamo_muestra_amortizacion_con_fecha_y_prestamo_pagador(client):
    conn = app_module.get_db()
    try:
        conn.execute(
            '''
            INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado, frecuencia)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            ('SOC-TEST-001', 'Ana', 'Lopez', '1000000000101', '2026-01-01', 'activo', 'Quincenal')
        )
        socio_id = conn.execute("SELECT id FROM socios WHERE codigo='SOC-TEST-001'").fetchone()['id']

        conn.execute(
            '''
            INSERT INTO prestamos (
                numero, socio_id, monto_solicitado, monto_aprobado, tasa_interes,
                plazo_meses, cuota_mensual, saldo_pendiente, fecha_solicitud,
                fecha_aprobacion, estado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('PRE-VIG-001', socio_id, 3000, 3000, 18, 12, 150, 1200, '2026-02-01', '2026-02-05', 'aprobado')
        )
        prestamo_vigente_id = conn.execute("SELECT id FROM prestamos WHERE numero='PRE-VIG-001'").fetchone()['id']

        conn.execute(
            '''
            INSERT INTO prestamos (
                numero, socio_id, monto_solicitado, monto_aprobado, tasa_interes,
                plazo_meses, cuota_mensual, saldo_pendiente, fecha_solicitud,
                fecha_aprobacion, estado, refinanciado_de, monto_amortizado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('PRE-PAG-001', socio_id, 5000, 5000, 18, 24, 260, 4300, '2026-03-30', '2026-04-04', 'aprobado', prestamo_vigente_id, 1200)
        )
        conn.commit()
    finally:
        conn.close()

    resp = client.get(f'/prestamos/{prestamo_vigente_id}')
    html = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert 'Pagado con préstamo PRE-PAG-001' in html
    assert '2026-04-04' in html


def test_solicitud_retiro_amortiza_prestamo_vigente_al_aprobar(client):
    conn = app_module.get_db()
    try:
        conn.execute(
            '''
            INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado, frecuencia)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            ('SOC-RET-001', 'Luis', 'Perez', '1000000000999', '2026-01-01', 'activo', 'Quincenal')
        )
        socio_id = conn.execute("SELECT id FROM socios WHERE codigo='SOC-RET-001'").fetchone()['id']

        conn.execute(
            '''
            INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            ('CTA-RET-001', socio_id, 'ahorro', 1500, 3.5, '2026-01-01', 'activa')
        )
        cuenta_id = conn.execute("SELECT id FROM cuentas WHERE numero='CTA-RET-001'").fetchone()['id']

        conn.execute(
            '''
            INSERT INTO prestamos (
                numero, socio_id, monto_solicitado, monto_aprobado, tasa_interes,
                plazo_meses, cuota_mensual, saldo_pendiente, fecha_solicitud,
                fecha_aprobacion, estado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('PRE-RET-001', socio_id, 2000, 2000, 18, 12, 180, 900, '2026-02-01', '2026-02-03', 'aprobado')
        )
        prestamo_id = conn.execute("SELECT id FROM prestamos WHERE numero='PRE-RET-001'").fetchone()['id']
        conn.commit()
    finally:
        conn.close()

    resp_crear = client.post(
        '/gestiones/retiro/nuevo',
        data={
            'cuenta_id': str(cuenta_id),
            'monto': '300',
            'descripcion': 'Amortizacion desde ahorro',
            'metodo_retiro': 'cheque',
            'destino': 'amortizacion_prestamo',
            'prestamo_id': str(prestamo_id),
        }
    )
    assert resp_crear.status_code == 302

    conn = app_module.get_db()
    try:
        solicitud = conn.execute(
            "SELECT id, numero, estado FROM solicitudes_retiro ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert solicitud is not None
        assert solicitud['estado'] == 'pendiente'
        rid = solicitud['id']
        numero_solicitud = solicitud['numero']
    finally:
        conn.close()

    resp_aprobar = client.post(f'/gestiones/retiro/{rid}/aprobar')
    assert resp_aprobar.status_code == 302

    conn = app_module.get_db()
    try:
        prestamo = conn.execute("SELECT saldo_pendiente, estado FROM prestamos WHERE id=?", [prestamo_id]).fetchone()
        assert prestamo is not None
        assert float(prestamo['saldo_pendiente']) == pytest.approx(600.0)
        assert prestamo['estado'] == 'aprobado'

        pago = conn.execute(
            '''
            SELECT monto, capital, interes, boleta_deposito, descripcion
            FROM pagos_prestamo
            WHERE prestamo_id=?
            ORDER BY id DESC LIMIT 1
            ''',
            [prestamo_id]
        ).fetchone()
        assert pago is not None
        assert float(pago['monto']) == pytest.approx(300.0)
        assert float(pago['capital']) == pytest.approx(300.0)
        assert float(pago['interes']) == pytest.approx(0.0)
        assert pago['boleta_deposito'] == numero_solicitud
        assert 'Amortización desde solicitud de retiro' in (pago['descripcion'] or '')

        cuenta = conn.execute("SELECT saldo FROM cuentas WHERE id=?", [cuenta_id]).fetchone()
        assert cuenta is not None
        assert float(cuenta['saldo']) == pytest.approx(1200.0)
    finally:
        conn.close()


def test_gestiones_muestra_indicador_visual_para_retiro_amortizacion(client):
    conn = app_module.get_db()
    try:
        conn.execute(
            '''
            INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado, frecuencia)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            ('SOC-RET-002', 'Marta', 'Soto', '1000000000888', '2026-01-01', 'activo', 'Quincenal')
        )
        socio_id = conn.execute("SELECT id FROM socios WHERE codigo='SOC-RET-002'").fetchone()['id']

        conn.execute(
            '''
            INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            ('CTA-RET-002', socio_id, 'ahorro', 2000, 3.5, '2026-01-01', 'activa')
        )
        cuenta_id = conn.execute("SELECT id FROM cuentas WHERE numero='CTA-RET-002'").fetchone()['id']

        conn.execute(
            '''
            INSERT INTO prestamos (
                numero, socio_id, monto_solicitado, monto_aprobado, tasa_interes,
                plazo_meses, cuota_mensual, saldo_pendiente, fecha_solicitud,
                fecha_aprobacion, estado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('PRE-RET-002', socio_id, 2500, 2500, 18, 12, 210, 1000, '2026-02-10', '2026-02-12', 'aprobado')
        )
        prestamo_id = conn.execute("SELECT id FROM prestamos WHERE numero='PRE-RET-002'").fetchone()['id']
        conn.commit()
    finally:
        conn.close()

    resp_crear = client.post(
        '/gestiones/retiro/nuevo',
        data={
            'cuenta_id': str(cuenta_id),
            'monto': '250',
            'descripcion': 'Solicitud para amortizacion',
            'metodo_retiro': 'cheque',
            'destino': 'amortizacion_prestamo',
            'prestamo_id': str(prestamo_id),
        }
    )
    assert resp_crear.status_code == 302

    resp_crear_normal = client.post(
        '/gestiones/retiro/nuevo',
        data={
            'cuenta_id': str(cuenta_id),
            'monto': '100',
            'descripcion': 'Retiro normal prueba',
            'metodo_retiro': 'cheque',
            'destino': 'retiro',
        }
    )
    assert resp_crear_normal.status_code == 302

    resp_lista = client.get('/gestiones?tipo=retiro&estado=pendiente')
    html = resp_lista.get_data(as_text=True)

    assert resp_lista.status_code == 200
    assert 'Amortización · PRE-RET-002' in html
    assert 'Amortización de préstamo' in html

    resp_filtrado = client.get('/gestiones?tipo=retiro&estado=pendiente&destino=amortizacion')
    html_filtrado = resp_filtrado.get_data(as_text=True)

    assert resp_filtrado.status_code == 200
    assert 'Amortización · PRE-RET-002' in html_filtrado

    resp_filtrado_retiro = client.get('/gestiones?tipo=retiro&estado=pendiente&destino=retiro')
    html_filtrado_retiro = resp_filtrado_retiro.get_data(as_text=True)

    assert resp_filtrado_retiro.status_code == 200
    assert 'Amortización · PRE-RET-002' not in html_filtrado_retiro


def test_rechaza_amortizacion_si_ahorro_es_menor_que_saldo_prestamo(client):
    conn = app_module.get_db()
    try:
        conn.execute(
            '''
            INSERT INTO socios (codigo, nombre, apellido, dpi, fecha_ingreso, estado, frecuencia)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            ('SOC-RET-003', 'Carlos', 'Mena', '1000000000777', '2026-01-01', 'activo', 'Quincenal')
        )
        socio_id = conn.execute("SELECT id FROM socios WHERE codigo='SOC-RET-003'").fetchone()['id']

        conn.execute(
            '''
            INSERT INTO cuentas (numero, socio_id, tipo, saldo, tasa_interes, fecha_apertura, estado)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            ('CTA-RET-003', socio_id, 'ahorro', 600, 3.5, '2026-01-01', 'activa')
        )
        cuenta_id = conn.execute("SELECT id FROM cuentas WHERE numero='CTA-RET-003'").fetchone()['id']

        conn.execute(
            '''
            INSERT INTO prestamos (
                numero, socio_id, monto_solicitado, monto_aprobado, tasa_interes,
                plazo_meses, cuota_mensual, saldo_pendiente, fecha_solicitud,
                fecha_aprobacion, estado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            ('PRE-RET-003', socio_id, 2500, 2500, 18, 12, 210, 900, '2026-02-10', '2026-02-12', 'aprobado')
        )
        prestamo_id = conn.execute("SELECT id FROM prestamos WHERE numero='PRE-RET-003'").fetchone()['id']
        conn.commit()
    finally:
        conn.close()

    resp = client.post(
        '/gestiones/retiro/nuevo',
        data={
            'cuenta_id': str(cuenta_id),
            'monto': '300',
            'descripcion': 'Intento amortizacion con ahorro insuficiente',
            'metodo_retiro': 'cheque',
            'destino': 'amortizacion_prestamo',
            'prestamo_id': str(prestamo_id),
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'debe ser mayor o igual al saldo pendiente del préstamo a amortizar' in html

    conn = app_module.get_db()
    try:
        total = conn.execute("SELECT COUNT(*) AS total FROM solicitudes_retiro").fetchone()['total']
        assert int(total or 0) == 0
    finally:
        conn.close()
