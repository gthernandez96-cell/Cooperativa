import pytest
import app as app_module
from datetime import date

@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "cooperativa_test.db"
    monkeypatch.setattr(app_module, 'DB', str(db_path))
    from utils import db as db_module
    monkeypatch.setattr(db_module, 'DB', str(db_path))
    app_module.app.config['TESTING'] = True
    app_module.app.config['WTF_CSRF_ENABLED'] = False
    app_module.init_db()
    with app_module.app.test_client() as client:
        yield client

def login_as_admin(client):
    return client.post('/login', data={'username': 'admin', 'password': 'admin123'}, follow_redirects=True)

def test_reportes_dashboard_y_vistas(client):
    login_as_admin(client)

    # 1. Dashboard de reportes
    resp = client.get('/reportes/')
    assert resp.status_code == 200
    assert b'Reporter' in resp.data

    # 2. Reporte de Cartera
    resp_cartera = client.get('/reportes/cartera')
    assert resp_cartera.status_code == 200
    html = resp_cartera.data.decode('utf-8').lower()
    assert 'cartera' in html
    assert 'dpi' in html
    assert 'saldo' in html
    assert 'vista previa' in html

    # CSV Cartera
    resp_csv = client.get('/reportes/cartera/exportar')
    assert resp_csv.status_code == 200
    assert resp_csv.mimetype == 'text/csv'
    csv = resp_csv.data.decode('utf-8').lower()
    assert 'dpi' in csv
    assert 'cuota mensual' in csv

    # 3. Reporte de Morosidad
    resp_mora = client.get('/reportes/morosidad')
    assert resp_mora.status_code == 200
    html = resp_mora.data.decode('utf-8').lower()
    assert 'morosidad' in html or 'antig' in html
    assert 'dpi' in html
    assert 'vista previa' in html

    # CSV Morosidad
    resp_csv = client.get('/reportes/morosidad/exportar')
    assert resp_csv.status_code == 200
    assert resp_csv.mimetype == 'text/csv'
    csv = resp_csv.data.decode('utf-8').lower()
    assert 'dpi' in csv
    assert 'mora' in csv or 'cobranza' in csv

    # 4. Reporte de Captaciones
    resp_cap = client.get('/reportes/captaciones')
    assert resp_cap.status_code == 200
    html = resp_cap.data.decode('utf-8').lower()
    assert 'ipf' in html or 'ahorros' in html
    assert 'dpi' in html
    assert 'vista previa' in html

    # CSV Captaciones
    resp_csv = client.get('/reportes/captaciones/exportar')
    assert resp_csv.status_code == 200
    assert resp_csv.mimetype == 'text/csv'
    csv = resp_csv.data.decode('utf-8').lower()
    assert 'dpi' in csv
    assert 'ipf' in csv

    # 5. Reporte de Planillas
    resp_plan = client.get('/reportes/planillas')
    assert resp_plan.status_code == 200
    html = resp_plan.data.decode('utf-8').lower()
    assert 'planilla' in html
    assert 'vista previa' in html

    # CSV Planillas
    resp_csv = client.get('/reportes/planillas/exportar')
    assert resp_csv.status_code == 200
    assert resp_csv.mimetype == 'text/csv'
    csv = resp_csv.data.decode('utf-8').lower()
    assert 'planilla' in csv or 'monto' in csv

    # 6. Reporte de Auditoría
    resp_aud = client.get('/reportes/auditoria')
    assert resp_aud.status_code == 200
    html = resp_aud.data.decode('utf-8').lower()
    assert 'bit' in html  # bitácora
    assert 'usuario' in html
    assert 'vista previa' in html

    # CSV Auditoría
    resp_csv = client.get('/reportes/auditoria/exportar')
    assert resp_csv.status_code == 200
    assert resp_csv.mimetype == 'text/csv'
    csv_data = resp_csv.data.decode('utf-8').lower()
    assert 'usuario' in csv_data or 'modulo' in csv_data or 'módulo' in csv_data

    # 7. Reporte de Asociados (Nuevo)
    resp_socios = client.get('/reportes/socios')
    assert resp_socios.status_code == 200
    html = resp_socios.data.decode('utf-8').lower()
    assert 'padron' in html or 'padrón' in html or 'asociado' in html
    assert 'dpi' in html
    assert 'saldo ahorro' in html or 'saldo' in html
    assert 'vista previa' in html

    # CSV Asociados
    resp_csv_socios = client.get('/reportes/socios/exportar')
    assert resp_csv_socios.status_code == 200
    assert resp_csv_socios.mimetype == 'text/csv'
    csv_socios_data = resp_csv_socios.data.decode('utf-8').lower()
    assert 'código' in csv_socios_data or 'codigo' in csv_socios_data
    assert 'dpi' in csv_socios_data
    assert 'estado' in csv_socios_data
    assert 'saldo ahorro' in csv_socios_data or 'saldo' in csv_socios_data


def test_generar_reporte_prestamos_endpoints(client):
    login_as_admin(client)

    # Test POST /generar_reporte_prestamos
    resp = client.post('/generar_reporte_prestamos', json={
        'tipo_reporte': 'cartera_activa',
        'fecha_inicio': '2026-01-01',
        'fecha_fin': '2026-12-31'
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['success'] is True
    assert 'resultados' in data
    assert 'estadisticas' in data
    assert 'morosidad' in data

    # Test export endpoints (Excel)
    resp_export_excel = client.get('/reporte_prestamos/export?tipo_reporte=cartera_activa&formato=excel')
    assert resp_export_excel.status_code in (200, 302)
    if resp_export_excel.status_code == 200:
        assert resp_export_excel.mimetype == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    # Test export endpoints (CSV)
    resp_export_csv = client.get('/reporte_prestamos/export?tipo_reporte=cartera_activa&formato=csv')
    assert resp_export_csv.status_code == 200
    assert resp_export_csv.mimetype == 'text/csv'
