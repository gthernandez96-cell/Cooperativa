import pytest
import app as app_module
from werkzeug.security import generate_password_hash

@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "cooperativa_test.db"
    monkeypatch.setattr(app_module, 'DB', str(db_path))
    app_module.app.config['TESTING'] = True
    app_module.app.config['WTF_CSRF_ENABLED'] = False  # Desactivar CSRF en tests
    app_module.init_db()

    with app_module.app.test_client() as client:
        yield client


def login_as_admin(client):
    return client.post('/login', data={'username': 'admin', 'password': 'admin123'}, follow_redirects=True)


def test_editar_socio_actualiza_campos(client):
    login_as_admin(client)

    conn = app_module.get_db()
    socio = conn.execute("SELECT id FROM socios WHERE codigo=?", ('SOC-001',)).fetchone()
    conn.close()

    assert socio is not None
    sid = socio['id']

    response = client.post(f'/socios/{sid}/editar', data={
        'codigo': 'SOC-001',
        'primer_nombre': 'María',
        'segundo_nombre': 'Modificada',
        'tercer_nombre': '',
        'primer_apellido': 'García',
        'segundo_apellido': '',
        'dpi': '1234567890101',
        'telefono': '5555-9999',
        'email': 'mariana@example.com',
        'direccion': 'Zona 1 actualizada',
        'rol': 'Asociado',
        'frecuencia': 'Quincenal',
        'cuota_ahorro': '0',
        'tipo_ahorro': 'ahorro corriente',
    }, follow_redirects=True)

    assert b'Socio actualizado correctamente' in response.data

    conn = app_module.get_db()
    socio_actualizado = conn.execute('SELECT telefono, email, direccion FROM socios WHERE id=?', (sid,)).fetchone()
    conn.close()

    assert socio_actualizado['telefono'] == '5555-9999'
    assert socio_actualizado['email'] == 'mariana@example.com'


def test_inactivar_y_activar_socio(client):
    login_as_admin(client)

    conn = app_module.get_db()
    conn.execute("INSERT INTO socios (codigo,nombre,apellido,dpi,telefono,email,direccion,rol,fecha_ingreso,estado) VALUES (?,?,?,?,?,?,?,?,?,?)",
                 ('SOC-TEMP', 'Temp', 'User', '9999999999999', '0000', 'temp@a.com', 'temp', 'Asociado', '2025-01-01', 'activo'))
    conn.commit()
    socio = conn.execute("SELECT id FROM socios WHERE codigo=?", ('SOC-TEMP',)).fetchone()
    conn.close()

    sid = socio['id']

    response = client.post(f'/socios/{sid}/inactivar', follow_redirects=True)
    assert b'Socio inactivado.' in response.data

    conn = app_module.get_db()
    estado = conn.execute('SELECT estado FROM socios WHERE id=?', (sid,)).fetchone()['estado']
    conn.close()
    assert estado == 'inactivo'

    response = client.post(f'/socios/{sid}/activar', follow_redirects=True)
    assert b'Socio activado.' in response.data

    conn = app_module.get_db()
    estado = conn.execute('SELECT estado FROM socios WHERE id=?', (sid,)).fetchone()['estado']
    conn.close()
    assert estado == 'activo'


def test_editar_socio_requiere_admin_operador(client):
    conn = app_module.get_db()
    conn.execute("INSERT INTO socios (codigo,nombre,apellido,dpi,telefono,email,direccion,rol,fecha_ingreso,estado) VALUES (?,?,?,?,?,?,?,?,?,?)",
                 ('SOC-LOCK', 'Lock', 'User', '8888888888888', '0000', 'lock@a.com', 'temp', 'Asociado', '2025-01-01', 'activo'))
    conn.commit()
    socio = conn.execute("SELECT id FROM socios WHERE codigo=?", ('SOC-LOCK',)).fetchone()
    conn.close()

    sid = socio['id']

    with client.session_transaction() as sess:
        sess['user_id'] = 99
        sess['username'] = 'testuser'
        sess['user_role'] = 'Asociado'

    response = client.get(f'/socios/{sid}/editar', follow_redirects=True)
    assert b'Acceso denegado' in response.data
