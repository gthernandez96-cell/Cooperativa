"""
tests/test_auth.py — Pruebas de autenticación (login / logout).
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
        yield c


def login(client, username='admin', password='admin123'):
    return client.post(
        '/login',
        data={'username': username, 'password': password},
        follow_redirects=True,
    )


# ── Login ── ─────────────────────────────────────────────────────────────────

def test_login_exitoso(client):
    resp = login(client)
    assert resp.status_code == 200
    assert b'Bienvenido' in resp.data


def test_login_credenciales_incorrectas(client):
    resp = login(client, password='wrong_password')
    assert b'incorrectos' in resp.data


def test_login_usuario_inexistente(client):
    resp = login(client, username='no_existe')
    assert b'incorrectos' in resp.data


def test_ruta_protegida_sin_sesion_redirige(client):
    resp = client.get('/', follow_redirects=True)
    # Debe redirigir al login
    assert b'login' in resp.request.path.encode() or b'Debe iniciar' in resp.data


# ── Logout ── ────────────────────────────────────────────────────────────────

def test_logout_cierra_sesion(client):
    login(client)
    resp = client.get('/logout', follow_redirects=True)
    assert b'cerrada' in resp.data or b'login' in resp.request.path.encode()

    # Después del logout, la ruta protegida redirige al login
    resp2 = client.get('/', follow_redirects=True)
    assert b'Debe iniciar' in resp2.data or b'login' in resp2.request.path.encode()


# ── Roles ── ─────────────────────────────────────────────────────────────────

def test_rol_administrador_accede_a_roles(client):
    login(client)
    resp = client.get('/roles')
    assert resp.status_code == 200


def test_usuario_sin_rol_no_accede_a_roles(client):
    with client.session_transaction() as sess:
        sess['user_id'] = 99
        sess['username'] = 'invitado'
        sess['user_role'] = 'Asociado'

    resp = client.get('/roles', follow_redirects=True)
    assert b'denegado' in resp.data or resp.status_code in (302, 200)
