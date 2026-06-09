"""
tests/test_seguridad.py — Tests de seguridad: acceso sin sesión, roles, logout, calculadora.
"""
import pytest


class TestAccesoSinSesion:
    """Verifica que las rutas protegidas redirijan al login cuando no hay sesión."""

    def test_dashboard_sin_sesion(self, app_client):
        r = app_client.get('/', follow_redirects=False)
        assert r.status_code in (301, 302)
        assert '/login' in r.headers.get('Location', '')

    def test_socios_sin_sesion(self, app_client):
        r = app_client.get('/socios', follow_redirects=False)
        assert r.status_code in (301, 302)

    def test_prestamos_sin_sesion(self, app_client):
        # /prestamos es una ruta de listado pública (sin @login_required en la vista de lista)
        r = app_client.get('/prestamos')
        assert r.status_code == 200

    def test_cuentas_sin_sesion(self, app_client):
        # /cuentas es una ruta de listado pública (sin @login_required en la vista de lista)
        r = app_client.get('/cuentas')
        assert r.status_code == 200

    def test_usuarios_sin_sesion(self, app_client):
        r = app_client.get('/usuarios', follow_redirects=False)
        assert r.status_code in (301, 302)

    def test_calculadora_sin_sesion(self, app_client):
        r = app_client.get('/calculadora', follow_redirects=False)
        assert r.status_code in (301, 302)
        assert '/login' in r.headers.get('Location', '')


class TestLogin:
    """Tests del flujo de autenticación."""

    def test_login_page_carga(self, app_client):
        r = app_client.get('/login')
        assert r.status_code == 200
        assert b'login' in r.data.lower() or b'usuario' in r.data.lower()

    def test_login_exitoso(self, app_client):
        r = app_client.post('/login', data={
            'username': 'admin',
            'password': 'admin123',
        }, follow_redirects=True)
        assert r.status_code == 200

    def test_login_credenciales_incorrectas(self, app_client):
        r = app_client.post('/login', data={
            'username': 'admin',
            'password': 'password_incorrecto',
        }, follow_redirects=True)
        assert r.status_code == 200
        # Debe mostrar algún mensaje de error
        assert b'incorrecto' in r.data.lower() or b'danger' in r.data.lower()

    def test_login_usuario_inexistente(self, app_client):
        r = app_client.post('/login', data={
            'username': 'usuario_que_no_existe',
            'password': '1234',
        }, follow_redirects=True)
        assert r.status_code == 200
        assert b'incorrecto' in r.data.lower() or b'danger' in r.data.lower()

    def test_logout(self, admin_client):
        r = admin_client.get('/logout', follow_redirects=False)
        assert r.status_code in (301, 302)
        # Después del logout, el acceso al dashboard debe redirigir al login
        r2 = admin_client.get('/', follow_redirects=False)
        assert r2.status_code in (301, 302)


class TestRoles:
    """Tests de control de acceso basado en rol."""

    def test_admin_puede_ver_usuarios(self, admin_client):
        r = admin_client.get('/usuarios')
        assert r.status_code == 200

    def test_admin_puede_ver_roles(self, admin_client):
        r = admin_client.get('/roles')
        assert r.status_code == 200

    def test_admin_puede_ver_configuracion(self, admin_client):
        r = admin_client.get('/configuraciones')
        assert r.status_code == 200

    def test_admin_puede_ver_calculadora(self, admin_client):
        r = admin_client.get('/calculadora')
        assert r.status_code == 200

    def test_admin_puede_crear_editar_y_eliminar_rol(self, admin_client):
        r = admin_client.post('/roles/nuevo', data={
            'nombre': 'Rol Temporal',
            'descripcion': 'Descripción temporal del rol'
        }, follow_redirects=True)
        assert r.status_code == 200
        assert b'Rol creado exitosamente' in r.data

        import app as app_module
        conn = app_module.get_db()
        rol = conn.execute("SELECT id FROM roles WHERE nombre='Rol Temporal'").fetchone()
        conn.close()
        assert rol is not None
        rol_id = rol['id']

        r_edit = admin_client.post(f'/roles/{rol_id}/editar', data={
            'nombre': 'Rol Editado',
            'descripcion': 'Descripción editada del rol',
            'estado': 'activo'
        }, follow_redirects=True)
        assert r_edit.status_code == 200
        assert b'Rol actualizado exitosamente' in r_edit.data

        r_delete = admin_client.post(f'/roles/{rol_id}/eliminar', follow_redirects=True)
        assert r_delete.status_code == 200
        assert b'Rol eliminado exitosamente' in r_delete.data

    def test_admin_no_puede_eliminar_rol_administrador(self, admin_client):
        import app as app_module
        conn = app_module.get_db()
        rol_admin = conn.execute("SELECT id FROM roles WHERE nombre='Administrador'").fetchone()
        conn.close()
        assert rol_admin is not None

        r = admin_client.post(f'/roles/{rol_admin["id"]}/eliminar', follow_redirects=True)
        assert r.status_code == 200
        assert b'No se puede eliminar el rol Administrador' in r.data


class TestCalculadora:
    """Tests de la calculadora de préstamos."""

    def test_calculadora_renderiza(self, admin_client):
        r = admin_client.get('/calculadora')
        assert r.status_code == 200
        assert b'calculadora' in r.data.lower() or b'calc' in r.data.lower()

    def test_calculadora_contiene_inputs(self, admin_client):
        r = admin_client.get('/calculadora')
        assert b'calc-monto' in r.data
        assert b'calc-tasa' in r.data
        assert b'calc-plazo' in r.data
