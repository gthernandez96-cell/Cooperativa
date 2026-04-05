"""
tests/conftest.py — Fixtures compartidos para toda la suite de pruebas.
"""
import pytest
import app as app_module


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Cliente HTTP con BD limpia en directorio temporal. CSRF desactivado."""
    db_path = tmp_path / "cooperativa_test.db"
    monkeypatch.setattr(app_module, 'DB', str(db_path))
    app_module.app.config['TESTING'] = True
    app_module.app.config['WTF_CSRF_ENABLED'] = False
    app_module.init_db()
    with app_module.app.test_client() as c:
        yield c


@pytest.fixture
def admin_client(app_client):
    """Cliente HTTP ya autenticado como administrador."""
    app_client.post('/login', data={'username': 'admin', 'password': 'admin123'})
    return app_client
