"""
tests/test_https.py — Pruebas para verificar la redirección a HTTPS y soporte de ProxyFix.
"""
import os
import pytest
from app import create_app

def test_https_redirection_disabled_in_testing():
    """
    En modo de pruebas (TESTING=True), la redirección a HTTPS debe ser omitida
    para no romper las pruebas unitarias que se hacen sobre http.
    """
    app = create_app()
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    with app.test_client() as client:
        resp = client.get('/login')
        assert resp.status_code == 200

def test_https_redirection_with_force_https(monkeypatch):
    """
    Si FORCE_HTTPS está habilitado, las peticiones HTTP inseguras deben
    ser redirigidas automáticamente a HTTPS con un código 301.
    """
    monkeypatch.setenv('FORCE_HTTPS', 'True')
    app = create_app()
    app.config['TESTING'] = False
    app.config['WTF_CSRF_ENABLED'] = False
    with app.test_client() as client:
        resp = client.get('/login', base_url='http://localhost:8001')
        assert resp.status_code == 301
        assert resp.headers['Location'].startswith('https://')

def test_proxy_fix_headers(monkeypatch):
    """
    Si se usa la cabecera X-Forwarded-Proto: https (por ejemplo, detrás de PythonAnywhere),
    el middleware ProxyFix debe procesarla de forma que Flask considere la petición como segura
    y no realice redirecciones redundantes a HTTPS.
    """
    monkeypatch.setenv('FORCE_HTTPS', 'True')
    app = create_app()
    app.config['TESTING'] = False
    app.config['WTF_CSRF_ENABLED'] = False
    with app.test_client() as client:
        # Con X-Forwarded-Proto: https, Flask debe considerarlo request.is_secure y retornar 200
        resp = client.get('/login', base_url='http://localhost:8001', headers={'X-Forwarded-Proto': 'https'})
        assert resp.status_code == 200
