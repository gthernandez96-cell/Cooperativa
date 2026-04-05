# blueprints/__init__.py
"""
Paquete de Blueprints de Flask.

Estructura planificada para migración incremental desde app.py:

  auth.py          → /login, /logout
  main.py          → / (dashboard)
  socios.py        → /socios, /socios/<id>, /socios/nuevo, etc.
  usuarios.py      → /usuarios, /roles
  ahorro.py        → /cuentas, /generar_planilla_cuotas_ahorro, etc.
  prestamos.py     → /prestamos, /api/cuota, etc.
  configuraciones.py → /configuraciones
  auditoria.py     → /auditoria_eventos, /cierres_periodo

Para activar un blueprint, importarlo en app.py y registrarlo:
    from blueprints.auth import bp as auth_bp
    app.register_blueprint(auth_bp)
"""
