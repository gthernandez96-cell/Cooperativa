"""
utils/auth.py — Decorador de autenticación y autorización.
"""
from functools import wraps
from flask import session, flash, redirect, url_for


def login_required(role=None):
    """Decorador que protege rutas.
    - Sin argumento: solo requiere sesión activa.
    - Con role (str o tupla): además verifica el rol del usuario.
      Los Administradores siempre tienen acceso.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if 'user_id' not in session:
                flash('Debe iniciar sesión', 'danger')
                return redirect(url_for('auth.login'))

            if role:
                user_role = session.get('user_role')
                allowed_roles = [role] if isinstance(role, str) else list(role)
                if user_role != 'Administrador' and user_role not in allowed_roles:
                    flash('Acceso denegado para su rol', 'danger')
                    return redirect(url_for('main.index'))

            return fn(*args, **kwargs)
        return wrapper
    return decorator
