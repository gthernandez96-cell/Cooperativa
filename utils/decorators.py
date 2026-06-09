import hashlib
from datetime import datetime
from functools import wraps
from flask import session, flash, redirect, url_for, request
from utils.db import get_db
from config import ROLE_PERMISSION_DEFAULTS



def usuario_tiene_permiso(conn, user_id, user_role, permiso):
    if not user_id:
        return False
    u_role_lower = (user_role or '').strip().lower()
    if u_role_lower == 'administrador':
        return True

    role_defaults = set()
    for key, val in ROLE_PERMISSION_DEFAULTS.items():
        if key.strip().lower() == u_role_lower:
            role_defaults = val
            break

    if '*' in role_defaults or permiso in role_defaults:
        return True

    row = conn.execute(
        '''
        SELECT 1
        FROM usuarios u
        JOIN roles r ON r.id = u.rol_id
        JOIN rol_permisos rp ON rp.rol_id = r.id
        JOIN permisos p ON p.id = rp.permiso_id
        WHERE u.id = ? AND p.codigo = ?
        LIMIT 1
        ''',
        (user_id, permiso),
    ).fetchone()
    return row is not None

def permission_required(permiso):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if 'user_id' not in session:
                flash('Debe iniciar sesión', 'danger')
                return redirect(url_for('auth.login'))
            
            conn = get_db()
            ok = usuario_tiene_permiso(
                conn,
                session.get('user_id'),
                session.get('user_role'),
                permiso,
            )
            
            if not ok:
                flash('No tiene permisos para ejecutar esta acción.', 'danger')
                return redirect(url_for('main.index'))
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def validate_idempotency(conn, scope):
    key = request.headers.get('X-Idempotency-Key') or (request.get_json(silent=True) or {}).get('idempotency_key')
    if not key:
        return True
    composed = f"{scope}:{session.get('user_id', 'anon')}:{key}"
    digest = hashlib.sha256(composed.encode('utf-8')).hexdigest()
    row = conn.execute('SELECT 1 FROM idempotency_keys WHERE key_hash=?', (digest,)).fetchone()
    if row:
        return False
    conn.execute(
        'INSERT INTO idempotency_keys (key_hash, scope, user_id, created_at) VALUES (?, ?, ?, ?)',
        (digest, scope, session.get('user_id'), datetime.now().isoformat()),
    )
    return True

def login_required(role=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if 'user_id' not in session:
                flash('Debe iniciar sesión', 'danger')
                return redirect(url_for('auth.login'))

            if role:
                user_role = (session.get('user_role') or '').strip().lower()
                allowed_roles = role
                if isinstance(role, str):
                    allowed_roles = [role]
                allowed_roles_lower = [r.strip().lower() for r in allowed_roles]
                if user_role != 'administrador' and user_role not in allowed_roles_lower:
                    flash('Acceso denegado para su rol', 'danger')
                    return redirect(url_for('main.index'))

            return fn(*args, **kwargs)
        return wrapper
    return decorator
