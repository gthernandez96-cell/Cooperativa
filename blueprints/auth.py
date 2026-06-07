from flask import Blueprint, request, render_template, session, flash, redirect, url_for
from werkzeug.security import check_password_hash
from datetime import datetime
from utils.db import get_db, db_fetchone

bp = Blueprint('auth', __name__)

def get_limiter():
    """Importación lazy del limiter para evitar importación circular."""
    from app import limiter
    return limiter

@bp.route('/login', methods=['GET','POST'])
def login():
    # Aplicar rate limiting: 10 intentos por 5 minutos por IP
    try:
        limiter = get_limiter()
        limiter.limit("10 per 5 minutes")(lambda: None)()
    except Exception:
        pass

    if request.method == 'POST':
        user = request.form['username'].strip()
        pwd = request.form['password'].strip()
        conn = get_db()
        row = db_fetchone(
            conn,
            "SELECT u.*, r.nombre as rol_nombre FROM usuarios u LEFT JOIN roles r ON u.rol_id=r.id WHERE u.username=?",
            (user,)
        )
        conn.close()
        if not row or not check_password_hash(row['password'], pwd):
            flash('Usuario o contraseña incorrectos', 'danger')
            return render_template('login.html')
        if row['activo'] != 'si':
            flash('Cuenta inactiva', 'danger')
            return render_template('login.html')
        session.permanent = True
        session['user_id'] = row['id']
        session['username'] = row['username']
        session['user_role'] = (row['rol_nombre'] or 'Asociado').strip()
        session['_last_active'] = datetime.now().isoformat()
        flash('Bienvenido ' + session['username'], 'success')
        if session['user_role'].lower() == 'promotora':
            return redirect(url_for('promotora.dashboard'))
        return redirect(url_for('main.index'))
    return render_template('login.html')


@bp.route('/logout')
def logout():
    session.clear()
    flash('Sesión cerrada', 'info')
    return redirect(url_for('auth.login'))
