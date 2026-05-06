from flask import Blueprint, render_template, request, redirect, url_for, flash
from werkzeug.security import generate_password_hash
from datetime import date
from utils.db import get_db, db_fetchall, db_execute
from utils.decorators import login_required

bp = Blueprint('usuarios', __name__)

@bp.route('/roles')
@login_required(role='Administrador')
def roles():
    conn = get_db()
    rows = db_fetchall(conn, "SELECT * FROM roles ORDER BY id DESC")
    conn.close()
    return render_template('roles.html', roles=rows)

@bp.route('/roles/nuevo', methods=['GET','POST'])
def nuevo_rol():
    if request.method == 'POST':
        conn = get_db()
        try:
            db_execute(
                conn,
                "INSERT INTO roles (nombre,descripcion) VALUES (?,?)",
                (request.form['nombre'], request.form['descripcion'])
            )
            conn.commit()
            flash('Rol creado exitosamente.', 'success')
            return redirect(url_for('usuarios.roles'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()
    return render_template('nuevo_rol.html')

@bp.route('/usuarios')
@login_required(role='Administrador')
def usuarios():
    conn = get_db()
    rows = db_fetchall(
        conn,
        '''SELECT u.*, r.nombre as rol_nombre
           FROM usuarios u LEFT JOIN roles r ON u.rol_id=r.id
           ORDER BY u.id DESC'''
    )
    conn.close()
    return render_template('usuarios.html', usuarios=rows)

@bp.route('/usuarios/nuevo', methods=['GET','POST'])
def nuevo_usuario():
    conn = get_db()
    roles = db_fetchall(conn, "SELECT id,nombre FROM roles WHERE estado='activo'")
    if request.method == 'POST':
        try:
            db_execute(
                conn,
                "INSERT INTO usuarios (username,password,rol_id,fecha_creacion) VALUES (?,?,?,?)",
                (
                    request.form['username'],
                    generate_password_hash(request.form['password']),
                    request.form.get('rol_id'),
                    date.today().isoformat(),
                )
            )
            conn.commit()
            flash('Usuario creado exitosamente.', 'success')
            return redirect(url_for('usuarios.usuarios'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()
    conn.close()
    return render_template('nuevo_usuario.html', roles=roles)
