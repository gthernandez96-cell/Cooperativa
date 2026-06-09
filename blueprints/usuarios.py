from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from werkzeug.security import generate_password_hash
from datetime import date
from utils.db import get_db, db_fetchall, db_execute, db_fetchone
from utils.decorators import login_required

bp = Blueprint('usuarios', __name__)

@bp.route('/usuarios')
@login_required(role='Administrador')
def usuarios():
    conn = get_db()
    
    # Parámetros de filtrado
    q = request.args.get('q', '').strip()
    rol_id = request.args.get('rol_id', '').strip()
    estado = request.args.get('estado', '').strip().lower()

    # Construcción de la consulta con filtros
    query = '''
        SELECT u.*, r.nombre as rol_nombre
        FROM usuarios u 
        LEFT JOIN roles r ON u.rol_id=r.id
        WHERE 1=1
    '''
    params = []

    if q:
        query += " AND (u.username LIKE ?)"
        params.append(f'%{q}%')
    
    if rol_id:
        query += " AND u.rol_id = ?"
        params.append(rol_id)
    
    if estado:
        query += " AND u.activo = ?"
        params.append('si' if estado == 'activo' else 'no')

    query += " ORDER BY u.id DESC"
    
    rows = db_fetchall(conn, query, params)

    roles = db_fetchall(conn, "SELECT id, nombre FROM roles WHERE estado='activo' ORDER BY nombre")
    conn.close()

    # Si es petición AJAX, retornamos solo el cuerpo de la tabla o JSON
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render_template('usuarios_tabla_fragmento.html', usuarios=rows)

    return render_template('usuarios.html', 
                           usuarios=rows, 
                           roles_filtro=roles, 
                           q=q, 
                           rol_id=rol_id, 
                           estado=estado)


@bp.route('/usuarios/toggle_estado/<int:uid>', methods=['POST'])
@login_required(role='Administrador')
def toggle_usuario_estado(uid):
    conn = get_db()
    try:
        user = db_fetchone(conn, "SELECT activo FROM usuarios WHERE id=?", [uid])
        if not user:
            return jsonify({'success': False, 'error': 'Usuario no encontrado'}), 404
        
        nuevo_estado = 'no' if user['activo'] == 'si' else 'si'
        db_execute(conn, "UPDATE usuarios SET activo=? WHERE id=?", (nuevo_estado, uid))
        conn.commit()
        return jsonify({'success': True, 'nuevo_estado': nuevo_estado})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()

@bp.route('/roles')
@login_required(role='Administrador')
def roles():
    conn = get_db()
    rows = db_fetchall(conn, "SELECT * FROM roles ORDER BY id DESC")
    conn.close()
    return render_template('roles.html', roles=rows)

@bp.route('/roles/nuevo', methods=['GET','POST'])
@login_required(role='Administrador')
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


@bp.route('/roles/<int:rid>/editar', methods=['GET','POST'])
@login_required(role='Administrador')
def editar_rol(rid):
    conn = get_db()
    rol = db_fetchone(conn, "SELECT * FROM roles WHERE id=?", [rid])
    if not rol:
        conn.close()
        flash('Rol no encontrado.', 'danger')
        return redirect(url_for('usuarios.roles'))

    if request.method == 'POST':
        nombre = request.form.get('nombre', '').strip()
        descripcion = request.form.get('descripcion', '').strip()
        estado = request.form.get('estado', 'activo').strip()

        if not nombre:
            flash('El nombre del rol es obligatorio.', 'danger')
        else:
            try:
                if rol['nombre'] == 'Administrador' and (nombre != 'Administrador' or estado != 'activo'):
                    flash('No se permite modificar el nombre o estado del rol Administrador.', 'danger')
                else:
                    db_execute(
                        conn,
                        "UPDATE roles SET nombre=?, descripcion=?, estado=? WHERE id=?",
                        (nombre, descripcion, estado, rid)
                    )
                    conn.commit()
                    flash('Rol actualizado exitosamente.', 'success')
                    return redirect(url_for('usuarios.roles'))
            except Exception as e:
                flash(f'Error al actualizar el rol: {e}', 'danger')
            finally:
                conn.close()
                conn = get_db()  # Reabrir para el render_template en caso de error
    
    conn.close()
    return render_template('editar_rol.html', rol=rol)


@bp.route('/roles/<int:rid>/eliminar', methods=['POST'])
@login_required(role='Administrador')
def eliminar_rol(rid):
    conn = get_db()
    try:
        rol = db_fetchone(conn, "SELECT * FROM roles WHERE id=?", [rid])
        if not rol:
            flash('Rol no encontrado.', 'danger')
            return redirect(url_for('usuarios.roles'))

        if rol['nombre'] == 'Administrador':
            flash('No se puede eliminar el rol Administrador.', 'danger')
            return redirect(url_for('usuarios.roles'))

        # Verificar si hay usuarios asociados a este rol
        usuarios_asociados = db_fetchone(conn, "SELECT 1 FROM usuarios WHERE rol_id=? LIMIT 1", [rid])
        if usuarios_asociados:
            flash('No se puede eliminar el rol porque tiene usuarios asociados.', 'danger')
            return redirect(url_for('usuarios.roles'))

        db_execute(conn, "DELETE FROM roles WHERE id=?", [rid])
        conn.commit()
        flash('Rol eliminado exitosamente.', 'success')
    except Exception as e:
        flash(f'Error al eliminar el rol: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('usuarios.roles'))


@bp.route('/usuarios/nuevo', methods=['GET','POST'])
@login_required(role='Administrador')
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
    else:
        conn.close()
    return render_template('nuevo_usuario.html', roles=roles)

@bp.route('/usuarios/editar/<int:uid>', methods=['GET', 'POST'])
@login_required(role='Administrador')
def editar_usuario(uid):
    conn = get_db()
    user = db_fetchone(conn, "SELECT * FROM usuarios WHERE id=?", [uid])
    roles = db_fetchall(conn, "SELECT id, nombre FROM roles WHERE estado='activo'")
    
    if not user:
        conn.close()
        flash('Usuario no encontrado', 'danger')
        return redirect(url_for('usuarios.usuarios'))

    if request.method == 'POST':
        try:
            db_execute(
                conn,
                "UPDATE usuarios SET username=?, rol_id=? WHERE id=?",
                (request.form['username'], request.form.get('rol_id'), uid)
            )
            conn.commit()
            flash('Usuario actualizado correctamente', 'success')
            return redirect(url_for('usuarios.usuarios'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()
    else:
        conn.close()
    
    return render_template('editar_usuario.html', user=user, roles=roles)

@bp.route('/usuarios/cambiar_password/<int:uid>', methods=['GET', 'POST'])
@login_required(role='Administrador')
def cambiar_password(uid):
    conn = get_db()
    user = db_fetchone(conn, "SELECT username FROM usuarios WHERE id=?", [uid])
    
    if not user:
        conn.close()
        flash('Usuario no encontrado', 'danger')
        return redirect(url_for('usuarios.usuarios'))

    if request.method == 'POST':
        nueva_pwd = request.form['password']
        confirmar = request.form['confirm_password']
        
        if nueva_pwd != confirmar:
            flash('Las contraseñas no coinciden', 'danger')
        else:
            try:
                hashed = generate_password_hash(nueva_pwd)
                db_execute(conn, "UPDATE usuarios SET password=? WHERE id=?", (hashed, uid))
                conn.commit()
                flash('Contraseña actualizada con éxito', 'success')
                return redirect(url_for('usuarios.usuarios'))
            except Exception as e:
                flash(f'Error: {e}', 'danger')
            finally:
                conn.close()
    else:
        conn.close()
    
    return render_template('cambiar_password.html', user=user, uid=uid)




