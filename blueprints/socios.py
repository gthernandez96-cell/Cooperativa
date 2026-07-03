from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
import math
import json
import os
from datetime import date, datetime

from utils.db import get_db, db_fetchone, db_fetchall, db_execute, db_insert_and_get_id, db_executemany
from utils.decorators import login_required
from utils.nombres import preparar_datos_socio, construir_nombre_completo, construir_apellido_completo, validar_dpi, resumen_beneficiarios
from utils.images import allowed_image as allowed_socio_image, procesar_foto_socio
from utils.financial import calcular_total_cuotas_prestamo
from utils.helpers import obtener_beneficiarios_socio, parsear_beneficiarios_form, log_auditoria_socio

def asegurar_cuentas_socio(conn, socio_id):
    productos = ['ahorro_corriente', 'ahorro_aportacion', 'ahorro_inscripcion']
    prefijos = {
        'ahorro_aportacion': 'APR',
        'ahorro_corriente': 'COR',
        'ahorro_inscripcion': 'INS',
    }
    for prod in productos:
        cuenta_existente = db_fetchone(
            conn,
            "SELECT id FROM cuentas WHERE socio_id=? AND tipo='ahorro' AND COALESCE(producto_ahorro, 'ahorro_corriente')=?",
            [socio_id, prod]
        )
        if not cuenta_existente:
            count = db_fetchone(conn, "SELECT COUNT(*) FROM cuentas")[0] or 0
            numero = f"{prefijos[prod]}-{count+1:04d}"
            tasa = db_fetchone(conn, "SELECT tasa_interes FROM configuraciones WHERE tipo=?", [prod])
            tasa_val = float(tasa['tasa_interes']) if tasa else 0.0
            db_execute(
                conn,
                """
                INSERT INTO cuentas (numero, socio_id, tipo, producto_ahorro, saldo, tasa_interes, fecha_apertura)
                VALUES (?, ?, 'ahorro', ?, 0, ?, ?)
                """,
                (numero, socio_id, prod, tasa_val, date.today().isoformat())
            )

bp = Blueprint('socios', __name__)

@bp.route('/socios')
@login_required()
def socios():
    q = request.args.get('q', '')
    estado_filtro = request.args.get('estado', '').strip().lower()
    frecuencia_filtro = request.args.get('frecuencia', '').strip()
    page = max(1, int(request.args.get('page', 1) or 1))
    per_page = min(100, max(10, int(request.args.get('per_page', 25) or 25)))
    offset = (page - 1) * per_page
    conn = get_db()

    # ── Conteos para las tarjetas de estadísticas ──────────────────────────────
    conteos = {
        'activos':    db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE estado='activo'")[0],
        'inactivos':  db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE estado='inactivo'")[0],
        'quincenal':  db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE frecuencia='Quincenal'")[0],
        'catorcenal': db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE frecuencia='Catorcenal'")[0],
    }

    # ── Construcción dinámica de la query con filtros ──────────────────────────
    conditions = []
    params = []

    if q:
        like = f'%{q}%'
        conditions.append('''(nombre LIKE ? OR apellido LIKE ? OR codigo LIKE ? OR dpi LIKE ?
            OR primer_nombre LIKE ? OR segundo_nombre LIKE ? OR primer_apellido LIKE ?
            OR segundo_apellido LIKE ? OR telefono LIKE ?)''')
        params.extend([like] * 9)

    if estado_filtro:
        conditions.append('estado = ?')
        params.append(estado_filtro)

    if frecuencia_filtro:
        conditions.append('frecuencia = ?')
        params.append(frecuencia_filtro)

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''

    total = db_fetchone(conn, f'SELECT COUNT(*) FROM socios {where}', params)[0]
    rows = db_fetchall(
        conn,
        f'SELECT * FROM socios {where} ORDER BY id DESC LIMIT ? OFFSET ?',
        params + [per_page, offset]
    )

    conn.close()
    socios_lista = [preparar_datos_socio(row) for row in rows]
    total_pages = max(1, math.ceil(total / per_page))
    return render_template(
        'socios.html',
        socios=socios_lista,
        q=q,
        estado_filtro=estado_filtro,
        frecuencia_filtro=frecuencia_filtro,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        conteos=conteos,
    )


@bp.route('/socios/nuevo', methods=['GET','POST'])
def nuevo_socio():
    conn = get_db()
    codigo_sugerido = f"SOC-{db_fetchone(conn, 'SELECT COUNT(*) FROM socios')[0] + 1:03d}"
    conn.close()
    if request.method == 'POST':
        conn = get_db()
        count = db_fetchone(conn, "SELECT COUNT(*) FROM socios")[0]
        codigo = request.form.get('codigo', '').strip().upper() or f'SOC-{count+1:03d}'

        existente_codigo = db_fetchone(conn, "SELECT id FROM socios WHERE codigo=?", (codigo,))
        if existente_codigo:
            flash('Ya existe un socio con ese código.', 'danger')
            conn.close()
            return render_template('nuevo_socio.html', codigo_sugerido=codigo_sugerido, beneficiarios=request.form)

        try:
            beneficiarios = parsear_beneficiarios_form(request.form)
            primer_nombre = request.form.get('primer_nombre', '').strip()
            segundo_nombre = request.form.get('segundo_nombre', '').strip()
            tercer_nombre = request.form.get('tercer_nombre', '').strip()
            primer_apellido = request.form.get('primer_apellido', '').strip()
            segundo_apellido = request.form.get('segundo_apellido', '').strip()
            estado_civil = request.form.get('estado_civil', 'Soltero').strip() or 'Soltero'
            apellido_casada = request.form.get('apellido_casada', '').strip() if estado_civil == 'Casado' else ''
            nombre = construir_nombre_completo(primer_nombre, segundo_nombre, tercer_nombre)
            apellido = construir_apellido_completo(primer_apellido, segundo_apellido)

            if not primer_nombre or not primer_apellido or not request.form.get('dpi', '').strip():
                raise ValueError('Código, primer nombre, primer apellido y DPI son obligatorios.')

            salario = request.form.get('salario', '').strip()
            salario_val = float(salario) if salario else None
            fecha_ingreso_laborar = request.form.get('fecha_ingreso_laborar', '').strip() or None
            departamento = request.form.get('departamento', '').strip()
            municipio = request.form.get('municipio', '').strip()
            fecha_ingreso_cooperativa = request.form.get('fecha_ingreso_cooperativa', '').strip() or date.today().isoformat()

            db_execute(
                conn,
                '''INSERT INTO socios (
                       codigo,nombre,primer_nombre,segundo_nombre,tercer_nombre,
                       apellido,primer_apellido,segundo_apellido,estado_civil,apellido_casada,
                       dpi,telefono,email,direccion,departamento,municipio,rol,fecha_ingreso,nit,beneficiario,
                       banco_nombre,banco_tipo_cuenta,banco_numero_cuenta,
                       frecuencia,cuota_ahorro,cuota_aportacion,cuota_inscripcion,tipo_ahorro,finca,
                       salario,fecha_ingreso_laborar,frecuencia_credito_pos,cuota_credito_pos,limite_credito_pos
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    codigo, nombre, primer_nombre, segundo_nombre, tercer_nombre,
                    apellido, primer_apellido, segundo_apellido, estado_civil, apellido_casada,
                    request.form.get('dpi', '').strip(), request.form.get('telefono', '').strip(),
                    request.form.get('email', '').strip(), request.form.get('direccion', '').strip(),
                    departamento, municipio, 'Asociado', fecha_ingreso_cooperativa,
                    request.form.get('nit', '').strip(), resumen_beneficiarios(beneficiarios),
                    request.form.get('banco_nombre', '').strip(), request.form.get('banco_tipo_cuenta', '').strip(),
                    request.form.get('banco_numero_cuenta', '').strip(),
                    request.form.get('frecuencia', 'Quincenal').strip() or 'Quincenal',
                    float(request.form.get('cuota_ahorro', 0) or 0),
                    float(request.form.get('cuota_aportacion', 0) or 0),
                    float(request.form.get('cuota_inscripcion', 0) or 0),
                    request.form.get('tipo_ahorro', 'ahorro corriente').strip() or 'ahorro corriente',
                    request.form.get('finca', '').strip(),
                    salario_val,
                    fecha_ingreso_laborar,
                    request.form.get('frecuencia_credito_pos', 'Quincenal').strip() or 'Quincenal',
                    float(request.form.get('cuota_credito_pos', 0) or 0),
                    float(request.form.get('limite_credito_pos', 500) or 500),
                )
            )
            socio_insertado = db_fetchone(conn, 'SELECT id FROM socios WHERE codigo=?', (codigo,))
            socio_id = socio_insertado['id'] if socio_insertado else None
            
            # Apertura automática de cuentas
            if socio_id:
                asegurar_cuentas_socio(conn, socio_id)

            if beneficiarios:
                db_executemany(
                    conn,
                    'INSERT INTO socio_beneficiarios (socio_id, nombre, parentesco, porcentaje) VALUES (?, ?, ?, ?)',
                    [
                        (socio_id, item['nombre'], item['parentesco'], item['porcentaje'])
                        for item in beneficiarios
                    ]
                )
            conn.commit()
            flash('Socio registrado exitosamente.', 'success')
            return redirect(url_for('socios.socios'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()
    return render_template('nuevo_socio.html', codigo_sugerido=codigo_sugerido, beneficiarios=None)

@bp.route('/socios/<int:sid>')
@login_required()
def detalle_socio(sid):
    conn = get_db()
    socio = db_fetchone(conn, "SELECT * FROM socios WHERE id=?", [sid])
    cuentas = db_fetchall(conn, "SELECT * FROM cuentas WHERE socio_id=?", [sid])
    prestamos = db_fetchall(conn, '''
        SELECT p.*,
               s.frecuencia,
               pc.nombre AS categoria_nombre,
               COALESCE(pp.pagos_realizados, 0) AS pagos_realizados,
               COALESCE(pp.monto_pagado, 0) AS monto_pagado,
               CASE
                   WHEN p.estado = 'pendiente' THEN 'Pendiente de aprobacion'
                   WHEN p.estado = 'pagado' OR COALESCE(p.saldo_pendiente, 0) <= 0 THEN 'Cancelado'
                   WHEN p.estado = 'aprobado' AND COALESCE(pp.pagos_realizados, 0) = 0 THEN 'Activo sin pagos'
                   WHEN p.estado = 'aprobado' THEN 'Activo con pagos'
                   ELSE 'En revision'
               END AS estado_cuenta
        FROM prestamos p
        JOIN socios s ON s.id = p.socio_id
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        LEFT JOIN (
            SELECT prestamo_id,
                   COUNT(*) AS pagos_realizados,
                   SUM(monto) AS monto_pagado
            FROM pagos_prestamo
            GROUP BY prestamo_id
        ) pp ON p.id = pp.prestamo_id
        WHERE p.socio_id=?
        ORDER BY p.id DESC
    ''', [sid])

    pagos_prestamos = db_fetchall(conn, '''
        SELECT pp.*, p.numero AS numero_prestamo
        FROM pagos_prestamo pp
        JOIN prestamos p ON pp.prestamo_id = p.id
        WHERE p.socio_id=?
        ORDER BY date(pp.fecha) DESC, pp.id DESC
    ''', [sid])

    beneficiarios = obtener_beneficiarios_socio(conn, sid)

    conn.close()

    socio = preparar_datos_socio(socio)

    prestamos_normalizados = []
    for prestamo in prestamos:
        item = dict(prestamo)
        item['total_cuotas'] = calcular_total_cuotas_prestamo(item.get('plazo_meses'), item.get('frecuencia'))
        prestamos_normalizados.append(item)

    return render_template(
        'detalle_socio.html',
        socio=socio,
        beneficiarios=beneficiarios,
        cuentas=cuentas,
        prestamos=prestamos_normalizados,
        pagos_prestamos=pagos_prestamos
    )

@bp.route('/socios/<int:sid>/editar', methods=['GET', 'POST'])
@login_required(role=('Administrador', 'Operador'))
def editar_socio(sid):
    conn = get_db()
    socio = db_fetchone(conn, "SELECT * FROM socios WHERE id=?", [sid])
    if not socio:
        conn.close()
        flash('Socio no encontrado.', 'danger')
        return redirect(url_for('socios.socios'))

    # Convertir a diccionario para acceso seguro
    socio_dict = preparar_datos_socio(socio)
    beneficiarios_existentes = obtener_beneficiarios_socio(conn, sid)

    if request.method == 'POST':
        codigo = request.form.get('codigo', '').strip().upper()
        primer_nombre = request.form.get('primer_nombre', '').strip()
        segundo_nombre = request.form.get('segundo_nombre', '').strip()
        tercer_nombre = request.form.get('tercer_nombre', '').strip()
        primer_apellido = request.form.get('primer_apellido', '').strip()
        segundo_apellido = request.form.get('segundo_apellido', '').strip()
        nombre = construir_nombre_completo(primer_nombre, segundo_nombre, tercer_nombre)
        apellido = construir_apellido_completo(primer_apellido, segundo_apellido)
        dpi = request.form.get('dpi', '').strip()
        telefono = request.form.get('telefono', '').strip()
        email = request.form.get('email', '').strip()
        direccion = request.form.get('direccion', '').strip()
        estado_civil = request.form.get('estado_civil', socio_dict.get('estado_civil') or 'Soltero').strip() or 'Soltero'
        apellido_casada = request.form.get('apellido_casada', '').strip() if estado_civil == 'Casado' else ''
        frecuencia = request.form.get('frecuencia', socio_dict.get('frecuencia') or 'Quincenal')
        frecuencia_credito_pos = request.form.get('frecuencia_credito_pos', socio_dict.get('frecuencia_credito_pos') or 'Quincenal')
        cuota_credito_pos = float(request.form.get('cuota_credito_pos', socio_dict.get('cuota_credito_pos') or 0) or 0)
        cuota_ahorro = float(request.form.get('cuota_ahorro', socio_dict.get('cuota_ahorro') or 0) or 0)
        cuota_aportacion = float(request.form.get('cuota_aportacion', socio_dict.get('cuota_aportacion') or 0) or 0)
        cuota_inscripcion = float(request.form.get('cuota_inscripcion', socio_dict.get('cuota_inscripcion') or 0) or 0)
        tipo_ahorro = request.form.get('tipo_ahorro', socio_dict.get('tipo_ahorro') or 'ahorro corriente')
        nit = request.form.get('nit', '').strip()
        finca = request.form.get('finca', '').strip()
        banco_nombre = request.form.get('banco_nombre', '').strip()
        banco_tipo_cuenta = request.form.get('banco_tipo_cuenta', '').strip()
        banco_numero_cuenta = request.form.get('banco_numero_cuenta', '').strip()
        foto = request.files.get('foto')

        if not codigo or not primer_nombre or not primer_apellido or not dpi:
            flash('Código, primer nombre, primer apellido y DPI son obligatorios.', 'danger')
            conn.close()
            return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)

        existente_codigo = db_fetchone(conn, "SELECT id FROM socios WHERE codigo=? AND id<>?", (codigo, sid))
        if existente_codigo:
            flash('Ya existe otro socio con ese código.', 'danger')
            conn.close()
            return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)

        existente = db_fetchone(conn, "SELECT id FROM socios WHERE dpi=? AND id<>?", (dpi, sid))
        if existente:
            flash('Ya existe otro socio con ese DPI.', 'danger')
            conn.close()
            return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)

        try:
            beneficiarios = parsear_beneficiarios_form(request.form)
            datos_previos = dict(socio)
            ruta_foto = socio_dict.get('foto')

            if foto and foto.filename:
                if not allowed_socio_image(foto.filename):
                    conn.close()
                    flash('Formato de foto no permitido. Use PNG, JPG, JPEG o WEBP.', 'warning')
                    return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)
                ruta_foto = procesar_foto_socio(foto, sid)

            salario = request.form.get('salario', '').strip()
            salario_val = float(salario) if salario else None
            fecha_ingreso_laborar = request.form.get('fecha_ingreso_laborar', '').strip() or None
            departamento = request.form.get('departamento', '').strip()
            municipio = request.form.get('municipio', '').strip()
            fecha_ingreso_cooperativa = request.form.get('fecha_ingreso_cooperativa', '').strip() or date.today().isoformat()

            db_execute(conn, '''
                UPDATE socios SET codigo=?, nombre=?, primer_nombre=?, segundo_nombre=?, tercer_nombre=?,
                                  apellido=?, primer_apellido=?, segundo_apellido=?, estado_civil=?, apellido_casada=?,
                                  dpi=?, telefono=?, email=?, direccion=?, departamento=?, municipio=?,
                                  rol=?, fecha_ingreso=?, frecuencia=?, cuota_ahorro=?, cuota_aportacion=?, cuota_inscripcion=?, tipo_ahorro=?,
                                  nit=?, beneficiario=?, finca=?, banco_nombre=?, banco_tipo_cuenta=?, banco_numero_cuenta=?, foto=?,
                                  salario=?, fecha_ingreso_laborar=?, frecuencia_credito_pos=?, cuota_credito_pos=?
                WHERE id=?
            ''', (
                  codigo, nombre, primer_nombre, segundo_nombre, tercer_nombre,
                  apellido, primer_apellido, segundo_apellido, estado_civil, apellido_casada,
                  dpi, telefono, email, direccion, departamento, municipio,
                  'Asociado', fecha_ingreso_cooperativa, frecuencia, cuota_ahorro, cuota_aportacion, cuota_inscripcion, tipo_ahorro,
                  nit, resumen_beneficiarios(beneficiarios), finca,
                  banco_nombre, banco_tipo_cuenta, banco_numero_cuenta, ruta_foto,
                  salario_val, fecha_ingreso_laborar, frecuencia_credito_pos, cuota_credito_pos, sid))
            
            # Apertura automática de cuentas si no existen
            asegurar_cuentas_socio(conn, sid)

            db_execute(conn, 'DELETE FROM socio_beneficiarios WHERE socio_id=?', [sid])
            if beneficiarios:
                db_executemany(
                    conn,
                    'INSERT INTO socio_beneficiarios (socio_id, nombre, parentesco, porcentaje) VALUES (?, ?, ?, ?)',
                    [(sid, item['nombre'], item['parentesco'], item['porcentaje']) for item in beneficiarios]
                )
            conn.commit()

            if (foto and foto.filename and datos_previos.get('foto') and
                    datos_previos.get('foto').startswith('uploads/socios/') and
                    datos_previos.get('foto') != ruta_foto):
                try:
                    os.remove(os.path.join(os.path.dirname(__file__), 'static', datos_previos.get('foto')))
                except OSError:
                    pass

            datos_nuevos = {
                'codigo': codigo,
                'nombre': nombre,
                'apellido': apellido,
                'primer_nombre': primer_nombre,
                'segundo_nombre': segundo_nombre,
                'tercer_nombre': tercer_nombre,
                'primer_apellido': primer_apellido,
                'segundo_apellido': segundo_apellido,
                'estado_civil': estado_civil,
                'apellido_casada': apellido_casada,
                'dpi': dpi,
                'telefono': telefono,
                'email': email,
                'direccion': direccion,
                'departamento': departamento,
                'municipio': municipio,
                'rol': 'Asociado',
                'fecha_ingreso': fecha_ingreso_cooperativa,
                'frecuencia': frecuencia,
                'frecuencia_credito_pos': frecuencia_credito_pos,
                'cuota_credito_pos': cuota_credito_pos,
                'cuota_ahorro': cuota_ahorro,
                'cuota_aportacion': cuota_aportacion,
                'cuota_inscripcion': cuota_inscripcion,
                'tipo_ahorro': tipo_ahorro,
                'nit': nit,
                'beneficiario': resumen_beneficiarios(beneficiarios),
                'finca': finca,
                'banco_nombre': banco_nombre,
                'banco_tipo_cuenta': banco_tipo_cuenta,
                'banco_numero_cuenta': banco_numero_cuenta,
                'beneficiarios': beneficiarios,
                'salario': salario_val,
                'fecha_ingreso_laborar': fecha_ingreso_laborar,
                'foto': ruta_foto
            }
            log_auditoria_socio(sid, session.get('user_id'), 'editar', 
                                json.dumps(datos_previos), 
                                json.dumps(datos_nuevos)
            )

            flash('Socio actualizado correctamente.', 'success')
            return redirect(url_for('socios.detalle_socio', sid=sid))
        except Exception as e:
            flash(f'Error actualizando socio: {e}', 'danger')
        finally:
            conn.close()

    conn.close()
    return render_template('editar_socio.html', socio=socio_dict, beneficiarios=beneficiarios_existentes)

@bp.route('/socios/<int:sid>/activar', methods=['POST'])
@login_required(role=('Administrador','Operador'))
def activar_socio(sid):
    conn = get_db()
    db_execute(conn, "UPDATE socios SET estado='activo' WHERE id=?", [sid])
    conn.commit()
    conn.close()
    log_auditoria_socio(sid, session.get('user_id'), 'activar', None, 'activo')
    flash('Socio activado.', 'success')
    return redirect(url_for('socios.detalle_socio', sid=sid))

@bp.route('/socios/<int:sid>/inactivar', methods=['POST'])
@login_required(role=('Administrador','Operador'))
def inactivar_socio(sid):
    conn = get_db()
    db_execute(conn, "UPDATE socios SET estado='inactivo' WHERE id=?", [sid])
    conn.commit()
    conn.close()
    log_auditoria_socio(sid, session.get('user_id'), 'inactivar', None, 'inactivo')
    flash('Socio inactivado.', 'warning')
    return redirect(url_for('socios.detalle_socio', sid=sid))
@bp.route('/api/buscar-asociado')
@login_required()
def api_buscar_asociado():
    codigo = request.args.get('codigo', '').strip().upper()
    if not codigo:
        return jsonify({'success': False, 'error': 'Código requerido'}), 400
    
    conn = get_db()
    socio = db_fetchone(
        conn,
        "SELECT id, nombre, apellido, estado FROM socios WHERE codigo=?",
        [codigo]
    )
    conn.close()
    
    if not socio:
        return jsonify({'success': False, 'error': 'Asociado no encontrado'}), 404
    
    if socio['estado'] != 'activo':
        return jsonify({'success': False, 'error': 'El asociado no está activo', 'socio': dict(socio)}), 403
        
    return jsonify({'success': True, 'socio': dict(socio)})
