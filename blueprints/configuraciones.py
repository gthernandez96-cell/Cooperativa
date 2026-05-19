from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, send_file, current_app
import json, os, csv, math
from datetime import date, datetime, timedelta
from utils.db import get_db, db_fetchone, db_fetchall, db_execute, db_insert_and_get_id, db_executemany, get_system_setting, set_system_setting, ensure_required_configurations, ensure_default_prestamo_categories, ensure_system_settings
from config import DEFAULT_COOPERATIVA_NOMBRE, SYSTEM_SETTINGS_DEFAULTS, REQUIRED_CONFIGURACIONES
from utils.images import allowed_image as allowed_system_image, procesar_foto_cooperativa
from utils.decorators import login_required, permission_required
from utils.helpers import log_auditoria_evento, periodo_cerrado, generar_numero_comprobante, validar_pago_frecuencia, obtener_tipo_cuenta_desde_planilla, tipo_transaccion_label, es_transaccion_positiva, get_config_label
from utils.financial import *

bp = Blueprint('configuraciones', __name__)

@bp.route('/login_test', methods=['GET','POST'])
def login_test():
    if request.method == 'POST':
        user = request.form['username'].strip()
        pwd = request.form['password'].strip()
        return f"POST received: user={user}, pwd={pwd[:10]}..."
    return render_template('login.html')

@bp.route('/configuraciones')
@login_required(role=('Administrador',))
def configuraciones():
    conn = get_db()
    ensure_required_configurations(conn)
    ensure_system_settings(conn)
    ensure_default_prestamo_categories(conn)
    configs = db_fetchall(
        conn,
        """SELECT * FROM configuraciones
           WHERE tipo IN ('ahorro_corriente', 'ahorro_plazo_fijo', 'ahorro_aportacion', 'prestamo')
           ORDER BY CASE tipo
               WHEN 'ahorro_corriente' THEN 1
               WHEN 'ahorro_plazo_fijo' THEN 2
               WHEN 'ahorro_aportacion' THEN 3
               WHEN 'prestamo' THEN 4
               ELSE 99
           END"""
    )
    categorias_prestamo = db_fetchall(
        conn,
        """SELECT * FROM prestamo_categorias
           WHERE estado='activo'
           ORDER BY nombre"""
    )
    cooperativa_nombre = get_system_setting(conn, 'cooperativa_nombre', DEFAULT_COOPERATIVA_NOMBRE)
    cooperativa_foto = get_system_setting(conn, 'cooperativa_foto', '')
    prestamo_finiquito_texto = get_system_setting(
        conn,
        'prestamo_finiquito_texto',
        SYSTEM_SETTINGS_DEFAULTS['prestamo_finiquito_texto']
    )
    retiro_comprobante_texto = get_system_setting(
        conn,
        'retiro_comprobante_texto',
        SYSTEM_SETTINGS_DEFAULTS['retiro_comprobante_texto']
    )
    cooperativa_mision = get_system_setting(conn, 'cooperativa_mision', SYSTEM_SETTINGS_DEFAULTS.get('cooperativa_mision', ''))
    cooperativa_vision = get_system_setting(conn, 'cooperativa_vision', SYSTEM_SETTINGS_DEFAULTS.get('cooperativa_vision', ''))
    cooperativa_principios = get_system_setting(conn, 'cooperativa_principios', SYSTEM_SETTINGS_DEFAULTS.get('cooperativa_principios', ''))
    login_background_image = get_system_setting(conn, 'login_background_image', '')

    conn.close()
    return render_template(
        'configuraciones.html',
        configuraciones=configs,
        categorias_prestamo=categorias_prestamo,
        cooperativa_nombre=cooperativa_nombre,
        cooperativa_foto=cooperativa_foto,
        prestamo_finiquito_texto=prestamo_finiquito_texto,
        retiro_comprobante_texto=retiro_comprobante_texto,
        cooperativa_mision=cooperativa_mision,
        cooperativa_vision=cooperativa_vision,
        cooperativa_principios=cooperativa_principios,
        login_background_image=login_background_image,
    )

@bp.route('/configuraciones/actualizar', methods=['POST'])
@login_required(role=('Administrador',))
def actualizar_configuraciones():
    conn = get_db()
    try:
        hoy = date.today().isoformat()
        ensure_default_prestamo_categories(conn)

        nombre_cooperativa = (request.form.get('cooperativa_nombre') or DEFAULT_COOPERATIVA_NOMBRE).strip()
        if not nombre_cooperativa:
            raise ValueError('El nombre de la cooperativa es obligatorio.')
        set_system_setting(conn, 'cooperativa_nombre', nombre_cooperativa, session.get('username'))
        set_system_setting(
            conn,
            'prestamo_finiquito_texto',
            (request.form.get('prestamo_finiquito_texto') or SYSTEM_SETTINGS_DEFAULTS['prestamo_finiquito_texto']).strip(),
            session.get('username')
        )
        set_system_setting(
            conn,
            'retiro_comprobante_texto',
            (request.form.get('retiro_comprobante_texto') or SYSTEM_SETTINGS_DEFAULTS['retiro_comprobante_texto']).strip(),
            session.get('username')
        )

        # Nuevos campos de identidad
        set_system_setting(conn, 'cooperativa_mision', request.form.get('cooperativa_mision', '').strip(), session.get('username'))
        set_system_setting(conn, 'cooperativa_vision', request.form.get('cooperativa_vision', '').strip(), session.get('username'))
        set_system_setting(conn, 'cooperativa_principios', request.form.get('cooperativa_principios', '').strip(), session.get('username'))

        foto_cooperativa = request.files.get('cooperativa_foto')
        if foto_cooperativa and foto_cooperativa.filename:
            if not allowed_system_image(foto_cooperativa.filename):
                raise ValueError('La foto de la cooperativa debe ser PNG, JPG, JPEG o WEBP.')

            foto_anterior = get_system_setting(conn, 'cooperativa_foto', '')
            nueva_foto = procesar_foto_cooperativa(foto_cooperativa)
            set_system_setting(conn, 'cooperativa_foto', nueva_foto, session.get('username'))

            if foto_anterior:
                ruta_anterior = os.path.join(current_app.static_folder, foto_anterior)
                if os.path.exists(ruta_anterior):
                    try:
                        os.remove(ruta_anterior)
                    except OSError:
                        pass

        # Fondo de login
        foto_bg = request.files.get('login_background_image')
        if foto_bg and foto_bg.filename:
            if not allowed_system_image(foto_bg.filename):
                raise ValueError('La imagen de fondo debe ser PNG, JPG, JPEG o WEBP.')

            bg_anterior = get_system_setting(conn, 'login_background_image', '')
            nueva_bg = procesar_foto_cooperativa(foto_bg) # Reusamos el procesador de fotos
            set_system_setting(conn, 'login_background_image', nueva_bg, session.get('username'))

            if bg_anterior:
                ruta_anterior_bg = os.path.join(current_app.static_folder, bg_anterior)
                if os.path.exists(ruta_anterior_bg):
                    try:
                        os.remove(ruta_anterior_bg)
                    except OSError:
                        pass

        for tipo, tasa_default, descripcion in REQUIRED_CONFIGURACIONES:
            tasa = float(request.form.get(tipo, tasa_default))
            if tasa < 0:
                raise ValueError(f'La tasa para {get_config_label(tipo)} no puede ser negativa.')

            db_execute(
                conn,
                """INSERT INTO configuraciones
                   (tipo, tasa_interes, descripcion, fecha_actualizacion, usuario_actualizacion)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(tipo) DO UPDATE SET
                       tasa_interes=excluded.tasa_interes,
                       descripcion=excluded.descripcion,
                       fecha_actualizacion=excluded.fecha_actualizacion,
                       usuario_actualizacion=excluded.usuario_actualizacion""",
                (tipo, tasa, descripcion, hoy, session.get('username'))
            )

        categoria_ids = request.form.getlist('categoria_id[]')
        categoria_nombres = request.form.getlist('categoria_nombre[]')
        categoria_descripciones = request.form.getlist('categoria_descripcion[]')

        for categoria_id, nombre, descripcion in zip(categoria_ids, categoria_nombres, categoria_descripciones):
            nombre = (nombre or '').strip()
            descripcion = (descripcion or '').strip()
            if not nombre:
                continue

            existente = db_fetchone(
                conn,
                "SELECT id FROM prestamo_categorias WHERE lower(nombre)=lower(?) AND id != COALESCE(?, 0)",
                (nombre, categoria_id or None)
            )
            if existente:
                raise ValueError(f'La categoria de prestamo "{nombre}" ya existe.')

            if categoria_id:
                db_execute(
                    conn,
                    """UPDATE prestamo_categorias
                       SET nombre=?, descripcion=?, fecha_actualizacion=?, usuario_actualizacion=?
                       WHERE id=?""",
                    (nombre, descripcion, hoy, session.get('username'), categoria_id)
                )
            else:
                db_execute(
                    conn,
                    """INSERT INTO prestamo_categorias
                       (nombre, descripcion, estado, fecha_actualizacion, usuario_actualizacion)
                       VALUES (?, ?, 'activo', ?, ?)""",
                    (nombre, descripcion, hoy, session.get('username'))
                )
        
        conn.commit()
        flash('Configuraciones actualizadas correctamente.', 'success')
    except Exception as e:
        flash(f'Error actualizando configuraciones: {e}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('configuraciones.configuraciones'))

@bp.route('/auditoria_eventos')
@login_required(role=('Administrador',))
def auditoria_eventos():
    modulo = request.args.get('modulo', '').strip().lower()
    entidad = request.args.get('entidad', '').strip().lower()
    usuario = request.args.get('usuario', '').strip().lower()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()

    query = 'SELECT * FROM auditoria_eventos WHERE 1=1'
    params = []

    if modulo:
        query += ' AND lower(modulo) = ?'
        params.append(modulo)
    if entidad:
        query += ' AND lower(entidad) LIKE ?'
        params.append(f'%{entidad}%')
    if usuario:
        query += ' AND lower(COALESCE(usuario, "")) LIKE ?'
        params.append(f'%{usuario}%')
    if fecha_desde:
        query += ' AND date(fecha) >= date(?)'
        params.append(fecha_desde)
    if fecha_hasta:
        query += ' AND date(fecha) <= date(?)'
        params.append(fecha_hasta)

    query += ' ORDER BY id DESC LIMIT 500'

    conn = get_db()
    eventos = db_fetchall(conn, query, params)
    conn.close()

    return render_template(
        'auditoria_eventos.html',
        eventos=eventos,
        filtros={
            'modulo': modulo,
            'entidad': request.args.get('entidad', '').strip(),
            'usuario': request.args.get('usuario', '').strip(),
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta,
        }
    )

@bp.route('/cierres_periodo', methods=['GET', 'POST'])
@login_required(role=('Administrador',))
def cierres_periodo():
    conn = get_db()

    if request.method == 'POST':
        modulo = request.form.get('modulo', '').strip()
        fecha_inicio = request.form.get('fecha_inicio', '').strip()
        fecha_fin = request.form.get('fecha_fin', '').strip()
        observaciones = request.form.get('observaciones', '').strip()

        if modulo not in ('ahorro', 'prestamos') or not fecha_inicio or not fecha_fin:
            conn.close()
            flash('Debe completar módulo, fecha inicio y fecha fin.', 'danger')
            return redirect(url_for('configuraciones.cierres_periodo'))

        db_execute(
            conn,
            '''
            INSERT INTO cierres_periodo (modulo, fecha_inicio, fecha_fin, estado, observaciones, usuario, fecha_creacion)
            VALUES (?, ?, ?, 'cerrado', ?, ?, ?)
            ''',
            (modulo, fecha_inicio, fecha_fin, observaciones, session.get('username'), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()

        log_auditoria_evento(
            modulo=modulo,
            entidad='cierre_periodo',
            accion='crear',
            descripcion=f'Cierre de periodo {modulo} {fecha_inicio} a {fecha_fin}',
            datos={'observaciones': observaciones}
        )
        flash('Cierre de periodo registrado correctamente.', 'success')
        return redirect(url_for('configuraciones.cierres_periodo'))

    cierres = db_fetchall(
        conn,
        "SELECT * FROM cierres_periodo ORDER BY id DESC LIMIT 100"
    )
    conn.close()
    return render_template('cierres_periodo.html', cierres=cierres)

@bp.route('/historial_planillas')
@login_required()
def historial_planillas():
    tipo = request.args.get('tipo', 'todos').strip().lower()
    nombre = request.args.get('nombre', '').strip().lower()
    boleta = request.args.get('boleta', '').strip().lower()
    frecuencia = request.args.get('frecuencia', '').strip()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()

    planillas, total_general, total_registros = _obtener_historial_planillas(
        tipo=tipo,
        nombre=nombre,
        boleta=boleta,
        frecuencia=frecuencia,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta
    )

    formato_export = request.args.get('export', '').strip().lower()
    if formato_export == 'csv':
        return _exportar_historial_csv(planillas)
    if formato_export == 'excel':
        return _exportar_historial_excel(planillas)

    filtros = {
        'tipo': tipo,
        'nombre': request.args.get('nombre', '').strip(),
        'boleta': request.args.get('boleta', '').strip(),
        'frecuencia': frecuencia,
        'fecha_desde': fecha_desde,
        'fecha_hasta': fecha_hasta
    }

    return render_template(
        'historial_planillas.html',
        planillas=planillas,
        total_general=total_general,
        total_registros=total_registros,
        filtros=filtros
    )
