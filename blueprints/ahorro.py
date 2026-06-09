from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, send_file
import math
import json
import os
import csv
import io
from datetime import date, datetime, timedelta
from werkzeug.utils import secure_filename
from utils.db import (
    get_db, db_fetchone, db_fetchall, db_execute, 
    db_insert_and_get_id, db_executemany, ensure_system_settings, 
    ensure_module_settings, get_system_setting, set_system_setting
)
from utils.decorators import login_required, permission_required
from utils.nombres import preparar_datos_socio, construir_nombre_completo, construir_apellido_completo, validar_dpi, resumen_beneficiarios
from utils.financial import normalizar_fecha_referencia
from utils.helpers import log_auditoria_evento, periodo_cerrado, generar_numero_comprobante, validar_pago_frecuencia, obtener_mensaje_validacion_frecuencia, obtener_tipo_cuenta_desde_planilla
from config import SYSTEM_SETTINGS_DEFAULTS, AHORRO_SETTINGS_DEFAULTS

bp = Blueprint('ahorro', __name__)

@bp.route('/cuentas')
def cuentas():
    conn = get_db()
    rows = db_fetchall(
        conn,
        '''SELECT c.*, s.nombre||' '||s.apellido as socio
           FROM cuentas c JOIN socios s ON c.socio_id=s.id
           ORDER BY c.id DESC'''
    )
    conn.close()
    return render_template('cuentas.html', cuentas=rows)

@bp.route('/cuentas/nueva', methods=['GET','POST'])
def nueva_cuenta():
    conn = get_db()

    tipos_ahorro = db_fetchall(
        conn,
        """
        SELECT tipo, tasa_interes
        FROM configuraciones
        WHERE tipo IN ('ahorro_aportacion', 'ahorro_corriente', 'ahorro_plazo_fijo', 'ahorro_inscripcion')
        ORDER BY CASE tipo
            WHEN 'ahorro_aportacion' THEN 1
            WHEN 'ahorro_corriente' THEN 2
            WHEN 'ahorro_plazo_fijo' THEN 3
            WHEN 'ahorro_inscripcion' THEN 4
            ELSE 99
        END
        """
    )

    tasas_por_tipo = {row['tipo']: float(row['tasa_interes'] or 0) for row in tipos_ahorro}
    selected_socio_id = request.form.get('socio_id', '').strip()
    selected_producto = request.form.get('producto_ahorro', 'ahorro_aportacion').strip() or 'ahorro_aportacion'

    if request.method == 'POST':
        try:
            if not selected_socio_id:
                raise ValueError('Debe seleccionar un asociado activo.')

            socio = db_fetchone(
                conn,
                "SELECT id, estado FROM socios WHERE id=?",
                [selected_socio_id]
            )
            if not socio or (socio['estado'] or '').lower() != 'activo':
                raise ValueError('Solo se permite abrir cuentas a asociados activos.')

            productos_validos = {'ahorro_aportacion', 'ahorro_corriente', 'ahorro_plazo_fijo', 'ahorro_inscripcion'}
            if selected_producto not in productos_validos:
                raise ValueError('Debe seleccionar un tipo de cuenta válido.')

            cuenta_existente = db_fetchone(
                conn,
                """
                SELECT id, numero
                FROM cuentas
                WHERE socio_id=?
                  AND tipo='ahorro'
                  AND COALESCE(producto_ahorro, 'ahorro_corriente')=?
                LIMIT 1
                """,
                [selected_socio_id, selected_producto]
            )
            if cuenta_existente:
                raise ValueError('El asociado ya tiene una cuenta de ese tipo.')

            count = db_fetchone(conn, "SELECT COUNT(*) FROM cuentas")[0]
            prefijos = {
                'ahorro_aportacion': 'APR',
                'ahorro_corriente': 'COR',
                'ahorro_plazo_fijo': 'PLF',
                'ahorro_inscripcion': 'INS',
            }
            numero = f"{prefijos[selected_producto]}-{count+1:04d}"
            tasa = tasas_por_tipo.get(selected_producto, 0)

            db_execute(
                conn,
                """
                INSERT INTO cuentas (numero, socio_id, tipo, producto_ahorro, saldo, tasa_interes, fecha_apertura)
                VALUES (?, ?, 'ahorro', ?, 0, ?, ?)
                """,
                (numero, selected_socio_id, selected_producto, tasa, date.today().isoformat())
            )
            conn.commit()
            flash('Cuenta creada exitosamente.', 'success')
            return redirect(url_for('ahorro.cuentas'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')

    conn.close()
    return render_template(
        'nueva_cuenta.html',
        tipos_ahorro=tipos_ahorro,
        selected_socio_id=selected_socio_id,
        selected_producto=selected_producto,
    )

@bp.route('/ahorro/aplicar-intereses', methods=['POST'])
@login_required(role='Administrador')
def aplicar_intereses():
    conn = get_db()
    hoy = date.today()
    # Obtenemos cuentas de ahorro activas con saldo y tasa
    cuentas = db_fetchall(
        conn,
        "SELECT id, numero, saldo, tasa_interes FROM cuentas WHERE tipo='ahorro' AND estado='activa' AND saldo > 0 AND tasa_interes > 0"
    )
    
    procesados = 0
    total_interes_bruto = 0
    total_ipf = 0
    
    for c in cuentas:
        # Cálculo Bruto: (Saldo * (Tasa/100)) / 12 meses
        monto_interes_bruto = round(c['saldo'] * (c['tasa_interes'] / 100 / 12), 2)
        
        if monto_interes_bruto > 0:
            # Cálculo IPF (10% sobre el interés generado)
            monto_ipf = round(monto_interes_bruto * 0.10, 2)
            
            # 1. Registrar Crédito de INTERES
            saldo_con_interes = round(c['saldo'] + monto_interes_bruto, 2)
            db_execute(
                conn,
                """INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha)
                   VALUES (?, 'interes', ?, ?, ?, ?)""",
                (c['id'], monto_interes_bruto, saldo_con_interes, f"INTERES - {hoy.strftime('%B %Y')}", datetime.now().isoformat())
            )
            
            # 2. Registrar Débito de IPF (si aplica)
            nuevo_saldo = saldo_con_interes
            if monto_ipf > 0:
                nuevo_saldo = round(saldo_con_interes - monto_ipf, 2)
                db_execute(
                    conn,
                    """INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha)
                       VALUES (?, 'ipf', ?, ?, ?, ?)""",
                    (c['id'], monto_ipf, nuevo_saldo, f"IPF 10% s/Interés - {hoy.strftime('%B %Y')}", datetime.now().isoformat())
                )
            
            # 3. Actualizar Saldo Final en la Cuenta
            db_execute(conn, "UPDATE cuentas SET saldo=? WHERE id=?", (nuevo_saldo, c['id']))
            
            procesados += 1
            total_interes_bruto += monto_interes_bruto
            total_ipf += monto_ipf
            
    conn.commit()
    conn.close()
    
    log_auditoria_evento(
        modulo='ahorro',
        entidad='cuentas',
        entidad_id=None,
        accion='capitalizacion',
        descripcion=f'Capitalización mensual procesada: {procesados} cuentas. Total Interés: Q{total_interes_bruto:.2f}, Total IPF: Q{total_ipf:.2f}',
        datos={'cuentas_procesadas': procesados, 'interes_total': total_interes_bruto, 'ipf_total': total_ipf}
    )
    
    flash(f'Proceso finalizado: Se aplicaron intereses a {procesados} cuentas. (Total Interés: Q{total_interes_bruto:,.2f}, Total IPF: Q{total_ipf:,.2f})', 'success')
    return redirect(url_for('ahorro.configuracion_ahorro'))

@bp.route('/cuentas/<int:cid>')
def detalle_cuenta(cid):
    conn = get_db()
    cuenta = db_fetchone(
        conn,
        '''SELECT c.*, s.nombre||' '||s.apellido as socio,
                  s.codigo as socio_codigo
           FROM cuentas c JOIN socios s ON c.socio_id=s.id
           WHERE c.id=?''',
        [cid]
    )
    if not cuenta:
        conn.close()
        flash('Cuenta no encontrada.', 'danger')
        return redirect(url_for('ahorro.cuentas'))

    per_page = 50
    page = max(1, int(request.args.get('page', 1) or 1))
    total_txn = db_fetchone(conn, "SELECT COUNT(*) FROM transacciones WHERE cuenta_id=?", [cid])[0]
    import math as _math
    total_pages = max(1, _math.ceil(total_txn / per_page))
    offset = (page - 1) * per_page

    txns = db_fetchall(
        conn,
        "SELECT * FROM transacciones WHERE cuenta_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
        [cid, per_page, offset]
    )
    conn.close()
    return render_template(
        'detalle_cuenta.html',
        cuenta=cuenta,
        transacciones=txns,
        total_txn=total_txn,
        page=page,
        total_pages=total_pages,
    )


@bp.route('/cuentas/<int:cid>/cancelar', methods=['GET', 'POST'])
@login_required(role=('Administrador', 'Operador'))
def cancelar_cuenta(cid):
    conn = get_db()
    try:
        cuenta = db_fetchone(
            conn,
            '''SELECT c.*, s.nombre||' '||s.apellido as socio_nombre, s.codigo as socio_codigo
               FROM cuentas c JOIN socios s ON c.socio_id=s.id
               WHERE c.id=?''',
            [cid]
        )
        if not cuenta:
            flash('Cuenta no encontrada.', 'danger')
            return redirect(url_for('ahorro.cuentas'))

        # Validar si el socio tiene préstamos activos
        prestamo_activo = db_fetchone(
            conn,
            "SELECT 1 FROM prestamos WHERE socio_id=? AND estado='aprobado' AND saldo_pendiente > 0 LIMIT 1",
            [cuenta['socio_id']]
        )
        if prestamo_activo:
            flash('No se puede cancelar la cuenta porque el asociado tiene un préstamo vigente con saldo pendiente.', 'danger')
            return redirect(url_for('ahorro.detalle_cuenta', cid=cid))

        if cuenta['estado'] != 'activa':
            flash('Solo se pueden cancelar cuentas en estado activa.', 'warning')
            return redirect(url_for('ahorro.detalle_cuenta', cid=cid))

        if request.method == 'POST':
            descripcion = request.form.get('descripcion', 'Cancelación de cuenta').strip()
            count = db_fetchone(conn, "SELECT COUNT(*) FROM solicitudes_retiro")[0] or 0
            numero = f"CAN-{count + 1:05d}"
            
            db_execute(
                conn,
                '''
                INSERT INTO solicitudes_retiro
                (numero, cuenta_id, socio_id, monto, descripcion, metodo_retiro, destino, fecha_solicitud, estado)
                VALUES (?, ?, ?, ?, ?, 'cheque', 'cancelacion_cuenta', ?, 'pendiente')
                ''',
                (numero, cid, cuenta['socio_id'], cuenta['saldo'], descripcion, date.today().isoformat())
            )
            conn.commit()
            flash('Solicitud de cancelación enviada a Gestiones para su aprobación.', 'success')
            return redirect(url_for('ahorro.detalle_cuenta', cid=cid))

    except Exception as e:
        flash(f'Error al procesar la cancelación: {e}', 'danger')
    finally:
        conn.close()
    return render_template('cancelar_cuenta.html', cuenta=cuenta)


@bp.route('/cuentas/<int:cid>/pdf')
@login_required()
def descargar_pdf_cuenta(cid):
    """Genera y descarga un PDF del estado de cuenta completo."""
    conn = get_db()
    cuenta = db_fetchone(
        conn,
        '''SELECT c.*, s.nombre, s.apellido, s.codigo as socio_codigo, s.dpi, s.direccion
           FROM cuentas c
           JOIN socios s ON c.socio_id = s.id
           WHERE c.id = ?''',
        [cid]
    )
    if not cuenta:
        conn.close()
        flash('Cuenta no encontrada.', 'danger')
        return redirect(url_for('ahorro.cuentas'))

    txns = db_fetchall(conn, "SELECT * FROM transacciones WHERE cuenta_id=? ORDER BY fecha ASC", [cid])
    cooperativa_nombre = get_system_setting(conn, 'cooperativa_nombre', 'Cooperativa')
    conn.close()

    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        import io

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer, pagesize=letter,
            leftMargin=2*cm, rightMargin=2*cm,
            topMargin=2*cm, bottomMargin=2*cm
        )
        styles = getSampleStyleSheet()
        story = []

        # Encabezado
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=16, textColor=colors.HexColor('#003b74'), spaceAfter=4)
        subtitle_style = ParagraphStyle('Sub', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor('#475569'), spaceAfter=12)
        story.append(Paragraph(cooperativa_nombre, title_style))
        story.append(Paragraph('Estado de Cuenta de Ahorro', subtitle_style))
        story.append(Spacer(1, 0.3*cm))

        # Datos de la cuenta
        cuenta_data = [
            ['Número de Cuenta:', cuenta['numero'], 'Titular:', f"{cuenta['nombre']} {cuenta['apellido']}"],
            ['Código Socio:', cuenta['socio_codigo'] or '—', 'Saldo Actual:', f"Q{float(cuenta['saldo'] or 0):,.2f}"],
            ['Tipo de Cuenta:', (cuenta['producto_ahorro'] or cuenta['tipo'] or '').replace('_', ' ').title(), 'Tasa de Interés:', f"{cuenta['tasa_interes']}% Anual"],
            ['Fecha de Apertura:', cuenta['fecha_apertura'] or '—', 'Estado:', (cuenta['estado'] or '').capitalize()],
        ]
        info_table = Table(cuenta_data, colWidths=[4*cm, 5.5*cm, 4*cm, 5.5*cm])
        info_table.setStyle(TableStyle([
            ('FONTSIZE', (0,0), (-1,-1), 9),
            ('TEXTCOLOR', (0,0), (0,-1), colors.HexColor('#475569')),
            ('TEXTCOLOR', (2,0), (2,-1), colors.HexColor('#475569')),
            ('FONTNAME', (1,0), (1,-1), 'Helvetica-Bold'),
            ('FONTNAME', (3,0), (3,-1), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('ROWBACKGROUNDS', (0,0), (-1,-1), [colors.HexColor('#f8fafc'), colors.white]),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e2e8f0')),
            ('BORDERPADDING', (0,0), (-1,-1), 6),
        ]))
        story.append(info_table)
        story.append(Spacer(1, 0.5*cm))

        # Tabla de movimientos
        story.append(Paragraph('Historial de Movimientos', ParagraphStyle('H2', parent=styles['Heading2'], fontSize=12, textColor=colors.HexColor('#003b74'), spaceBefore=8, spaceAfter=6)))

        header = ['Fecha', 'Operación', 'Monto (Q)', 'Saldo (Q)', 'Descripción']
        rows = [header]
        TIPOS_POSITIVOS = {'deposito', 'interes', 'credito'}
        for t in txns:
            tipo_txt = (t['tipo'] or '').capitalize()
            monto = float(t['monto'] or 0)
            saldo = float(t['saldo_despues'] or 0)
            signo = '+' if t['tipo'] in TIPOS_POSITIVOS else '-'
            rows.append([
                str(t['fecha'])[:10],
                tipo_txt,
                f"{signo}Q{monto:,.2f}",
                f"Q{saldo:,.2f}",
                (t['descripcion'] or '—')[:50],
            ])

        if len(rows) > 1:
            col_widths = [3*cm, 3*cm, 3.5*cm, 3.5*cm, None]
            txn_table = Table(rows, colWidths=col_widths, repeatRows=1)
            txn_table.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#003b74')),
                ('TEXTCOLOR', (0,0), (-1,0), colors.white),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTSIZE', (0,0), (-1,-1), 8),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f8fafc')]),
                ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#e2e8f0')),
                ('TOPPADDING', (0,0), (-1,-1), 4),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                ('ALIGN', (2,1), (3,-1), 'RIGHT'),
            ]))
            story.append(txn_table)
        else:
            story.append(Paragraph('Sin movimientos registrados.', styles['Normal']))

        # Resumen final
        story.append(Spacer(1, 0.5*cm))
        total_dep = sum(float(t['monto'] or 0) for t in txns if t['tipo'] in TIPOS_POSITIVOS)
        total_ret = sum(float(t['monto'] or 0) for t in txns if t['tipo'] not in TIPOS_POSITIVOS)
        resumen_data = [
            ['Total Depósitos / Intereses', f"Q{total_dep:,.2f}"],
            ['Total Retiros / Cargos', f"Q{total_ret:,.2f}"],
            ['Saldo Actual', f"Q{float(cuenta['saldo'] or 0):,.2f}"],
        ]
        resumen_table = Table(resumen_data, colWidths=[10*cm, 5*cm])
        resumen_table.setStyle(TableStyle([
            ('FONTSIZE', (0,0), (-1,-1), 9),
            ('FONTNAME', (0,2), (-1,2), 'Helvetica-Bold'),
            ('TEXTCOLOR', (0,2), (-1,2), colors.HexColor('#003b74')),
            ('ALIGN', (1,0), (1,-1), 'RIGHT'),
            ('LINEABOVE', (0,2), (-1,2), 1, colors.HexColor('#003b74')),
            ('TOPPADDING', (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ]))
        story.append(resumen_table)

        doc.build(story)
        buffer.seek(0)
        filename = f"estado_cuenta_{cuenta['numero']}_{date.today().isoformat()}.pdf"
        from flask import send_file
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/pdf'
        )

    except ImportError:
        flash('ReportLab no está instalado. Instale con: pip install reportlab', 'danger')
        return redirect(url_for('ahorro.detalle_cuenta', cid=cid))
    except Exception as e:
        flash(f'Error al generar PDF: {e}', 'danger')
        return redirect(url_for('ahorro.detalle_cuenta', cid=cid))

@bp.route('/cuentas/<int:cid>/imprimir')
def imprimir_estado_cuenta(cid):
    conn = get_db()
    # Obtener datos de la cuenta y socio con más detalle
    cuenta = db_fetchone(
        conn,
        '''SELECT c.*, s.nombre, s.apellido, s.codigo as socio_codigo, s.dpi, s.direccion
           FROM cuentas c 
           JOIN socios s ON c.socio_id = s.id
           WHERE c.id = ?''',
        [cid]
    )
    if not cuenta:
        conn.close()
        flash('Cuenta no encontrada.', 'danger')
        return redirect(url_for('ahorro.cuentas'))
    
    # Obtener historial completo para el reporte
    txns = db_fetchall(conn, "SELECT * FROM transacciones WHERE cuenta_id=? ORDER BY fecha ASC", [cid])
    conn.close()
    
    # Calcular resumen para el reporte
    total_depositos = sum(t['monto'] for t in txns if t['tipo'] in ['deposito', 'interes'])
    total_retiros = sum(t['monto'] for t in txns if t['tipo'] in ['retiro', 'ipf', 'debito'])

    
    return render_template('imprimir_estado_cuenta.html', 
                           cuenta=cuenta, 
                           transacciones=txns,
                           resumen={
                               'depositos': total_depositos,
                               'retiros': total_retiros,
                               'neto': total_depositos - total_retiros
                           },
                           hoy=datetime.now())

@bp.route('/cuentas/<int:cid>/transaccion', methods=['POST'])
def hacer_transaccion(cid):
    conn = get_db()
    cuenta = db_fetchone(conn, "SELECT * FROM cuentas WHERE id=?", [cid])
    socio = db_fetchone(conn, "SELECT id FROM socios WHERE id=?", [cuenta['socio_id']])
    tipo = request.form['tipo']
    monto = float(request.form['monto'])
    desc = request.form.get('descripcion', tipo.capitalize())

    if periodo_cerrado('ahorro', datetime.now().isoformat()):
        conn.close()
        flash('El periodo de ahorro está cerrado. No se permiten movimientos en la fecha actual.', 'warning')
        return redirect(url_for('ahorro.detalle_cuenta', cid=cid))
    
    # Validar frecuencia para depósitos de ahorro
    if tipo == 'deposito':
        val = validar_pago_frecuencia(socio['id'], 'ahorro')
        if not val:
            mensaje = obtener_mensaje_validacion_frecuencia(socio['id'], 'ahorro')
            flash(f'No se puede realizar el depósito. {mensaje}', 'warning')
            conn.close()
            return redirect(url_for('ahorro.detalle_cuenta', cid=cid))
    
    try:
        if tipo == 'retiro' and monto > cuenta['saldo']:
            flash('Saldo insuficiente.', 'danger')
        else:
            nuevo_saldo = cuenta['saldo'] + monto if tipo == 'deposito' else cuenta['saldo'] - monto
            db_execute(conn, "UPDATE cuentas SET saldo=? WHERE id=?", [nuevo_saldo, cid])
            db_execute(
                conn,
                "INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)",
                (cid, tipo, monto, nuevo_saldo, desc, datetime.now().isoformat())
            )
            conn.commit()
            log_auditoria_evento(
                modulo='ahorro',
                entidad='transaccion',
                entidad_id=cid,
                accion='crear',
                descripcion=f'Transaccion {tipo} registrada en cuenta {cuenta["numero"]}',
                datos={'monto': monto, 'saldo_despues': nuevo_saldo}
            )
            flash('Transacción realizada.', 'success')
    except Exception as e:
        flash(f'Error: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('ahorro.detalle_cuenta', cid=cid))

@bp.route('/menu_ahorro')
@login_required()
def menu_ahorro():
    conn = get_db()
    stats = db_fetchone(
        conn,
        """
        SELECT 
            COUNT(*) as total_cuentas,
            COALESCE(SUM(saldo), 0) as saldo_total,
            COALESCE((SELECT SUM(monto) FROM transacciones WHERE tipo='interes'), 0) as intereses_pagados
        FROM cuentas 
        WHERE tipo='ahorro' AND estado='activa'
        """
    )
    conn.close()
    return render_template('menu_ahorro.html', stats=stats)

@bp.route('/gestiones/retiro')
@login_required(role=('Administrador', 'Operador'))
def gestion_retiro():
    conn = get_db()
    cuentas = db_fetchall(
        conn,
        '''
        SELECT c.id,
               c.numero,
               c.saldo,
             c.socio_id,
               c.producto_ahorro,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre,
               s.banco_nombre,
               s.banco_tipo_cuenta,
               s.banco_numero_cuenta
        FROM cuentas c
        JOIN socios s ON s.id = c.socio_id
        WHERE c.tipo='ahorro' AND c.estado='activa' AND s.estado='activo'
        ORDER BY s.codigo, c.numero
        '''
    )
    prestamos_vigentes = db_fetchall(
        conn,
        '''
        SELECT p.id,
               p.socio_id,
               p.numero,
               COALESCE(p.saldo_pendiente, 0) AS saldo_pendiente
        FROM prestamos p
        WHERE p.estado='aprobado' AND COALESCE(p.saldo_pendiente, 0) > 0
        ORDER BY p.numero
        '''
    )
    conn.close()
    return render_template('nuevo_retiro.html', cuentas=cuentas, prestamos_vigentes=prestamos_vigentes)

@bp.route('/gestiones/retiro/nuevo', methods=['POST'])
@login_required(role=('Administrador', 'Operador'))
def nueva_solicitud_retiro():
    conn = get_db()
    cuenta_id = (request.form.get('cuenta_id') or '').strip()
    monto_raw = (request.form.get('monto') or '').strip()
    descripcion = (request.form.get('descripcion') or 'Retiro solicitado desde modulo gestiones').strip()
    metodo_retiro = (request.form.get('metodo_retiro') or '').strip().lower()
    destino = (request.form.get('destino') or 'retiro').strip().lower()
    prestamo_id_raw = (request.form.get('prestamo_id') or '').strip()
    boleta_numero = (request.form.get('boleta_numero') or '').strip()
    boleta_fecha = (request.form.get('boleta_fecha') or '').strip()
    banco_tipo_cuenta = ''
    banco_numero_cuenta = ''
    prestamo_id = None

    cuentas = db_fetchall(
        conn,
        '''
        SELECT c.id,
               c.numero,
               c.saldo,
             c.socio_id,
               c.producto_ahorro,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre,
               s.banco_nombre,
               s.banco_tipo_cuenta,
               s.banco_numero_cuenta
        FROM cuentas c
        JOIN socios s ON s.id = c.socio_id
        WHERE c.tipo='ahorro' AND c.estado='activa' AND s.estado='activo'
        ORDER BY s.codigo, c.numero
        '''
    )

    try:
        if not cuenta_id.isdigit():
            raise ValueError('Debe seleccionar una cuenta de ahorro válida.')
        monto = float(monto_raw)
        if monto <= 0:
            raise ValueError('El monto debe ser mayor a cero.')
        if metodo_retiro not in ('cheque', 'deposito'):
            raise ValueError('Debe seleccionar una forma de retiro válida.')
        if destino not in ('retiro', 'amortizacion_prestamo'):
            raise ValueError('Debe seleccionar un destino válido para la solicitud.')

        cuenta = db_fetchone(
            conn,
            '''
            SELECT c.id, c.numero, c.saldo, c.socio_id,
                   s.estado AS socio_estado,
                   s.banco_nombre,
                   s.banco_tipo_cuenta,
                   s.banco_numero_cuenta
            FROM cuentas c
            JOIN socios s ON s.id = c.socio_id
            WHERE c.id=?
            ''',
            [int(cuenta_id)],
        )
        if not cuenta or (cuenta['socio_estado'] or '').lower() != 'activo':
            raise ValueError('La cuenta seleccionada no está disponible para retiro.')

        if monto > float(cuenta['saldo'] or 0):
            raise ValueError('El monto solicitado excede el saldo disponible de la cuenta.')

        if destino == 'amortizacion_prestamo':
            if not prestamo_id_raw.isdigit():
                raise ValueError('Debe seleccionar un préstamo vigente para amortizar.')
            prestamo = db_fetchone(
                conn,
                '''
                SELECT id,
                       socio_id,
                       numero,
                       COALESCE(saldo_pendiente, 0) AS saldo_pendiente,
                       estado
                FROM prestamos
                WHERE id=?
                ''',
                [int(prestamo_id_raw)],
            )
            if not prestamo:
                raise ValueError('El préstamo seleccionado no existe.')
            if int(prestamo['socio_id']) != int(cuenta['socio_id']):
                raise ValueError('El préstamo seleccionado no pertenece al titular de la cuenta.')
            if (prestamo['estado'] or '').lower() != 'aprobado' or float(prestamo['saldo_pendiente'] or 0) <= 0:
                raise ValueError('El préstamo seleccionado ya no está vigente.')
            if float(cuenta['saldo'] or 0) < float(prestamo['saldo_pendiente'] or 0):
                raise ValueError('El saldo de ahorro de la cuenta seleccionada debe ser mayor o igual al saldo pendiente del préstamo a amortizar.')
            if monto > float(prestamo['saldo_pendiente'] or 0):
                raise ValueError('El monto solicitado excede el saldo pendiente del préstamo a amortizar.')
            prestamo_id = int(prestamo['id'])

        if metodo_retiro == 'deposito':
            banco_tipo_cuenta = (cuenta['banco_tipo_cuenta'] or '').strip()
            banco_numero_cuenta = (cuenta['banco_numero_cuenta'] or '').strip()
            banco_nombre = (cuenta['banco_nombre'] or '').strip()
            if not banco_nombre or not banco_tipo_cuenta or not banco_numero_cuenta:
                raise ValueError('El asociado no tiene datos bancarios completos. Actualice banco, tipo y número de cuenta en el perfil del socio.')
        else:
            banco_tipo_cuenta = ''
            banco_numero_cuenta = ''

        count = db_fetchone(conn, "SELECT COUNT(*) FROM solicitudes_retiro")[0] or 0
        numero = f'RET-{count + 1:05d}'

        db_execute(
            conn,
            '''
            INSERT INTO solicitudes_retiro
            (numero, cuenta_id, socio_id, monto, descripcion, metodo_retiro, banco_tipo_cuenta, banco_numero_cuenta, destino, prestamo_id, boleta_numero, boleta_fecha, fecha_solicitud, estado)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pendiente')
            ''',
            (
                numero,
                cuenta['id'],
                cuenta['socio_id'],
                monto,
                descripcion,
                metodo_retiro,
                banco_tipo_cuenta,
                banco_numero_cuenta,
                destino,
                prestamo_id,
                boleta_numero,
                boleta_fecha,
                date.today().isoformat(),
            ),
        )
        conn.commit()
        flash('Solicitud de retiro enviada correctamente.', 'success')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))
    except Exception as e:
        flash(f'Error: {e}', 'danger')
        return render_template('nuevo_retiro.html', cuentas=cuentas)
    finally:
        conn.close()


@bp.route('/gestiones/deposito')
@login_required(role=('Administrador', 'Operador'))
def gestion_deposito():
    conn = get_db()
    cuentas = db_fetchall(
        conn,
        '''
        SELECT c.id,
               c.numero,
               c.saldo,
               c.socio_id,
               c.producto_ahorro,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre
        FROM cuentas c
        JOIN socios s ON s.id = c.socio_id
        WHERE c.tipo='ahorro' AND c.estado='activa' AND s.estado='activo'
        ORDER BY s.codigo, c.numero
        '''
    )
    conn.close()
    return render_template('nuevo_deposito.html', cuentas=cuentas)


@bp.route('/gestiones/deposito/nuevo', methods=['POST'])
@login_required(role=('Administrador', 'Operador'))
def nueva_solicitud_deposito():
    conn = get_db()
    cuenta_id = (request.form.get('cuenta_id') or '').strip()
    monto_raw = (request.form.get('monto') or '').strip()
    descripcion = (request.form.get('descripcion') or 'Depósito solicitado desde modulo gestiones').strip()
    metodo_pago = (request.form.get('metodo_pago') or 'deposito').strip().lower()
    boleta_numero = (request.form.get('boleta_numero') or '').strip()
    boleta_fecha = (request.form.get('boleta_fecha') or '').strip()

    cuentas = db_fetchall(
        conn,
        '''
        SELECT c.id,
               c.numero,
               c.saldo,
               c.socio_id,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre
        FROM cuentas c
        JOIN socios s ON s.id = c.socio_id
        WHERE c.tipo='ahorro' AND c.estado='activa' AND s.estado='activo'
        ORDER BY s.codigo, c.numero
        '''
    )

    try:
        if not cuenta_id or not monto_raw:
            raise ValueError('La cuenta y el monto son obligatorios.')

        monto = float(monto_raw)
        if monto <= 0:
            raise ValueError('El monto debe ser mayor a cero.')

        cuenta = db_fetchone(conn, "SELECT id, socio_id FROM cuentas WHERE id=?", [cuenta_id])
        if not cuenta:
            raise ValueError('La cuenta seleccionada no existe.')

        count = db_fetchone(conn, "SELECT COUNT(*) FROM solicitudes_retiro")[0] or 0
        numero = f'DEP-{count + 1:05d}'

        db_execute(
            conn,
            '''
            INSERT INTO solicitudes_retiro
            (numero, cuenta_id, socio_id, monto, descripcion, metodo_retiro, destino, boleta_numero, boleta_fecha, fecha_solicitud, estado)
            VALUES (?, ?, ?, ?, ?, ?, 'deposito', ?, ?, ?, 'pendiente')
            ''',
            (
                numero,
                cuenta['id'],
                cuenta['socio_id'],
                monto,
                descripcion,
                metodo_pago,
                boleta_numero,
                boleta_fecha,
                date.today().isoformat(),
            ),
        )
        conn.commit()
        flash('Solicitud de depósito enviada correctamente.', 'success')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))
    except Exception as e:
        flash(f'Error: {e}', 'danger')
        return render_template('nuevo_deposito.html', cuentas=cuentas)
    finally:
        conn.close()


@bp.route('/gestiones/retiro/<int:rid>/aprobar', methods=['POST'])
@login_required(role=('Administrador', 'Operador'))
def aprobar_solicitud_retiro(rid):
    conn = get_db()
    solicitud = db_fetchone(
        conn,
        '''
        SELECT sr.*, c.numero AS cuenta_numero, c.saldo,
               p.numero AS prestamo_numero,
               COALESCE(p.saldo_pendiente, 0) AS prestamo_saldo_pendiente,
               p.estado AS prestamo_estado
        FROM solicitudes_retiro sr
        JOIN cuentas c ON c.id = sr.cuenta_id
        LEFT JOIN prestamos p ON p.id = sr.prestamo_id
        WHERE sr.id=?
        ''',
        [rid],
    )

    if not solicitud:
        conn.close()
        flash('Solicitud de retiro no encontrada.', 'danger')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

    if (solicitud['estado'] or '').lower() != 'pendiente':
        conn.close()
        flash('Solo se pueden aprobar solicitudes en estado pendiente.', 'warning')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

    monto = float(solicitud['monto'] or 0)
    saldo_actual = float(solicitud['saldo'] or 0)
    destino = (solicitud['destino'] or 'retiro').lower()

    if destino != 'deposito' and monto > saldo_actual:
        conn.close()
        flash('La solicitud no se puede aprobar porque el saldo actual es insuficiente.', 'danger')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

    if destino == 'amortizacion_prestamo':
        if not solicitud['prestamo_id']:
            conn.close()
            flash('La solicitud está marcada para amortización, pero no tiene préstamo asociado.', 'danger')
            return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))
        if (solicitud['prestamo_estado'] or '').lower() != 'aprobado' or float(solicitud['prestamo_saldo_pendiente'] or 0) <= 0:
            conn.close()
            flash('El préstamo asociado ya no está vigente. No se pudo aprobar la solicitud.', 'danger')
            return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))
        if saldo_actual < float(solicitud['prestamo_saldo_pendiente'] or 0):
            conn.close()
            flash('El saldo actual de ahorro debe ser mayor o igual al saldo pendiente del préstamo para aplicar amortización.', 'danger')
            return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))
        if monto > float(solicitud['prestamo_saldo_pendiente'] or 0):
            conn.close()
            flash('El monto solicitado supera el saldo pendiente actual del préstamo asociado.', 'danger')
            return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

    if destino == 'cancelacion_cuenta':
        # Re-validar que el socio no posea préstamos vigentes
        prestamo_activo = db_fetchone(
            conn,
            "SELECT 1 FROM prestamos WHERE socio_id=? AND estado='aprobado' AND saldo_pendiente > 0 LIMIT 1",
            [solicitud['socio_id']]
        )
        if prestamo_activo:
            conn.close()
            flash('No se puede aprobar la cancelación porque el asociado tiene un préstamo vigente con saldo pendiente.', 'danger')
            return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

        monto = saldo_actual
        nuevo_saldo = 0.0
        db_execute(conn, "UPDATE cuentas SET saldo=?, estado='cancelada' WHERE id=?", [nuevo_saldo, solicitud['cuenta_id']])
        db_execute(
            conn,
            '''
            INSERT INTO transacciones
            (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha, metodo_pago, boleta_numero, boleta_fecha)
            VALUES (?, 'retiro', ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                solicitud['cuenta_id'],
                monto,
                nuevo_saldo,
                f"Liquidación por cancelación: {solicitud['descripcion'] or 'Cancelación de cuenta'}",
                datetime.now().isoformat(),
                solicitud['metodo_retiro'] or 'cheque',
                solicitud['boleta_numero'],
                solicitud['boleta_fecha']
            )
        )
    elif destino == 'deposito':
        nuevo_saldo = saldo_actual + monto
        db_execute(conn, "UPDATE cuentas SET saldo=? WHERE id=?", [nuevo_saldo, solicitud['cuenta_id']])
        db_execute(
            conn,
            '''
            INSERT INTO transacciones
            (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha, metodo_pago, boleta_numero, boleta_fecha)
            VALUES (?, 'deposito', ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                solicitud['cuenta_id'],
                monto,
                nuevo_saldo,
                solicitud['descripcion'] or 'Depósito aprobado desde modulo gestiones',
                datetime.now().isoformat(),
                solicitud['metodo_retiro'] or 'deposito',
                solicitud['boleta_numero'],
                solicitud['boleta_fecha']
            )
        )
    else:
        # Retiro normal u amortización
        nuevo_saldo = saldo_actual - monto
        db_execute(conn, "UPDATE cuentas SET saldo=? WHERE id=?", [nuevo_saldo, solicitud['cuenta_id']])
        db_execute(
            conn,
            '''
            INSERT INTO transacciones
            (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha, metodo_pago, boleta_numero, boleta_fecha)
            VALUES (?, 'retiro', ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                solicitud['cuenta_id'],
                monto,
                nuevo_saldo,
                solicitud['descripcion'] or 'Retiro aprobado desde modulo gestiones',
                datetime.now().isoformat(),
                solicitud['metodo_retiro'] or 'cheque',
                solicitud['boleta_numero'],
                solicitud['boleta_fecha']
            )
        )

        if destino == 'amortizacion_prestamo':
            prestamo_saldo_actual = float(solicitud['prestamo_saldo_pendiente'] or 0)
            nuevo_saldo_prestamo = round(max(0, prestamo_saldo_actual - monto), 2)
            estado_prestamo = 'pagado' if nuevo_saldo_prestamo == 0 else 'aprobado'
            numero_comprobante = generar_numero_comprobante(conn)
            db_execute(
                conn,
                '''
                INSERT INTO pagos_prestamo
                (prestamo_id, monto, capital, interes, saldo_restante, descripcion, boleta_deposito, fecha, numero_comprobante)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    solicitud['prestamo_id'],
                    monto,
                    monto,
                    0,
                    nuevo_saldo_prestamo,
                    f"Amortización desde solicitud de retiro {solicitud['numero']}",
                    solicitud['numero'],
                    date.today().isoformat(),
                    numero_comprobante,
                ),
            )
            db_execute(
                conn,
                "UPDATE prestamos SET saldo_pendiente=?, estado=? WHERE id=?",
                [nuevo_saldo_prestamo, estado_prestamo, solicitud['prestamo_id']]
            )

    db_execute(
        conn,
        "UPDATE solicitudes_retiro SET estado='aprobado', fecha_aprobacion=?, aprobado_por=? WHERE id=?",
        [date.today().isoformat(), session.get('username'), rid],
    )
    conn.commit()
    conn.close()

    if destino == 'deposito':
        flash('Depósito realizado con éxito.', 'success')
    elif destino == 'cancelacion_cuenta':
        flash('Cuenta cancelada y saldo liquidado con éxito.', 'success')
    else:
        flash('Retiro realizado con éxito.', 'success')
    return redirect(url_for('ahorro.comprobante_retiro', rid=rid, auto_print='1'))

@bp.route('/gestiones/retiro/<int:rid>/comprobante')
@login_required(role=('Administrador', 'Operador'))
def comprobante_retiro(rid):
    conn = get_db()
    retiro = db_fetchone(
        conn,
        '''
        SELECT sr.*, c.numero AS cuenta_numero,
               s.codigo AS socio_codigo,
               s.nombre || ' ' || s.apellido AS socio_nombre,
               p.numero AS prestamo_numero
        FROM solicitudes_retiro sr
        JOIN cuentas c ON c.id = sr.cuenta_id
        JOIN socios s ON s.id = sr.socio_id
        LEFT JOIN prestamos p ON p.id = sr.prestamo_id
        WHERE sr.id=?
        ''',
        (rid,),
    )
    conn.close()

    if not retiro:
        flash('Comprobante de retiro no encontrado.', 'danger')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

    if (retiro['estado'] or '').lower() != 'aprobado':
        flash('El comprobante solo esta disponible para retiros aprobados.', 'warning')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

    auto_print = (request.args.get('auto_print') or '').strip() == '1'
    return render_template('comprobante_retiro.html', retiro=retiro, auto_print=auto_print)

@bp.route('/gestiones/retiro/<int:rid>/no-procede', methods=['POST'])
@login_required(role=('Administrador', 'Operador'))
def marcar_solicitud_retiro_no_procede(rid):
    conn = get_db()
    solicitud = db_fetchone(
        conn,
        "SELECT id, numero, estado FROM solicitudes_retiro WHERE id=?",
        [rid],
    )

    if not solicitud:
        conn.close()
        flash('Solicitud de retiro no encontrada.', 'danger')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

    if (solicitud['estado'] or '').lower() != 'pendiente':
        conn.close()
        flash('Solo se pueden marcar como no procede las solicitudes pendientes.', 'warning')
        return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

    db_execute(
        conn,
        "UPDATE solicitudes_retiro SET estado='no_procede', fecha_aprobacion=?, aprobado_por=? WHERE id=?",
        [date.today().isoformat(), session.get('username'), rid],
    )
    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='ahorro',
        entidad='solicitud_retiro',
        entidad_id=rid,
        accion='actualizar',
        descripcion=f'Solicitud de retiro {solicitud["numero"]} marcada como no procede',
        datos={'estado': 'no_procede'}
    )

    flash('La solicitud de retiro se marco como no procede.', 'success')
    return redirect(url_for('prestamos.gestiones', tipo='retiro', estado='pendiente'))

@bp.route('/configuracion_ahorro', methods=['GET', 'POST'])
@login_required()
@permission_required('config.ahorro')
def configuracion_ahorro():
    conn = get_db()
    try:
        ensure_system_settings(conn)
        ensure_module_settings(conn)

        if request.method == 'POST':
            campos = list(AHORRO_SETTINGS_DEFAULTS.keys())
            actualizados = 0

            for clave in campos:
                if clave not in request.form:
                    continue
                valor = (request.form.get(clave) or '').strip()
                if not valor:
                    continue
                set_system_setting(conn, clave, valor, session.get('username'))
                actualizados += 1

            conn.commit()
            if actualizados:
                flash('Configuracion de ahorro actualizada correctamente.', 'success')
            else:
                flash('No se recibieron cambios para guardar.', 'warning')
            return redirect(url_for('ahorro.configuracion_ahorro'))

        ahorro_cfg = {
            clave: get_system_setting(conn, clave, valor_default)
            for clave, valor_default in AHORRO_SETTINGS_DEFAULTS.items()
        }
        return render_template('configuracion_ahorro.html', ahorro_cfg=ahorro_cfg)
    except Exception as e:
        flash(f'Error cargando configuracion de ahorro: {e}', 'danger')
        return render_template('configuracion_ahorro.html', ahorro_cfg=AHORRO_SETTINGS_DEFAULTS)
    finally:
        conn.close()

@bp.route('/planilla_retiros_ahorro')
@login_required()
def planilla_retiros_ahorro():
    fecha_actual = date.today().isoformat()
    return render_template('planilla_retiros_ahorro.html', fecha_actual=fecha_actual)

@bp.route('/planilla_transferencias_ahorro')
@login_required()
def planilla_transferencias_ahorro():
    fecha_actual = date.today().isoformat()
    return render_template('planilla_transferencias_ahorro.html', fecha_actual=fecha_actual)

@bp.route('/reportes_ahorro')
@login_required()
def reportes_ahorro():
    fecha_actual = date.today().isoformat()
    fecha_mes_anterior = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1).isoformat()
    return render_template('reportes_ahorro.html', fecha_actual=fecha_actual, fecha_mes_anterior=fecha_mes_anterior)

@bp.route('/validar_retiros_ahorro', methods=['POST'])
@login_required()
def validar_retiros_ahorro():
    try:
        file = request.files.get('archivo')
        if not file:
            return jsonify({'success': False, 'error': 'Archivo no encontrado'}), 400

        filas = _leer_archivo_masivo(file, ['numero_cuenta', 'monto_retiro', 'descripcion'])
        total = len(filas)
        validos = 0
        errores = 0
        monto_total = 0.0
        errores_detalle = []
        datos_validos = []

        conn = get_db()
        for i, fila in enumerate(filas, start=1):
            numero = fila.get('numero_cuenta') or fila.get('numero')
            monto = fila.get('monto_retiro') or fila.get('monto')
            descripcion = fila.get('descripcion', '') or ''

            if not numero:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Falta número de cuenta'})
                continue

            try:
                monto = float(monto)
            except Exception:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Monto no válido'})
                continue

            if monto <= 0:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Monto debe ser mayor que 0'})
                continue

            cuenta = db_fetchone(conn, 'SELECT * FROM cuentas WHERE numero=? AND tipo="ahorro" AND estado="activa"', (numero,))
            if not cuenta:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Cuenta no encontrada o no activa'})
                continue

            if monto > cuenta['saldo']:
                errores += 1
                errores_detalle.append({'fila': i, 'numero_cuenta': numero, 'error': 'Saldo insuficiente'})
                continue

            validos += 1
            monto_total += monto
            datos_validos.append({'cuenta_id': cuenta['id'], 'numero_cuenta': numero, 'monto': monto, 'descripcion': descripcion})

        conn.close()

        return jsonify({
            'success': True,
            'total_registros': total,
            'validos': validos,
            'errores': errores,
            'monto_total': monto_total,
            'errores_detalle': errores_detalle,
            'datos_validos': datos_validos
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/procesar_retiros_ahorro', methods=['POST'])
@login_required()
@permission_required('ahorro.masivo')
def procesar_retiros_ahorro():
    data = request.get_json() or {}
    retiros = data.get('retiros', [])
    fecha_retiro = data.get('fecha_retiro', date.today().isoformat())

    if not retiros:
        return jsonify({'success': False, 'error': 'No hay retiros para procesar'}), 400

    conn = get_db()
    procesados = 0
    monto_total = 0.0
    errores = []

    for retiro in retiros:
        try:
            cuenta_id = retiro['cuenta_id']
            monto = float(retiro['monto'])
            descripcion = retiro.get('descripcion', 'Retiro masivo')

            cuenta = db_fetchone(conn, 'SELECT saldo FROM cuentas WHERE id=? AND tipo="ahorro" AND estado="activa"', (cuenta_id,))
            if not cuenta:
                errores.append(f'Cuenta {cuenta_id} no encontrada')
                continue

            if monto <= 0 or monto > cuenta['saldo']:
                errores.append(f'Monto inválido para cuenta {cuenta_id}')
                continue

            nuevo_saldo = cuenta['saldo'] - monto
            db_execute(conn, 'UPDATE cuentas SET saldo=? WHERE id=?', (nuevo_saldo, cuenta_id))
            db_execute(
                conn,
                'INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)',
                (cuenta_id, 'retiro', monto, nuevo_saldo, descripcion, fecha_retiro)
            )
            procesados += 1
            monto_total += monto
        except Exception as e:
            errores.append(f'Cuenta {retiro.get("numero_cuenta", cuenta_id)}: {str(e)}')

    conn.commit()
    conn.close()

    return jsonify({'success': True, 'procesados': procesados, 'monto_total': monto_total, 'errores': errores})

@bp.route('/validar_transferencias_ahorro', methods=['POST'])
@login_required()
def validar_transferencias_ahorro():
    try:
        file = request.files.get('archivo')
        if not file:
            return jsonify({'success': False, 'error': 'Archivo no encontrado'}), 400

        filas = _leer_archivo_masivo(file, ['cuenta_origen', 'cuenta_destino', 'monto_transferencia', 'descripcion'])
        total = len(filas)
        validos = 0
        errores = 0
        monto_total = 0.0
        errores_detalle = []
        datos_validos = []
        resumen_transferencias = []

        conn = get_db()

        for i, fila in enumerate(filas, start=1):
            origen = fila.get('cuenta_origen')
            destino = fila.get('cuenta_destino')
            monto = fila.get('monto_transferencia') or fila.get('monto')
            descripcion = fila.get('descripcion', '') or ''

            if not origen or not destino:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Faltan cuentas origen o destino'})
                continue

            if origen == destino:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Origen y destino deben ser diferentes'})
                continue

            try:
                monto = float(monto)
            except Exception:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Monto no válido'})
                continue

            if monto <= 0:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Monto debe ser mayor que 0'})
                continue

            c_origen = db_fetchone(conn, 'SELECT * FROM cuentas WHERE numero=? AND tipo="ahorro" AND estado="activa"', (origen,))
            c_destino = db_fetchone(conn, 'SELECT * FROM cuentas WHERE numero=? AND tipo="ahorro" AND estado="activa"', (destino,))

            if not c_origen or not c_destino:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Cuenta origen/destino no existe o no está activa'})
                continue

            if monto > c_origen['saldo']:
                errores += 1
                errores_detalle.append({'fila': i, 'cuenta_origen': origen, 'cuenta_destino': destino, 'error': 'Saldo insuficiente en origen'})
                continue

            validos += 1
            monto_total += monto
            datos_validos.append({'cuenta_origen': c_origen['id'], 'cuenta_destino': c_destino['id'], 'monto': monto, 'descripcion': descripcion})

            resumen_transferencias.append({
                'cuenta_origen': origen,
                'cuenta_destino': destino,
                'monto': monto,
                'saldo_origen_despues': c_origen['saldo'] - monto,
                'saldo_destino_despues': c_destino['saldo'] + monto,
                'descripcion': descripcion
            })

        conn.close()

        return jsonify({
            'success': True,
            'total_registros': total,
            'validos': validos,
            'errores': errores,
            'monto_total': monto_total,
            'errores_detalle': errores_detalle,
            'datos_validos': datos_validos,
            'resumen_transferencias': resumen_transferencias
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/procesar_transferencias_ahorro', methods=['POST'])
@login_required()
@permission_required('ahorro.masivo')
def procesar_transferencias_ahorro():
    data = request.get_json() or {}
    movimientos = data.get('transferencias', [])
    fecha_transferencia = data.get('fecha_transferencia', date.today().isoformat())
    tipo_transferencia = data.get('tipo_transferencia', 'inmediata')
    comision = float(data.get('comision_transferencia', 0.0) or 0.0)

    if not movimientos:
        return jsonify({'success': False, 'error': 'No hay transferencias para procesar'}), 400

    conn = get_db()
    procesados = 0
    monto_total = 0.0
    errores = []

    for movimiento in movimientos:
        try:
            origen_id = movimiento['cuenta_origen']
            destino_id = movimiento['cuenta_destino']
            monto = float(movimiento['monto'])
            descripcion = movimiento.get('descripcion', 'Transferencia interna')

            c_origen = db_fetchone(conn, 'SELECT saldo FROM cuentas WHERE id=? AND tipo="ahorro" AND estado="activa"', (origen_id,))
            c_destino = db_fetchone(conn, 'SELECT saldo FROM cuentas WHERE id=? AND tipo="ahorro" AND estado="activa"', (destino_id,))

            if not c_origen or not c_destino:
                errores.append(f'Origen o destino no válidos para transferencia {origen_id}->{destino_id}')
                continue

            if monto <= 0 or monto > c_origen['saldo']:
                errores.append(f'Monto inválido para transferencia {origen_id}->{destino_id}')
                continue

            saldo_origen_nuevo = c_origen['saldo'] - monto - comision
            saldo_destino_nuevo = c_destino['saldo'] + monto
            if saldo_origen_nuevo < 0:
                errores.append(f'Saldo insuficiente (incluyendo comisión) en cuenta origen {origen_id}')
                continue

            db_execute(conn, 'UPDATE cuentas SET saldo=? WHERE id=?', (saldo_origen_nuevo, origen_id))
            db_execute(conn, 'UPDATE cuentas SET saldo=? WHERE id=?', (saldo_destino_nuevo, destino_id))

            db_execute(
                conn,
                'INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)',
                (origen_id, 'transferencia_salida', monto, saldo_origen_nuevo, descripcion + f' ({tipo_transferencia})', fecha_transferencia)
            )
            db_execute(
                conn,
                'INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)',
                (destino_id, 'transferencia_entrada', monto, saldo_destino_nuevo, descripcion + f' ({tipo_transferencia})', fecha_transferencia)
            )

            if comision > 0:
                db_execute(
                    conn,
                    'INSERT INTO transacciones (cuenta_id,tipo,monto,saldo_despues,descripcion,fecha) VALUES (?,?,?,?,?,?)',
                    (origen_id, 'comision', comision, saldo_origen_nuevo, 'Comisión transferencia', fecha_transferencia)
                )

            procesados += 1
            monto_total += monto

        except Exception as e:
            errores.append(f'Error traslado {origen_id}->{destino_id}: {str(e)}')

    conn.commit()
    conn.close()

    return jsonify({'success': True, 'procesados': procesados, 'monto_total': monto_total, 'errores': errores})

@bp.route('/generar_reporte_ahorro', methods=['POST'])
@login_required()
def generar_reporte_ahorro():
    data = request.get_json() or {}
    tipo = data.get('tipo_reporte', 'saldos')
    fecha_inicio = data.get('fecha_inicio')
    fecha_fin = data.get('fecha_fin')

    conn = get_db()

    try:
        if tipo == 'saldos':
            rows = db_fetchall(conn, '''
                SELECT c.numero AS numero_cuenta, s.nombre || ' ' || s.apellido AS nombre_socio,
                       c.saldo AS saldo_actual, c.estado,
                       (SELECT fecha FROM transacciones t WHERE t.cuenta_id=c.id ORDER BY t.fecha DESC LIMIT 1) AS ultimo_movimiento
                FROM cuentas c
                JOIN socios s ON c.socio_id=s.id
                WHERE c.tipo='ahorro'
            ''')
            resultados = [{'numero_cuenta': r['numero_cuenta'], 'nombre_socio': r['nombre_socio'], 'saldo_actual': r['saldo_actual'] or 0.0, 'ultimo_movimiento': r['ultimo_movimiento'], 'estado': r['estado']} for r in rows]

        elif tipo == 'movimientos':
            if not fecha_inicio or not fecha_fin:
                return jsonify({'success': False, 'error': 'Debe indicar rango de fechas'}), 400
            rows = db_fetchall(conn, '''
                SELECT t.fecha, c.numero AS numero_cuenta, t.tipo, t.monto, t.saldo_despues, t.descripcion
                FROM transacciones t
                JOIN cuentas c ON t.cuenta_id=c.id
                WHERE c.tipo='ahorro' AND date(t.fecha) BETWEEN date(?) AND date(?)
                ORDER BY t.fecha ASC
            ''', (fecha_inicio, fecha_fin))
            resultados = [{'fecha': r['fecha'], 'numero_cuenta': r['numero_cuenta'], 'tipo': r['tipo'], 'monto': r['monto'], 'saldo_despues': r['saldo_despues'], 'descripcion': r['descripcion']} for r in rows]

        elif tipo == 'comparativo':
            if not fecha_inicio or not fecha_fin:
                return jsonify({'success': False, 'error': 'Debe indicar rango de fechas'}), 400
            cuentas_data = db_fetchall(conn, 'SELECT id, numero, socio_id, saldo FROM cuentas WHERE tipo="ahorro"')
            resultados = []
            for cuenta in cuentas_data:
                saldo_actual = cuenta['saldo'] or 0.0
                anterior = db_fetchone(conn, '''
                    SELECT saldo_despues FROM transacciones
                    WHERE cuenta_id=? AND date(fecha) < date(?)
                    ORDER BY fecha DESC LIMIT 1
                ''', (cuenta['id'], fecha_inicio))
                saldo_anterior = anterior['saldo_despues'] if anterior else 0.0
                socio = db_fetchone(conn, 'SELECT nombre, apellido FROM socios WHERE id=?', (cuenta['socio_id'],))
                resultados.append({
                    'numero_cuenta': cuenta['numero'],
                    'nombre_socio': socio['nombre'] + ' ' + socio['apellido'],
                    'saldo_anterior': saldo_anterior,
                    'saldo_actual': saldo_actual
                })

        elif tipo == 'inactivas':
            fecha_corte = fecha_fin or date.today().isoformat()
            if _is_postgres_connection(conn):
                rows = db_fetchall(conn, '''
                    SELECT c.numero AS numero_cuenta,
                           s.nombre || ' ' || s.apellido AS nombre_socio,
                           c.saldo AS saldo_actual,
                           MAX(t.fecha) AS ultima_actividad,
                           (DATE(%s) - MAX(DATE(t.fecha)))::int AS dias_inactiva
                    FROM cuentas c
                    JOIN socios s ON c.socio_id=s.id
                    LEFT JOIN transacciones t ON t.cuenta_id=c.id
                    WHERE c.tipo='ahorro'
                    GROUP BY c.id, c.numero, s.nombre, s.apellido, c.saldo
                    HAVING (DATE(%s) - MAX(DATE(t.fecha)))::int > 30
                ''', (fecha_corte, fecha_corte))
            else:
                rows = db_fetchall(conn, '''
                    SELECT c.numero AS numero_cuenta, s.nombre || ' ' || s.apellido AS nombre_socio,
                           c.saldo AS saldo_actual,
                           MAX(t.fecha) AS ultima_actividad,
                           julianday(date(?)) - julianday(MAX(date(t.fecha))) AS dias_inactiva
                    FROM cuentas c
                    JOIN socios s ON c.socio_id=s.id
                    LEFT JOIN transacciones t ON t.cuenta_id=c.id
                    WHERE c.tipo='ahorro'
                    GROUP BY c.id
                    HAVING dias_inactiva > 30
                ''', (fecha_corte,))
            resultados = [{'numero_cuenta': r['numero_cuenta'], 'nombre_socio': r['nombre_socio'], 'saldo_actual': r['saldo_actual'] or 0.0, 'ultima_actividad': r['ultima_actividad'] or 'N/A', 'dias_inactiva': int(r['dias_inactiva'] or 0)} for r in rows]

        else:
            return jsonify({'success': False, 'error': 'Tipo de reporte desconocido'}), 400

        total_cuentas = db_fetchone(conn, 'SELECT COUNT(*) FROM cuentas WHERE tipo="ahorro"')[0]
        total_saldo = db_fetchone(conn, 'SELECT COALESCE(SUM(saldo),0) FROM cuentas WHERE tipo="ahorro"')[0]
        promedio_saldo = float(total_saldo) / total_cuentas if total_cuentas else 0.0
        cuentas_activas = db_fetchone(conn, 'SELECT COUNT(*) FROM cuentas WHERE tipo="ahorro" AND estado="activa"')[0]

        return jsonify({'success': True, 'resultados': resultados, 'estadisticas': {
            'total_cuentas': total_cuentas,
            'saldo_total': total_saldo,
            'promedio_saldo': promedio_saldo,
            'cuentas_activas': cuentas_activas
        }})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        conn.close()

@bp.route('/generar_planilla_ahorro')
@login_required()
def generar_planilla_ahorro():
    conn = get_db()
    cuentas = db_fetchall(conn, '''
        SELECT c.id, c.numero, c.saldo, s.nombre, s.apellido, s.codigo
        FROM cuentas c
        JOIN socios s ON c.socio_id = s.id
        WHERE c.estado = 'activa' AND s.estado = 'activo'
        ORDER BY s.apellido, s.nombre
    ''')
    conn.close()

    return render_template('planilla_ahorro.html', cuentas=cuentas)

@bp.route('/planilla_ahorro')
@login_required()
def planilla_ahorro():
    return redirect(url_for('ahorro.generar_planilla_ahorro'))

@bp.route('/planillas_ahorro_pendientes')
@login_required()
def planillas_ahorro_pendientes():
    import math as _math
    conn = get_db()
    nombre = request.args.get('nombre', '').strip()
    frecuencia = request.args.get('frecuencia', '').strip()
    estado = request.args.get('estado', '').strip().lower()
    fecha_desde = request.args.get('fecha_desde', '').strip()
    fecha_hasta = request.args.get('fecha_hasta', '').strip()
    page = max(1, int(request.args.get('page', 1) or 1))
    per_page = min(100, max(10, int(request.args.get('per_page', 50) or 50)))

    base_query = "FROM planillas_masivas WHERE tipo = 'ahorro_cuotas'"
    params = []

    if nombre:
        base_query += ' AND nombre LIKE ?'
        params.append(f'%{nombre}%')
    if frecuencia:
        base_query += ' AND frecuencia = ?'
        params.append(frecuencia)
    if estado:
        base_query += ' AND estado = ?'
        params.append(estado)
    if fecha_desde:
        base_query += ' AND date(fecha_pago) >= date(?)'
        params.append(fecha_desde)
    if fecha_hasta:
        base_query += ' AND date(fecha_pago) <= date(?)'
        params.append(fecha_hasta)

    order_sql = '''
        ORDER BY CASE estado
            WHEN 'pendiente' THEN 1
            WHEN 'parcial' THEN 2
            WHEN 'aplicada' THEN 3
            ELSE 4
        END, fecha_creacion DESC, id DESC
    '''

    total_planillas = db_fetchone(conn, f'SELECT COUNT(*) {base_query}', params)[0]
    total_pages = max(1, _math.ceil(total_planillas / per_page))
    offset = (page - 1) * per_page

    planillas_rows = db_fetchall(
        conn,
        f'SELECT * {base_query} {order_sql} LIMIT ? OFFSET ?',
        params + [per_page, offset]
    )
    # Calcular total de monto sin paginación para el resumen
    total_monto_row = db_fetchone(conn, f'SELECT COALESCE(SUM(total_monto),0) {base_query}', params)
    total_monto = float(total_monto_row[0] or 0)
    conn.close()

    planillas = []
    for row in planillas_rows:
        item = dict(row)
        item['tipo_cuenta_label'] = obtener_tipo_cuenta_desde_planilla(item.get('nombre'))
        planillas.append(item)

    return render_template(
        'planillas_ahorro_pendientes.html',
        planillas=planillas,
        filtros={
            'nombre': nombre,
            'frecuencia': frecuencia,
            'estado': estado,
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta
        },
        total_planillas=total_planillas,
        total_monto=total_monto,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )

@bp.route('/planillas_ahorro/<int:planilla_id>')
@login_required()
def detalle_planilla_ahorro(planilla_id):
    conn = get_db()
    planilla = db_fetchone(conn, '''
        SELECT * FROM planillas_masivas
        WHERE id=? AND tipo='ahorro_cuotas'
    ''', (planilla_id,))

    if not planilla:
        conn.close()
        flash('Planilla de ahorro no encontrada.', 'danger')
        return redirect(url_for('ahorro.planillas_ahorro_pendientes'))

    detalles = db_fetchall(conn, '''
        SELECT d.*, c.saldo AS saldo_actual
        FROM planilla_masiva_detalles d
        LEFT JOIN cuentas c ON d.referencia_id = c.id AND d.referencia_tipo = 'cuenta_ahorro'
        WHERE d.planilla_id=?
        ORDER BY socio_nombre, numero_referencia
    ''', (planilla_id,))
    conn.close()

    return render_template(
        'planilla_cuotas_ahorro.html',
        planilla=planilla,
        detalles=detalles,
        nombre_planilla=planilla['nombre'],
        tipo_cuenta_label=obtener_tipo_cuenta_desde_planilla(planilla['nombre']),
        frecuencia=planilla['frecuencia'],
        fecha_pago=planilla['fecha_pago'],
        total_cuotas=planilla['total_monto'] or 0
    )

@bp.route('/planillas_ahorro/<int:planilla_id>/exportar')
@login_required()
def exportar_planilla_ahorro(planilla_id):
    conn = get_db()
    planilla = db_fetchone(conn, '''
        SELECT * FROM planillas_masivas
        WHERE id=? AND tipo='ahorro_cuotas'
    ''', (planilla_id,))

    if not planilla:
        conn.close()
        flash('Planilla de ahorro no encontrada.', 'danger')
        return redirect(url_for('ahorro.planillas_ahorro_pendientes'))

    detalles = db_fetchall(conn, '''
        SELECT d.*, c.saldo AS saldo_actual
        FROM planilla_masiva_detalles d
        LEFT JOIN cuentas c ON d.referencia_id = c.id AND d.referencia_tipo = 'cuenta'
        WHERE d.planilla_id=?
        ORDER BY socio_nombre, numero_referencia
    ''', (planilla_id,))
    conn.close()

    import io
    import csv
    from flask import Response

    output = io.StringIO()
    output.write('\ufeff')  # BOM de UTF-8 para Excel
    writer = csv.writer(output, delimiter=';')

    headers = [
        'Código Socio', 'Nombre Socio', 'Número de Cuenta', 
        'Saldo Actual', 'Abono Programado', 'Estado'
    ]
    writer.writerow(headers)

    for d in detalles:
        saldo = f"Q{d['saldo_actual']:.2f}" if d['saldo_actual'] is not None else "Pendiente de aplicar"
        writer.writerow([
            d['socio_codigo'],
            d['socio_nombre'],
            d['numero_referencia'],
            saldo,
            f"Q{d['monto']:.2f}",
            d['estado'].upper()
        ])

    filename = f"Planilla_Ahorro_{planilla['nombre'].replace(' ', '_')}"
    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    response.headers['Content-type'] = 'text/csv; charset=utf-8'
    return response

@bp.route('/planillas_ahorro/<int:planilla_id>/editar', methods=['GET', 'POST'])
@login_required()
def editar_planilla_ahorro(planilla_id):
    conn = get_db()
    planilla = db_fetchone(conn, '''
        SELECT * FROM planillas_masivas
        WHERE id=? AND tipo='ahorro_cuotas'
    ''', (planilla_id,))

    if not planilla:
        conn.close()
        flash('Planilla de ahorro no encontrada.', 'danger')
        return redirect(url_for('ahorro.planillas_ahorro_pendientes'))

    if planilla['estado'] == 'aplicada':
        conn.close()
        flash('No se puede modificar una planilla ya aplicada.', 'warning')
        return redirect(url_for('ahorro.detalle_planilla_ahorro', planilla_id=planilla_id))

    detalles = db_fetchall(
        conn,
        '''
        SELECT id, socio_codigo, socio_nombre, numero_referencia, monto, estado
        FROM planilla_masiva_detalles
        WHERE planilla_id=?
        ORDER BY socio_nombre, numero_referencia
        ''',
        (planilla_id,)
    )

    if request.method == 'POST':
        nombre = request.form.get('nombre_planilla', '').strip()
        fecha_pago = request.form.get('fecha_pago', '').strip()
        frecuencia = request.form.get('frecuencia', '').strip()
        accion = request.form.get('accion', 'guardar').strip().lower()

        if periodo_cerrado('ahorro', fecha_pago):
            conn.close()
            flash('No se puede modificar la planilla porque el periodo de ahorro está cerrado.', 'warning')
            return redirect(url_for('ahorro.detalle_planilla_ahorro', planilla_id=planilla_id))

        if not nombre or not fecha_pago or frecuencia not in ('Quincenal', 'Catorcenal'):
            conn.close()
            flash('Debe completar nombre, fecha de pago y frecuencia valida.', 'danger')
            return redirect(url_for('ahorro.editar_planilla_ahorro', planilla_id=planilla_id))

        if accion == 'recalcular':
            detalles_recalculo = db_fetchall(
                conn,
                '''
                SELECT d.id, d.monto, d.estado, d.referencia_id,
                       s.cuota_ahorro
                FROM planilla_masiva_detalles d
                LEFT JOIN cuentas c ON c.id = d.referencia_id AND d.referencia_tipo = 'cuenta_ahorro'
                LEFT JOIN socios s ON s.id = c.socio_id
                WHERE d.planilla_id=?
                ''',
                (planilla_id,)
            )

            for d in detalles_recalculo:
                if (d['estado'] or '').lower() != 'pendiente':
                    continue
                nueva_cuota = round(float(d['cuota_ahorro'] or 0), 2)
                if nueva_cuota < 0:
                    nueva_cuota = 0
                db_execute(
                    conn,
                    '''
                    UPDATE planilla_masiva_detalles
                    SET monto=?
                    WHERE id=? AND planilla_id=?
                    ''',
                    (nueva_cuota, d['id'], planilla_id)
                )
        else:
            detalle_ids = request.form.getlist('detalle_id[]')
            detalle_montos = request.form.getlist('detalle_monto[]')
            if len(detalle_ids) != len(detalle_montos):
                conn.close()
                flash('Los datos de cuotas programadas son inconsistentes.', 'danger')
                return redirect(url_for('ahorro.editar_planilla_ahorro', planilla_id=planilla_id))

            for detalle_id, monto_str in zip(detalle_ids, detalle_montos):
                try:
                    monto_valor = round(float(monto_str or 0), 2)
                except Exception:
                    conn.close()
                    flash('Cada cuota programada debe tener un monto valido.', 'danger')
                    return redirect(url_for('ahorro.editar_planilla_ahorro', planilla_id=planilla_id))

                if monto_valor < 0:
                    conn.close()
                    flash('La cuota programada no puede ser negativa.', 'danger')
                    return redirect(url_for('ahorro.editar_planilla_ahorro', planilla_id=planilla_id))

                db_execute(
                    conn,
                    '''
                    UPDATE planilla_masiva_detalles
                    SET monto=?
                    WHERE id=? AND planilla_id=? AND estado='pendiente'
                    ''',
                    (monto_valor, detalle_id, planilla_id)
                )

        total_monto = db_fetchone(
            conn,
            '''
            SELECT COALESCE(SUM(monto), 0)
            FROM planilla_masiva_detalles
            WHERE planilla_id=?
            ''',
            (planilla_id,)
        )[0]

        db_execute(conn, '''
            UPDATE planillas_masivas
            SET nombre=?, fecha_pago=?, frecuencia=?, total_monto=?
            WHERE id=?
        ''', (nombre, fecha_pago, frecuencia, round(total_monto, 2), planilla_id))
        conn.commit()
        conn.close()
        log_auditoria_evento(
            modulo='ahorro',
            entidad='planilla_masiva',
            entidad_id=planilla_id,
            accion='editar',
            descripcion='Planilla de ahorro modificada',
            datos={
                'nombre': nombre,
                'fecha_pago': fecha_pago,
                'frecuencia': frecuencia,
                'total_monto': round(float(total_monto or 0), 2),
                'accion': accion,
            }
        )
        if accion == 'recalcular':
            flash('Cuotas recalculadas con la cuota de ahorro vigente y planilla actualizada.', 'success')
            return redirect(url_for('ahorro.detalle_planilla_ahorro', planilla_id=planilla_id))
        else:
            flash('Planilla de ahorro actualizada correctamente.', 'success')
            return redirect(url_for('ahorro.planillas_ahorro_pendientes'))

    conn.close()
    return render_template('editar_planilla_ahorro.html', planilla=planilla, detalles=detalles)

@bp.route('/planillas_ahorro/<int:planilla_id>/eliminar', methods=['POST'])
@login_required()
def eliminar_planilla_ahorro(planilla_id):
    conn = get_db()
    planilla = db_fetchone(conn, '''
        SELECT * FROM planillas_masivas
        WHERE id=? AND tipo='ahorro_cuotas'
    ''', (planilla_id,))

    if not planilla:
        conn.close()
        flash('Planilla de ahorro no encontrada.', 'danger')
        return redirect(url_for('ahorro.planillas_ahorro_pendientes'))

    if planilla['estado'] == 'aplicada':
        conn.close()
        flash('No se puede eliminar una planilla ya aplicada.', 'warning')
        return redirect(url_for('ahorro.planillas_ahorro_pendientes'))

    db_execute(conn, 'DELETE FROM planilla_masiva_detalles WHERE planilla_id=?', (planilla_id,))
    db_execute(conn, 'DELETE FROM planillas_masivas WHERE id=?', (planilla_id,))
    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='ahorro',
        entidad='planilla_masiva',
        entidad_id=planilla_id,
        accion='eliminar',
        descripcion='Planilla de ahorro eliminada'
    )

    flash('Planilla de ahorro eliminada correctamente.', 'success')
    return redirect(url_for('ahorro.planillas_ahorro_pendientes'))

@bp.route('/generar_planilla_cuotas_ahorro', methods=['GET', 'POST'])
@login_required()
def generar_planilla_cuotas_ahorro():
    form_data = {
        'nombre_planilla': '',
        'frecuencia': 'Quincenal',
        'fecha_pago': date.today().isoformat(),
        'tipo_cuenta': 'ahorro_corriente',
    }

    if request.method == 'POST':
        nombre_planilla = request.form.get('nombre_planilla', '').strip()
        frecuencia = request.form.get('frecuencia', '').strip()
        fecha_pago = request.form.get('fecha_pago', '').strip()
        tipo_cuenta = request.form.get('tipo_cuenta', '').strip()

        form_data = {
            'nombre_planilla': nombre_planilla,
            'frecuencia': frecuencia or 'Quincenal',
            'fecha_pago': fecha_pago or date.today().isoformat(),
            'tipo_cuenta': tipo_cuenta or 'ahorro_corriente',
        }

        if not nombre_planilla or not frecuencia or not fecha_pago or not tipo_cuenta:
            flash('Todos los campos son obligatorios.', 'danger')
            return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

        if frecuencia not in ('Quincenal', 'Catorcenal'):
            flash('Frecuencia no valida.', 'danger')
            return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

        tipos_validos = {'ahorro_aportacion', 'ahorro_corriente', 'ahorro_plazo_fijo', 'ahorro_inscripcion'}
        if tipo_cuenta not in tipos_validos:
            flash('Tipo de cuenta no valido.', 'danger')
            return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

        columnas_cuota = {
            'ahorro_corriente': 'cuota_ahorro',
            'ahorro_aportacion': 'cuota_aportacion',
            'ahorro_inscripcion': 'cuota_inscripcion',
            'ahorro_plazo_fijo': 'cuota_ahorro'
        }
        columna_cuota = columnas_cuota.get(tipo_cuenta, 'cuota_ahorro')

        conn = get_db()

        filtro_tipo = "AND COALESCE(c.producto_ahorro, 'ahorro_corriente') = ?"
        params = [frecuencia, tipo_cuenta]

        # Obtener socios con cuota > 0, frecuencia y tipo de cuenta configurados
        cuentas = db_fetchall(
            conn,
            f'''
            SELECT c.id, c.numero, c.saldo, s.nombre, s.apellido, s.codigo,
                   s.{columna_cuota} AS cuota_monto, s.frecuencia
            FROM cuentas c
            JOIN socios s ON c.socio_id = s.id
            WHERE c.estado = 'activa'
              AND c.tipo = 'ahorro'
              AND s.estado = 'activo'
              AND s.{columna_cuota} > 0
              AND s.frecuencia = ?
              {filtro_tipo}
            ORDER BY s.apellido, s.nombre
            ''',
            params
        )

        # Calcular total de cuotas
        total_cuotas = sum(cuenta['cuota_monto'] for cuenta in cuentas)

        if not cuentas:
            conn.close()
            flash('No se encontraron socios para generar la planilla con los filtros seleccionados.', 'warning')
            return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

        tipo_label = {
            'ahorro_aportacion': 'Aportacion',
            'ahorro_corriente': 'Ahorro corriente',
            'ahorro_plazo_fijo': 'Plazo fijo',
            'ahorro_inscripcion': 'Inscripcion',
        }.get(tipo_cuenta, tipo_cuenta)

        nombre_planilla_guardado = f"{nombre_planilla} [{tipo_label}]"

        planilla_id = db_insert_and_get_id(
            conn,
            '''
            INSERT INTO planillas_masivas
            (tipo, nombre, fecha_pago, frecuencia, estado, total_monto, total_registros, fecha_creacion, usuario_creacion)
            VALUES (?, ?, ?, ?, 'pendiente', ?, ?, ?, ?)
            ''',
            (
                'ahorro_cuotas', nombre_planilla_guardado, fecha_pago, frecuencia,
                total_cuotas, len(cuentas), datetime.now().isoformat(), session.get('username')
            )
        )

        for cuenta in cuentas:
            db_execute(
                conn,
                '''
                INSERT INTO planilla_masiva_detalles
                (planilla_id, referencia_tipo, referencia_id, numero_referencia, socio_codigo, socio_nombre, monto, estado)
                VALUES (?, 'cuenta_ahorro', ?, ?, ?, ?, ?, 'pendiente')
                ''',
                (
                    planilla_id,
                    cuenta['id'],
                    cuenta['numero'],
                    cuenta['codigo'],
                    f"{cuenta['nombre']} {cuenta['apellido']}",
                    cuenta['cuota_monto']
                )
            )

        conn.commit()
        conn.close()
        flash('Planilla generada y guardada como pendiente.', 'success')
        return redirect(url_for('ahorro.detalle_planilla_ahorro', planilla_id=planilla_id))

    return render_template('generar_planilla_cuotas_ahorro.html', form_data=form_data)

@bp.route('/procesar_abonos_masivos', methods=['POST'])
@login_required()
@permission_required('ahorro.masivo')
def procesar_abonos_masivos():
    data = request.get_json()
    planilla_id = data.get('planilla_id')
    abonos = data.get('abonos', [])
    fecha_pago = data.get('fecha', datetime.now().isoformat())
    nombre_planilla = data.get('nombre_planilla', 'Abono masivo')
    boleta_deposito = data.get('boleta_deposito', '').strip()
    frecuencia = data.get('frecuencia', '').strip()

    if periodo_cerrado('ahorro', fecha_pago):
        return jsonify({'error': 'El periodo de ahorro está cerrado para la fecha indicada.'}), 400

    if not boleta_deposito:
        return jsonify({'error': 'Debe indicar numero de boleta de pago para aplicar la planilla.'}), 400
    
    conn = get_db()
    planilla = None
    detalles_planilla = []
    if planilla_id:
        planilla = db_fetchone(conn, '''
            SELECT * FROM planillas_masivas
            WHERE id=? AND tipo='ahorro_cuotas'
        ''', (planilla_id,))

        if not planilla:
            conn.close()
            return jsonify({'error': 'La planilla seleccionada no existe.'}), 404

        if planilla['estado'] == 'aplicada':
            conn.close()
            return jsonify({'error': 'La planilla ya fue aplicada anteriormente.'}), 400

        # Actualizar los montos modificados en la base de datos antes de aplicar
        for abono in abonos:
            if abono.get('detalle_id') and 'monto' in abono:
                try:
                    db_execute(conn, '''
                        UPDATE planilla_masiva_detalles
                        SET monto=?
                        WHERE id=? AND estado='pendiente'
                    ''', (float(abono['monto']), abono['detalle_id']))
                except (ValueError, TypeError):
                    pass

        # Recalcular el monto total de la planilla masiva
        db_execute(conn, '''
            UPDATE planillas_masivas
            SET total_monto = (SELECT SUM(monto) FROM planilla_masiva_detalles WHERE planilla_id=?)
            WHERE id=?
        ''', (planilla_id, planilla_id))

        # Recuperar los detalles actualizados
        detalles_planilla = db_fetchall(conn, '''
            SELECT * FROM planilla_masiva_detalles
            WHERE planilla_id=? AND estado='pendiente'
        ''', (planilla_id,))
        abonos = [
            {
                'cuenta_id': detalle['referencia_id'],
                'numero': detalle['numero_referencia'],
                'monto': detalle['monto'],
                'detalle_id': detalle['id']
            }
            for detalle in detalles_planilla
        ]
        nombre_planilla = planilla['nombre']
        fecha_pago = planilla['fecha_pago']
        frecuencia = planilla['frecuencia'] or frecuencia
    
    procesados = 0
    errores = []
    
    for abono in abonos:
        try:
            cuenta_id = abono['cuenta_id']
            monto = float(abono['monto'])
            
            if monto < 0:
                errores.append(f"Monto inválido para cuenta {abono.get('numero', cuenta_id)}")
                continue

            if monto == 0:
                # Si el monto es cero, se marca como aplicado sin registrar transacción ni alterar saldo
                if abono.get('detalle_id'):
                    db_execute(conn, "UPDATE planilla_masiva_detalles SET estado='aplicado', monto=0 WHERE id=?", (abono['detalle_id'],))
                procesados += 1
                continue
            
            # Obtener información de la cuenta y socio
            cuenta = db_fetchone(conn, 'SELECT c.saldo, c.socio_id FROM cuentas c WHERE c.id = ?', (cuenta_id,))
            
            if not cuenta:
                errores.append(f"Cuenta {abono.get('numero', cuenta_id)} no encontrada")
                continue
            
            nuevo_saldo = float(cuenta['saldo'] or 0) + monto
            
            # Actualizar saldo
            db_execute(conn, 'UPDATE cuentas SET saldo = ? WHERE id = ?', (nuevo_saldo, cuenta_id))
            
            # Registrar transacción
            descripcion_planilla = f"Planilla: {nombre_planilla}"
            if boleta_deposito:
                descripcion_planilla += f" | Boleta: {boleta_deposito}"
            if frecuencia:
                descripcion_planilla += f" | Frecuencia: {frecuencia}"

            db_execute(
                conn,
                '''
                INSERT INTO transacciones (cuenta_id, tipo, monto, saldo_despues, descripcion, fecha)
                VALUES (?, 'deposito', ?, ?, ?, ?)
                ''',
                (cuenta_id, monto, nuevo_saldo, descripcion_planilla, fecha_pago)
            )

            if abono.get('detalle_id'):
                db_execute(conn, "UPDATE planilla_masiva_detalles SET estado='aplicado' WHERE id=?", (abono['detalle_id'],))
            
            procesados += 1
            
        except Exception as e:
            errores.append(f"Error procesando cuenta {abono.get('numero', cuenta_id)}: {str(e)}")
    
    if planilla_id and planilla:
        estado_final = 'aplicada' if procesados == len(abonos) and not errores else ('parcial' if procesados > 0 else 'pendiente')
        db_execute(conn, '''
            UPDATE planillas_masivas
            SET estado=?, boleta_deposito=?, fecha_aplicacion=?, usuario_aplicacion=?
            WHERE id=?
        ''', (estado_final, boleta_deposito, datetime.now().isoformat(), session.get('username'), planilla_id))

    conn.commit()
    conn.close()

    log_auditoria_evento(
        modulo='ahorro',
        entidad='planilla_masiva',
        entidad_id=planilla_id,
        accion='aplicar',
        descripcion='Aplicación de abonos masivos',
        datos={'procesados': procesados, 'errores': len(errores), 'boleta': boleta_deposito}
    )
    
    return jsonify({
        'procesados': procesados,
        'errores': errores,
        'total': len(abonos),
        'planilla_id': planilla_id
    })
