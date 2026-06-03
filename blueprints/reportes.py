from flask import Blueprint, render_template, request, Response, session, url_for, redirect, flash
from datetime import date, datetime, timedelta
import csv
import io
from utils.db import get_db, db_fetchall, db_fetchone
from utils.decorators import login_required, permission_required
from utils.helpers import formatear_fecha_dmy

bp = Blueprint('reportes', __name__, url_prefix='/reportes')

def exportar_csv(filename, headers, rows):
    """
    Genera una respuesta HTTP para descargar un archivo CSV compatible con Excel en español.
    """
    output = io.StringIO()
    # Escribir UTF-8 BOM para soporte correcto de tildes y caracteres especiales en Excel
    output.write('\ufeff')
    writer = csv.writer(output, delimiter=';')
    writer.writerow(headers)
    writer.writerows(rows)
    
    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    response.headers['Content-type'] = 'text/csv; charset=utf-8'
    return response
@bp.route('/')
@login_required()
def index():
    return render_template('reportes/dashboard.html')


@bp.route('/cartera/exportar')
@bp.route('/cartera')
@login_required()
def cartera():
    conn = get_db()
    
    # Filtros
    categoria_id = request.args.get('categoria_id', '')
    estado = request.args.get('estado', 'todos')
    fecha_desde = request.args.get('fecha_desde', '')
    fecha_hasta = request.args.get('fecha_hasta', '')
    
    # Consulta ampliada con DPI, Frecuencia y Cuota mensual
    query = '''
        SELECT 
            p.numero,
            s.nombre || ' ' || s.apellido AS socio_nombre,
            s.codigo AS socio_codigo,
            s.dpi AS socio_dpi,
            s.frecuencia AS socio_frecuencia,
            COALESCE(pc.nombre, 'Sin categoría') AS categoria,
            p.monto_solicitado,
            p.monto_aprobado,
            p.saldo_pendiente,
            p.tasa_interes,
            p.plazo_meses,
            p.cuota_mensual,
            p.fecha_solicitud,
            p.fecha_aprobacion,
            p.estado
        FROM prestamos p
        JOIN socios s ON p.socio_id = s.id
        LEFT JOIN prestamo_categorias pc ON p.categoria_id = pc.id
        WHERE 1=1
    '''
    params = []
    
    if categoria_id:
        query += " AND p.categoria_id = ?"
        params.append(categoria_id)
        
    if estado != 'todos':
        query += " AND p.estado = ?"
        params.append(estado)
        
    if fecha_desde:
        query += " AND date(p.fecha_aprobacion) >= date(?)"
        params.append(fecha_desde)
        
    if fecha_hasta:
        query += " AND date(p.fecha_aprobacion) <= date(?)"
        params.append(fecha_hasta)
        
    query += " ORDER BY p.numero DESC"
    
    try:
        prestamos = db_fetchall(conn, query, params)
        categorias = db_fetchall(conn, "SELECT id, nombre FROM prestamo_categorias WHERE estado='activo'")
        
        # Estadísticas agregadas
        total_aprobado = sum(float(p['monto_aprobado'] or 0) for p in prestamos)
        total_pendiente = sum(float(p['saldo_pendiente'] or 0) for p in prestamos)
        total_solicitado = sum(float(p['monto_solicitado'] or 0) for p in prestamos)
        
    except Exception as e:
        flash(f"Error consultando cartera: {e}", "danger")
        prestamos, categorias = [], []
        total_aprobado = total_pendiente = total_solicitado = 0.0
        
    conn.close()
    
    # Si es exportación CSV
    if request.path.endswith('/exportar'):
        headers = [
            'No. Préstamo', 'Asociado', 'Código Socio', 'DPI', 'Frecuencia Cobro', 'Categoría', 
            'Monto Solicitado', 'Monto Aprobado', 'Saldo Pendiente', 'Cuota Mensual',
            'Tasa Interés (%)', 'Plazo (Meses)', 'Fecha Solicitud', 
            'Fecha Aprobación', 'Estado'
        ]
        rows = []
        for p in prestamos:
            rows.append([
                p['numero'], p['socio_nombre'], p['socio_codigo'], p['socio_dpi'], p['socio_frecuencia'], p['categoria'],
                p['monto_solicitado'], p['monto_aprobado'], p['saldo_pendiente'], p['cuota_mensual'] or 0.0,
                p['tasa_interes'], p['plazo_meses'], formatear_fecha_dmy(p['fecha_solicitud']),
                formatear_fecha_dmy(p['fecha_aprobacion']), p['estado'].upper()
            ])
        return exportar_csv('Reporte_Cartera_Creditos', headers, rows)
        
    return render_template(
        'reportes/cartera.html',
        prestamos=prestamos,
        categorias=categorias,
        categoria_id=categoria_id,
        estado_filtro=estado,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        total_aprobado=total_aprobado,
        total_pendiente=total_pendiente,
        total_solicitado=total_solicitado
    )

@bp.route('/morosidad/exportar')
@bp.route('/morosidad')
@login_required()
def morosidad():
    conn = get_db()
    
    rango_mora = request.args.get('rango_mora', 'todos')
    
    # Query ampliada con DPI, Teléfono, y subconsultas de último pago (fecha y monto)
    query = '''
        SELECT 
            p.numero,
            s.nombre || ' ' || s.apellido AS socio_nombre,
            s.codigo AS socio_codigo,
            s.dpi AS socio_dpi,
            s.telefono AS socio_telefono,
            p.monto_aprobado,
            p.saldo_pendiente,
            p.tasa_interes,
            p.etapa_cobranza,
            (
                SELECT MIN(cp.fecha_programada)
                FROM prestamo_calendario_pagos cp
                WHERE cp.prestamo_id = p.id AND cp.estado = 'pendiente'
            ) AS proxima_cuota_vencida,
            (
                SELECT COUNT(*)
                FROM prestamo_calendario_pagos cp
                WHERE cp.prestamo_id = p.id AND cp.estado = 'pendiente' AND date(cp.fecha_programada) < date('now')
            ) AS cuotas_vencidas_count,
            CAST(
                CASE 
                    WHEN (
                        SELECT MIN(cp.fecha_programada)
                        FROM prestamo_calendario_pagos cp
                        WHERE cp.prestamo_id = p.id AND cp.estado = 'pendiente'
                    ) IS NOT NULL AND date(
                        (
                            SELECT MIN(cp.fecha_programada)
                            FROM prestamo_calendario_pagos cp
                            WHERE cp.prestamo_id = p.id AND cp.estado = 'pendiente'
                        )
                    ) < date('now')
                    THEN julianday('now') - julianday(
                        (
                            SELECT MIN(cp.fecha_programada)
                            FROM prestamo_calendario_pagos cp
                            WHERE cp.prestamo_id = p.id AND cp.estado = 'pendiente'
                        )
                    )
                    ELSE 0
                END AS INTEGER
            ) AS dias_mora,
            (
                SELECT ca.tipo_accion || ': ' || ca.resultado
                FROM cobranza_acciones ca
                WHERE ca.prestamo_id = p.id
                ORDER BY ca.fecha_accion DESC LIMIT 1
            ) AS ultima_gestion,
            (
                SELECT pp.fecha 
                FROM pagos_prestamo pp 
                WHERE pp.prestamo_id = p.id 
                ORDER BY pp.fecha DESC LIMIT 1
            ) AS fecha_ultimo_pago,
            (
                SELECT pp.monto 
                FROM pagos_prestamo pp 
                WHERE pp.prestamo_id = p.id 
                ORDER BY pp.fecha DESC LIMIT 1
            ) AS monto_ultimo_pago
        FROM prestamos p
        JOIN socios s ON p.socio_id = s.id
        WHERE p.estado = 'aprobado' AND p.saldo_pendiente > 0
    '''
    
    try:
        raw_prestamos = db_fetchall(conn, query)
        
        # Filtrar en Python según rango de mora
        prestamos = []
        for p in raw_prestamos:
            d_mora = p['dias_mora']
            
            if rango_mora == 'mora' and d_mora <= 0:
                continue
            elif rango_mora == 'tramo1' and not (1 <= d_mora <= 30):
                continue
            elif rango_mora == 'tramo2' and not (31 <= d_mora <= 60):
                continue
            elif rango_mora == 'tramo3' and not (61 <= d_mora <= 90):
                continue
            elif rango_mora == 'tramo4' and d_mora <= 90:
                continue
                
            prestamos.append(p)
            
        # Calcular agregados
        total_monto_mora = sum(float(p['saldo_pendiente'] or 0) for p in prestamos if p['dias_mora'] > 0)
        total_creditos_mora = sum(1 for p in prestamos if p['dias_mora'] > 0)
        
    except Exception as e:
        flash(f"Error consultando morosidad: {e}", "danger")
        prestamos = []
        total_monto_mora = 0.0
        total_creditos_mora = 0
        
    conn.close()
    
    # Exportación CSV
    if request.path.endswith('/exportar'):
        headers = [
            'No. Préstamo', 'Asociado', 'Código Socio', 'DPI', 'Teléfono', 'Monto Aprobado',
            'Saldo Pendiente', 'Tasa Interés (%)', 'Etapa Cobranza',
            'Cuota Vencida más Antigua', 'Cuotas Vencidas', 'Días de Mora', 
            'Fecha Último Pago', 'Monto Último Pago', 'Última Gestión de Cobranza'
        ]
        rows = []
        for p in prestamos:
            rows.append([
                p['numero'], p['socio_nombre'], p['socio_codigo'], p['socio_dpi'], p['socio_telefono'] or '—', p['monto_aprobado'],
                p['saldo_pendiente'], p['tasa_interes'], p['etapa_cobranza'].upper(),
                formatear_fecha_dmy(p['proxima_cuota_vencida']) if p['proxima_cuota_vencida'] else 'N/A', 
                p['cuotas_vencidas_count'], p['dias_mora'],
                formatear_fecha_dmy(p['fecha_ultimo_pago']) if p['fecha_ultimo_pago'] else 'Ninguno', 
                p['monto_ultimo_pago'] or 0.0,
                p['ultima_gestion'] or 'Ninguna'
            ])
        return exportar_csv('Reporte_Morosidad_Antiguedad', headers, rows)
        
    return render_template(
        'reportes/morosidad.html',
        prestamos=prestamos,
        rango_mora=rango_mora,
        total_monto_mora=total_monto_mora,
        total_creditos_mora=total_creditos_mora
    )

@bp.route('/captaciones/exportar')
@bp.route('/captaciones')
@login_required()
def captaciones():
    conn = get_db()
    
    cuenta_tipo = request.args.get('cuenta_tipo', '')
    estado = request.args.get('estado', 'todos')
    fecha_desde = request.args.get('fecha_desde', (date.today() - timedelta(days=30)).isoformat())
    fecha_hasta = request.args.get('fecha_hasta', date.today().isoformat())
    
    # Query ampliada con DPI y conteo de transacciones operadas en el rango
    query = '''
        SELECT 
            c.id,
            c.numero AS cuenta_numero,
            s.nombre || ' ' || s.apellido AS socio_nombre,
            s.codigo AS socio_codigo,
            s.dpi AS socio_dpi,
            c.tipo AS cuenta_tipo,
            c.saldo,
            c.tasa_interes,
            c.fecha_apertura,
            c.estado,
            (
                SELECT COALESCE(SUM(t.monto), 0)
                FROM transacciones t
                WHERE t.cuenta_id = c.id AND t.tipo = 'interes' AND date(t.fecha) BETWEEN date(?) AND date(?)
            ) AS intereses_pagados,
            (
                SELECT COALESCE(SUM(t.monto), 0)
                FROM transacciones t
                WHERE t.cuenta_id = c.id AND t.tipo = 'ipf' AND date(t.fecha) BETWEEN date(?) AND date(?)
            ) AS ipf_retenido,
            (
                SELECT COUNT(*)
                FROM transacciones t
                WHERE t.cuenta_id = c.id AND date(t.fecha) BETWEEN date(?) AND date(?)
            ) AS txns_count
        FROM cuentas c
        JOIN socios s ON c.socio_id = s.id
        WHERE 1=1
    '''
    # Se repiten los parámetros ya que fecha_desde/hasta se usa 3 veces en subconsultas
    params = [fecha_desde, fecha_hasta, fecha_desde, fecha_hasta, fecha_desde, fecha_hasta]
    
    if cuenta_tipo:
        query += " AND c.tipo = ?"
        params.append(cuenta_tipo)
        
    if estado != 'todos':
        query += " AND c.estado = ?"
        params.append(estado)
        
    query += " ORDER BY c.numero ASC"
    
    try:
        cuentas = db_fetchall(conn, query, params)
        
        # Calcular agregados
        total_ahorros = sum(float(c['saldo'] or 0) for c in cuentas)
        total_interes = sum(float(c['intereses_pagados'] or 0) for c in cuentas)
        total_ipf = sum(float(c['ipf_retenido'] or 0) for c in cuentas)
        
    except Exception as e:
        flash(f"Error consultando captaciones: {e}", "danger")
        cuentas = []
        total_ahorros = total_interes = total_ipf = 0.0
        
    conn.close()
    
    # Exportación CSV
    if request.path.endswith('/exportar'):
        headers = [
            'No. Cuenta', 'Asociado', 'Código Socio', 'DPI', 'Tipo Cuenta', 
            'Saldo Actual', 'Tasa Interés (%)', 'Fecha Apertura', 'Estado',
            'Transacciones en Período', f'Intereses Pagados ({formatear_fecha_dmy(fecha_desde)} al {formatear_fecha_dmy(fecha_hasta)})', 
            f'IPF Retenido ({formatear_fecha_dmy(fecha_desde)} al {formatear_fecha_dmy(fecha_hasta)})'
        ]
        rows = []
        for c in cuentas:
            rows.append([
                c['cuenta_numero'], c['socio_nombre'], c['socio_codigo'], c['socio_dpi'], c['cuenta_tipo'].upper(),
                c['saldo'], c['tasa_interes'], formatear_fecha_dmy(c['fecha_apertura']), c['estado'].upper(),
                c['txns_count'], c['intereses_pagados'], c['ipf_retenido']
            ])
        return exportar_csv('Reporte_Captaciones_Ahorros', headers, rows)
        
    return render_template(
        'reportes/captaciones.html',
        cuentas=cuentas,
        cuenta_tipo=cuenta_tipo,
        estado_filtro=estado,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        total_ahorros=total_ahorros,
        total_interes=total_interes,
        total_ipf=total_ipf
    )

@bp.route('/planillas/exportar')
@bp.route('/planillas')
@login_required()
def planillas():
    conn = get_db()
    
    estado = request.args.get('estado', 'todos')
    frecuencia = request.args.get('frecuencia', 'todos')
    fecha_desde = request.args.get('fecha_desde', '')
    fecha_hasta = request.args.get('fecha_hasta', '')
    
    # Query ampliada con fecha de creación, usuario de creación y de aplicación
    query = '''
        SELECT 
            pm.id,
            pm.nombre AS planilla_nombre,
            pm.tipo AS planilla_tipo,
            pm.fecha_pago,
            pm.frecuencia,
            pm.estado,
            pm.total_monto,
            pm.total_registros,
            pm.fecha_creacion,
            pm.usuario_creacion,
            pm.usuario_aplicacion,
            (
                SELECT COUNT(*)
                FROM planilla_masiva_detalles pmd
                WHERE pmd.planilla_id = pm.id AND pmd.estado = 'pendiente'
            ) AS registros_pendientes,
            (
                SELECT COALESCE(SUM(pmd.monto), 0)
                FROM planilla_masiva_detalles pmd
                WHERE pmd.planilla_id = pm.id AND pmd.estado = 'pendiente'
            ) AS monto_pendiente
        FROM planillas_masivas pm
        WHERE 1=1
    '''
    params = []
    
    if estado != 'todos':
        query += " AND pm.estado = ?"
        params.append(estado)
        
    if frecuencia != 'todos':
        query += " AND pm.frecuencia = ?"
        params.append(frecuencia)
        
    if fecha_desde:
        query += " AND date(pm.fecha_pago) >= date(?)"
        params.append(fecha_desde)
        
    if fecha_hasta:
        query += " AND date(pm.fecha_pago) <= date(?)"
        params.append(fecha_hasta)
        
    query += " ORDER BY pm.fecha_pago DESC"
    
    try:
        planillas_list = db_fetchall(conn, query, params)
        
        # Calcular estadísticas agregadas
        total_recaudado = sum(float(p['total_monto'] or 0) for p in planillas_list if p['estado'] == 'aplicada')
        total_pendiente = sum(float(p['monto_pendiente'] or 0) for p in planillas_list)
        total_registros = sum(int(p['total_registros'] or 0) for p in planillas_list)
        
    except Exception as e:
        flash(f"Error consultando planillas: {e}", "danger")
        planillas_list = []
        total_recaudado = total_pendiente = 0.0
        total_registros = 0
        
    conn.close()
    
    # Exportación CSV
    if request.path.endswith('/exportar'):
        headers = [
            'ID Planilla', 'Nombre Planilla', 'Tipo', 'Fecha Programada',
            'Frecuencia', 'Estado', 'Monto Total', 'Registros Totales', 
            'Registros Pendientes', 'Monto Pendiente de Cobro',
            'Fecha Creación', 'Creado Por', 'Aplicado Por'
        ]
        rows = []
        for p in planillas_list:
            rows.append([
                p['id'], p['planilla_nombre'], p['planilla_tipo'].upper(), formatear_fecha_dmy(p['fecha_pago']),
                p['frecuencia'], p['estado'].upper(), p['total_monto'], p['total_registros'],
                p['registros_pendientes'], p['monto_pendiente'],
                formatear_fecha_dmy(p['fecha_creacion']), p['usuario_creacion'] or 'Sistema', p['usuario_aplicacion'] or '—'
            ])
        return exportar_csv('Reporte_Planillas_Conciliacion', headers, rows)
        
    return render_template(
        'reportes/planillas.html',
        planillas=planillas_list,
        estado_filtro=estado,
        frecuencia_filtro=frecuencia,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        total_recaudado=total_recaudado,
        total_pendiente=total_pendiente,
        total_registros=total_registros
    )

@bp.route('/auditoria/exportar')
@bp.route('/auditoria')
@login_required()
def auditoria():
    conn = get_db()
    
    modulo = request.args.get('modulo', 'todos')
    accion = request.args.get('accion', 'todos')
    fecha_desde = request.args.get('fecha_desde', '')
    fecha_hasta = request.args.get('fecha_hasta', '')
    
    # Query ampliada que incluye la columna datos conteniendo JSON técnico
    query = '''
        SELECT 
            id,
            modulo,
            entidad,
            entidad_id,
            accion,
            descripcion,
            datos,
            usuario,
            fecha
        FROM auditoria_eventos
        WHERE 1=1
    '''
    params = []
    
    if modulo != 'todos':
        query += " AND modulo = ?"
        params.append(modulo)
        
    if accion != 'todos':
        query += " AND accion = ?"
        params.append(accion)
        
    if fecha_desde:
        query += " AND date(fecha) >= date(?)"
        params.append(fecha_desde)
        
    if fecha_hasta:
        query += " AND date(fecha) <= date(?)"
        params.append(fecha_hasta)
        
    query += " ORDER BY fecha DESC LIMIT 500"
    
    try:
        eventos = db_fetchall(conn, query, params)
        modulos = [r['modulo'] for r in db_fetchall(conn, "SELECT DISTINCT modulo FROM auditoria_eventos ORDER BY modulo")]
        acciones = [r['accion'] for r in db_fetchall(conn, "SELECT DISTINCT accion FROM auditoria_eventos ORDER BY accion")]
        
    except Exception as e:
        flash(f"Error consultando auditoría: {e}", "danger")
        eventos = []
        modulos = acciones = []
        
    conn.close()
    
    # Exportación CSV
    if request.path.endswith('/exportar'):
        headers = [
            'ID Evento', 'Módulo', 'Entidad', 'ID Entidad', 
            'Acción', 'Descripción', 'Detalles Técnicos (JSON)', 'Usuario Responsable', 'Fecha y Hora'
        ]
        rows = []
        for e in eventos:
            rows.append([
                e['id'], e['modulo'].upper(), e['entidad'].upper(), e['entidad_id'] or 'N/A',
                e['accion'].upper(), e['descripcion'], e['datos'] or '{}', e['usuario'], formatear_fecha_dmy(e['fecha'])
            ])
        return exportar_csv('Reporte_Auditoria_Cumplimiento', headers, rows)
        
    return render_template(
        'reportes/auditoria.html',
        eventos=eventos,
        modulos=modulos,
        acciones=acciones,
        modulo_filtro=modulo,
        accion_filtro=accion,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta
    )

@bp.route('/socios/exportar')
@bp.route('/socios')
@login_required()
def socios():
    conn = get_db()
    
    # Filtros
    cuenta_tipo = request.args.get('cuenta_tipo', 'todos')
    estado = request.args.get('estado', 'todos')
    frecuencia = request.args.get('frecuencia', 'todos')
    fecha_desde = request.args.get('fecha_desde', '')
    fecha_hasta = request.args.get('fecha_hasta', '')
    
    params = []
    
    # Construcción de la consulta con saldo dinámico
    if fecha_hasta:
        select_clause = '''
            s.id,
            s.codigo,
            s.nombre || ' ' || s.apellido AS nombre_completo,
            s.dpi,
            s.telefono,
            s.email,
            s.direccion,
            s.departamento,
            s.municipio,
            s.rol,
            s.fecha_ingreso,
            s.estado,
            s.frecuencia,
            s.nit,
            s.salario,
            COALESCE(
                (
                    SELECT SUM(
                        COALESCE(
                            (
                                SELECT t.saldo_despues 
                                FROM transacciones t 
                                WHERE t.cuenta_id = c.id 
                                  AND date(t.fecha) <= date(?)
                                ORDER BY t.fecha DESC, t.id DESC 
                                LIMIT 1
                            ),
                            0.0
                        )
                    )
                    FROM cuentas c
                    WHERE c.socio_id = s.id
                      AND (? = 'todos' OR c.tipo = ?)
                ),
                0.0
            ) AS saldo_ahorro
        '''
        params.extend([fecha_hasta, cuenta_tipo, cuenta_tipo])
    else:
        select_clause = '''
            s.id,
            s.codigo,
            s.nombre || ' ' || s.apellido AS nombre_completo,
            s.dpi,
            s.telefono,
            s.email,
            s.direccion,
            s.departamento,
            s.municipio,
            s.rol,
            s.fecha_ingreso,
            s.estado,
            s.frecuencia,
            s.nit,
            s.salario,
            COALESCE(
                (
                    SELECT SUM(c.saldo)
                    FROM cuentas c
                    WHERE c.socio_id = s.id
                      AND (? = 'todos' OR c.tipo = ?)
                ),
                0.0
            ) AS saldo_ahorro
        '''
        params.extend([cuenta_tipo, cuenta_tipo])
        
    query = f'''
        SELECT {select_clause}
        FROM socios s
        WHERE 1=1
    '''
    
    if estado != 'todos':
        query += " AND s.estado = ?"
        params.append(estado)
        
    if frecuencia != 'todos':
        query += " AND s.frecuencia = ?"
        params.append(frecuencia)
        
    if fecha_desde:
        query += " AND date(s.fecha_ingreso) >= date(?)"
        params.append(fecha_desde)
        
    if fecha_hasta:
        query += " AND date(s.fecha_ingreso) <= date(?)"
        params.append(fecha_hasta)
        
    query += " ORDER BY s.codigo ASC"
    
    try:
        asociados_list = db_fetchall(conn, query, params)
        
        # Estadísticas agregadas basándonos en la lista actual filtrada
        total_asociados = len(asociados_list)
        total_activos = sum(1 for s in asociados_list if s['estado'] == 'activo')
        total_inactivos = sum(1 for s in asociados_list if s['estado'] == 'inactivo')
        total_saldo_ahorro = sum(float(s['saldo_ahorro'] or 0) for s in asociados_list)
        
        # Obtener valores para los select de filtros dinámicos (frecuencias distintas)
        frecuencias_disponibles = [f['frecuencia'] for f in db_fetchall(conn, "SELECT DISTINCT frecuencia FROM socios WHERE frecuencia IS NOT NULL AND frecuencia != '' ORDER BY frecuencia")]
        
    except Exception as e:
        flash(f"Error consultando asociados: {e}", "danger")
        asociados_list = []
        total_asociados = total_activos = total_inactivos = 0
        total_saldo_ahorro = 0.0
        frecuencias_disponibles = []
        
    conn.close()
    
    # Si es exportación CSV
    if request.path.endswith('/exportar'):
        headers = [
            'Código', 'Nombre Completo', 'DPI', 'Teléfono', 'Email', 
            'Dirección', 'Departamento', 'Municipio', 
            'Fecha Ingreso', 'Estado', 'Frecuencia Cobro/Ahorro', 
            'Saldo Ahorro', 'NIT', 'Salario'
        ]
        rows = []
        for s in asociados_list:
            rows.append([
                s['codigo'], s['nombre_completo'], s['dpi'], s['telefono'] or '—', s['email'] or '—',
                s['direccion'] or '—', s['departamento'] or '—', s['municipio'] or '—',
                formatear_fecha_dmy(s['fecha_ingreso']), s['estado'].upper(), s['frecuencia'],
                s['saldo_ahorro'], s['nit'] or '—', s['salario'] or 0.0
            ])
        return exportar_csv('Reporte_Asociados', headers, rows)
        
    # Variables de fecha de hoy para el reporte
    from datetime import datetime
    now = datetime.now()
    
    return render_template(
        'reportes/socios.html',
        asociados=asociados_list,
        estado_filtro=estado,
        cuenta_tipo=cuenta_tipo,
        frecuencia_filtro=frecuencia,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        total_asociados=total_asociados,
        total_activos=total_activos,
        total_inactivos=total_inactivos,
        total_saldo_ahorro=total_saldo_ahorro,
        frecuencias=frecuencias_disponibles,
        now=now
    )


