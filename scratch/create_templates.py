import os

files = {
    'templates/planillas_prestamos_pendientes.html': 'templates/pos_planillas.html',
    'templates/generar_planilla_prestamos.html': 'templates/pos_generar_planilla.html',
    'templates/planilla_prestamos.html': 'templates/pos_detalle_planilla.html'
}

for src, dst in files.items():
    with open(src, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Reemplazos básicos
    content = content.replace('url_for(\'prestamos.planillas_prestamos_pendientes\'', 'url_for(\'pos.planillas_credito_pos\'')
    content = content.replace('url_for(\'prestamos.generar_planilla_prestamos\'', 'url_for(\'pos.generar_planilla_pos\'')
    content = content.replace('url_for(\'prestamos.detalle_planilla_prestamos\'', 'url_for(\'pos.detalle_planilla_pos\'')
    
    # planillas_prestamos_pendientes.html -> pos_planillas.html
    content = content.replace('Planillas de Préstamos', 'Planillas de Créditos POS')
    content = content.replace('Gestione las planillas de descuentos de préstamos para asociados', 'Gestione las planillas de descuentos masivos de créditos POS')
    content = content.replace('Nueva Planilla de Préstamos', 'Nueva Planilla Crédito POS')
    content = content.replace('planillas de préstamos guardadas', 'planillas de créditos guardadas')
    content = content.replace('planilla de préstamos para comenzar', 'planilla de créditos para comenzar')
    
    # generar_planilla_prestamos.html -> pos_generar_planilla.html
    content = content.replace('Genere una nueva nómina masiva con préstamos activos', 'Genere una nueva nómina masiva con saldos de créditos POS')
    content = content.replace('únicamente préstamos que tengan estado **Aprobado** y **Saldo Pendiente mayor a cero**', 'únicamente asociados con **Saldo Crédito POS mayor a cero**')
    
    # planilla_prestamos.html -> pos_detalle_planilla.html
    content = content.replace('Planilla de Préstamos:', 'Planilla Crédito POS:')
    content = content.replace('No. Préstamo', 'Cód. Socio')
    content = content.replace('Monto Aprobado', 'Crédito Utilizado (Total)')
    content = content.replace('Saldo Pendiente', 'Saldo Restante')
    content = content.replace('Cuota Mensual', 'Cuota Programada')
    content = content.replace('procesar_pagos_masivos', 'procesar_planilla_pos')
    content = content.replace('url_for(\'prestamos.procesar_pagos_masivos\')', 'url_for(\'pos.procesar_planilla_pos\')')
    
    # Fixing variables on planilla_prestamos -> pos_detalle_planilla
    # In prestamos, they use p.monto_aprobado. In POS we just use d.monto. Wait, what variables do we have in pos_detalle_planilla?
    # d.socio_codigo, d.socio_nombre, d.monto, d.estado, d.saldo_actual (from query)
    
    with open(dst, 'w', encoding='utf-8') as f:
        f.write(content)

print("Templates created successfully.")
