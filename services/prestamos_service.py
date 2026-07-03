from utils.db import db_execute, db_insert_and_get_id, db_fetchone

def calcular_pago_distribucion(monto_pago, saldo_pendiente, tasa_interes_anual, dias_frecuencia):
    """
    Calcula la distribución de un pago entre capital e interés.
    """
    tasa_periodica = (tasa_interes_anual / 100) * (dias_frecuencia / 365)
    interes = round(saldo_pendiente * tasa_periodica, 2)
    capital = round(monto_pago - interes, 2)
    
    if capital <= 0:
        capital = 0
        interes = monto_pago
        
    nuevo_saldo = round(max(0, saldo_pendiente - capital), 2)
    
    return {
        'capital': capital,
        'interes': interes,
        'nuevo_saldo': nuevo_saldo
    }

def procesar_pago_db(conn, prestamo, monto_pago, fecha_pago, boleta, fecha_boleta, numero_comprobante, dias_frecuencia, session_username):
    """
    Registra el pago en la base de datos de manera atómica.
    """
    distribucion = calcular_pago_distribucion(monto_pago, prestamo['saldo_pendiente'], prestamo['tasa_interes'], dias_frecuencia)
    
    pago_id = db_insert_and_get_id(
        conn,
        "INSERT INTO pagos_prestamo (prestamo_id, monto, capital, interes, saldo_restante, fecha, boleta_deposito) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [prestamo['id'], monto_pago, distribucion['capital'], distribucion['interes'], distribucion['nuevo_saldo'], fecha_pago, boleta]
    )
    
    estado = 'pagado' if distribucion['nuevo_saldo'] <= 0 else 'activo'
    
    db_execute(
        conn,
        "UPDATE prestamos SET saldo_pendiente=?, estado=? WHERE id=?",
        [distribucion['nuevo_saldo'], estado, prestamo['id']]
    )
    
    # Marcar cuota como pagada (Simplificado)
    db_execute(
        conn,
        """
        UPDATE prestamo_calendario_pagos 
        SET estado='pagado' 
        WHERE prestamo_id=? AND estado='pendiente' 
        AND id = (SELECT id FROM prestamo_calendario_pagos WHERE prestamo_id=? AND estado='pendiente' ORDER BY numero_cuota ASC LIMIT 1)
        """,
        [prestamo['id'], prestamo['id']]
    )
    
    # Movimiento contable
    db_execute(conn, "INSERT INTO cont_movimientos (cuenta_id, tipo, monto, referencia, fecha) VALUES (?, ?, ?, ?, ?)", 
               (1, 'ingreso', monto_pago, f"Pago PR-{prestamo['id']}", fecha_pago))
    
    return distribucion, pago_id, estado
