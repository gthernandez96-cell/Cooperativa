from flask import Blueprint, render_template
from utils.db import get_db, db_fetchone, db_fetchall
from utils.decorators import login_required

bp = Blueprint('main', __name__)

@bp.route('/')
@login_required()
def index():
    conn = get_db()
    etiquetas_ahorro = {
        'ahorro_aportacion': 'Aportación',
        'ahorro_corriente': 'Ahorro corriente',
        'ahorro_plazo_fijo': 'Plazo fijo',
    }
    stats = {
        'total_socios': db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE estado='activo'")[0],
        'total_cuentas': db_fetchone(conn, "SELECT COUNT(*) FROM cuentas WHERE estado='activa'")[0],
        'total_ahorros': db_fetchone(conn, "SELECT COALESCE(SUM(saldo),0) FROM cuentas WHERE estado='activa'")[0],
        'prestamos_activos': db_fetchone(conn, "SELECT COUNT(*) FROM prestamos WHERE estado='aprobado'")[0],
        'cartera_prestamos': db_fetchone(conn, "SELECT COALESCE(SUM(saldo_pendiente),0) FROM prestamos WHERE estado='aprobado'")[0],
        'prestamos_pendientes': db_fetchone(conn, "SELECT COUNT(*) FROM prestamos WHERE estado='pendiente'")[0],
    }

    ahorro_por_categoria = db_fetchall(
        conn,
        '''
        SELECT COALESCE(producto_ahorro, 'ahorro_corriente') AS categoria,
               COALESCE(SUM(saldo), 0) AS total
        FROM cuentas
        WHERE estado='activa' AND tipo='ahorro'
        GROUP BY COALESCE(producto_ahorro, 'ahorro_corriente')
        ORDER BY CASE COALESCE(producto_ahorro, 'ahorro_corriente')
            WHEN 'ahorro_aportacion' THEN 1
            WHEN 'ahorro_corriente' THEN 2
            WHEN 'ahorro_plazo_fijo' THEN 3
            ELSE 99
        END
        '''
    )
    stats['ahorro_por_categoria'] = [
        {
            'nombre': etiquetas_ahorro.get(row['categoria'], (row['categoria'] or 'Otro').replace('_', ' ').title()),
            'total': float(row['total'] or 0),
        }
        for row in ahorro_por_categoria
    ]

    prestamos_por_categoria = db_fetchall(
        conn,
        '''
        SELECT COALESCE(pc.nombre, 'General') AS categoria,
               COALESCE(SUM(p.saldo_pendiente), 0) AS total
        FROM prestamos p
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        WHERE p.estado='aprobado'
        GROUP BY COALESCE(pc.nombre, 'General')
        ORDER BY categoria
        '''
    )
    stats['prestamos_por_categoria'] = [
        {
            'nombre': row['categoria'] or 'General',
            'total': float(row['total'] or 0),
        }
        for row in prestamos_por_categoria
    ]
    
# Estadísticas simples de préstamos
    stats['pagos_prestamos_hoy'] = 0  # Por ahora 0, se puede calcular más tarde si es necesario
    stats['monto_pagos_prestamos_hoy'] = 0.0
    
    # Socios por frecuencia
    stats['socios_catorcenal'] = db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE estado='activo' AND frecuencia='Catorcenal'")[0]
    stats['socios_quincenal'] = db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE estado='activo' AND frecuencia='Quincenal'")[0]
    
    ultimas_txn = db_fetchall(conn, '''
        SELECT t.*, c.numero as cuenta_num, s.nombre||' '||s.apellido as socio
        FROM transacciones t
        JOIN cuentas c ON t.cuenta_id=c.id
        JOIN socios s ON c.socio_id=s.id
        ORDER BY t.id DESC LIMIT 5
    ''')
    conn.close()
    return render_template('index.html', stats=stats, transacciones=ultimas_txn)
