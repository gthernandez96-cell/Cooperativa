from flask import Blueprint, render_template, g, redirect, url_for, session
from datetime import date, timedelta
from utils.db import get_db, db_fetchone, db_fetchall, get_system_setting
from utils.decorators import login_required
from utils.financial import obtener_dias_frecuencia, calcular_proximo_pago

bp = Blueprint('main', __name__)

def _calcular_alertas_cuotas(conn):
    """Retorna la lista de préstamos cuya próxima cuota cae en los próximos 7 días."""
    hoy = date.today()
    limite = hoy + timedelta(days=7)

    prestamos = db_fetchall(conn, '''
        SELECT p.id, p.numero, p.cuota_mensual, p.saldo_pendiente,
               s.nombre || ' ' || s.apellido AS socio_nombre,
               s.frecuencia,
               COALESCE(MAX(pp.fecha), p.fecha_aprobacion) AS ultimo_pago
        FROM prestamos p
        JOIN socios s ON s.id = p.socio_id
        LEFT JOIN pagos_prestamo pp ON pp.prestamo_id = p.id
        WHERE p.estado = 'aprobado' AND p.saldo_pendiente > 0
        GROUP BY p.id
    ''')

    alertas = []
    for p in prestamos:
        try:
            proximo = calcular_proximo_pago(p['ultimo_pago'], p['frecuencia'])
            proximo_date = proximo.date() if hasattr(proximo, 'date') else proximo
            if hoy <= proximo_date <= limite:
                alertas.append({
                    'id': p['id'],
                    'numero': p['numero'],
                    'socio': p['socio_nombre'],
                    'frecuencia': p['frecuencia'],
                    'cuota': float(p['cuota_mensual'] or 0),
                    'saldo': float(p['saldo_pendiente'] or 0),
                    'proximo_pago': proximo_date.isoformat(),
                    'dias_restantes': (proximo_date - hoy).days,
                })
        except Exception:
            continue
    alertas.sort(key=lambda x: x['dias_restantes'])
    return alertas


@bp.route('/')
@login_required()
def index():
    role = session.get('user_role', '').lower()
    if role == 'promotora':
        return redirect(url_for('promotora.dashboard'))

    # Administradores ven el portal de módulos si no tienen módulo activo
    modulo_activo = session.get('active_module')
    if not modulo_activo:
        if role == 'administrador':
            return render_template('portal.html')
        else:
            # Operadores y otros van directo a ahorros
            session['active_module'] = 'ahorro_credito'
            modulo_activo = 'ahorro_credito'

    if modulo_activo == 'pos':
        return redirect(url_for('pos.dashboard'))
    if modulo_activo == 'contabilidad':
        return redirect(url_for('contabilidad.dashboard'))

    # Dashboard de Ahorros y Créditos (comportamiento original)
    conn = get_db()
    etiquetas_ahorro = {
        'ahorro_aportacion': 'Aportación',
        'ahorro_corriente': 'Ahorro corriente',
        'ahorro_plazo_fijo': 'Plazo fijo',
    }
    hoy = date.today().isoformat()

    stats = {
        'total_socios': db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE estado='activo'")[0],
        'total_cuentas': db_fetchone(conn, "SELECT COUNT(*) FROM cuentas WHERE estado='activa'")[0],
        'total_ahorros': db_fetchone(conn, "SELECT COALESCE(SUM(saldo),0) FROM cuentas WHERE estado='activa'")[0],
        'prestamos_activos': db_fetchone(conn, "SELECT COUNT(*) FROM prestamos WHERE estado='aprobado'")[0],
        'cartera_prestamos': db_fetchone(conn, "SELECT COALESCE(SUM(saldo_pendiente),0) FROM prestamos WHERE estado='aprobado'")[0],
        'prestamos_pendientes': db_fetchone(conn, "SELECT COUNT(*) FROM prestamos WHERE estado='pendiente'")[0],
        'socios_catorcenal': db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE estado='activo' AND frecuencia='Catorcenal'")[0],
        'socios_quincenal': db_fetchone(conn, "SELECT COUNT(*) FROM socios WHERE estado='activo' AND frecuencia='Quincenal'")[0],
    }

    # ── Mora ────────────────────────────────────────────────────────────────────
    mora_row = db_fetchone(conn, '''
        SELECT COUNT(*) AS total_mora,
               COALESCE(SUM(p.saldo_pendiente), 0) AS monto_mora
        FROM prestamos p
        WHERE p.estado = 'aprobado'
          AND p.saldo_pendiente > 0
          AND p.etapa_cobranza IN ('intensiva', 'legal')
    ''')
    stats['prestamos_en_mora'] = mora_row['total_mora'] if mora_row else 0
    stats['monto_mora'] = float(mora_row['monto_mora'] if mora_row else 0)

    # ── Actividad del día ────────────────────────────────────────────────────────
    pagos_hoy = db_fetchone(conn, '''
        SELECT COUNT(*) AS total, COALESCE(SUM(monto), 0) AS monto
        FROM pagos_prestamo WHERE date(fecha) = date(?)
    ''', (hoy,))
    stats['pagos_hoy_count'] = pagos_hoy['total'] if pagos_hoy else 0
    stats['pagos_hoy_monto'] = float(pagos_hoy['monto'] if pagos_hoy else 0)

    depositos_hoy = db_fetchone(conn, '''
        SELECT COUNT(*) AS total, COALESCE(SUM(monto), 0) AS monto
        FROM transacciones WHERE tipo = 'deposito' AND date(fecha) = date(?)
    ''', (hoy,))
    stats['depositos_hoy_count'] = depositos_hoy['total'] if depositos_hoy else 0
    stats['depositos_hoy_monto'] = float(depositos_hoy['monto'] if depositos_hoy else 0)

    # ── Ahorro por categoría ─────────────────────────────────────────────────────
    ahorro_por_categoria = db_fetchall(conn, '''
        SELECT COALESCE(producto_ahorro, 'ahorro_corriente') AS categoria,
               COALESCE(SUM(saldo), 0) AS total
        FROM cuentas
        WHERE estado='activa' AND tipo='ahorro'
        GROUP BY COALESCE(producto_ahorro, 'ahorro_corriente')
        ORDER BY CASE COALESCE(producto_ahorro, 'ahorro_corriente')
            WHEN 'ahorro_aportacion' THEN 1
            WHEN 'ahorro_corriente' THEN 2
            WHEN 'ahorro_plazo_fijo' THEN 3
            ELSE 99 END
    ''')
    stats['ahorro_por_categoria'] = [
        {'nombre': etiquetas_ahorro.get(row['categoria'], (row['categoria'] or 'Otro').replace('_', ' ').title()),
         'total': float(row['total'] or 0)}
        for row in ahorro_por_categoria
    ]

    # ── Préstamos por categoría ──────────────────────────────────────────────────
    prestamos_por_categoria = db_fetchall(conn, '''
        SELECT COALESCE(pc.nombre, 'General') AS categoria,
               COALESCE(SUM(p.saldo_pendiente), 0) AS total
        FROM prestamos p
        LEFT JOIN prestamo_categorias pc ON pc.id = p.categoria_id
        WHERE p.estado='aprobado'
        GROUP BY COALESCE(pc.nombre, 'General')
        ORDER BY categoria
    ''')
    stats['prestamos_por_categoria'] = [
        {'nombre': row['categoria'] or 'General', 'total': float(row['total'] or 0)}
        for row in prestamos_por_categoria
    ]

    # ── Alertas de cuotas próximas ───────────────────────────────────────────────
    alertas_cuotas = _calcular_alertas_cuotas(conn)
    stats['alertas_cuotas_count'] = len(alertas_cuotas)

    # ── Últimas transacciones ────────────────────────────────────────────────────
    ultimas_txn = db_fetchall(conn, '''
        SELECT t.*, c.numero as cuenta_num, s.nombre||' '||s.apellido as socio
        FROM transacciones t
        JOIN cuentas c ON t.cuenta_id=c.id
        JOIN socios s ON c.socio_id=s.id
        ORDER BY t.id DESC LIMIT 5
    ''')
    conn.close()

    return render_template(
        'index.html',
        stats=stats,
        transacciones=ultimas_txn,
        alertas_cuotas=alertas_cuotas,
    )


@bp.route('/calculadora')
@login_required()
def calculadora():
    """Calculadora de préstamo: simula cuotas y amortización."""
    conn = get_db()
    try:
        # Obtener la tasa de interés default de la configuración
        tasa_default = db_fetchone(conn, "SELECT tasa_interes FROM configuraciones WHERE tipo='prestamo_personal' LIMIT 1")
        if not tasa_default:
            tasa_default = db_fetchone(conn, "SELECT tasa_interes FROM configuraciones ORDER BY id LIMIT 1")
        tasa_val = float(tasa_default['tasa_interes']) if tasa_default and tasa_default['tasa_interes'] else 18.0
    except Exception:
        tasa_val = 18.0
    finally:
        conn.close()

    return render_template('calculadora.html', tasa_default=tasa_val)


@bp.route('/seleccionar_modulo/<modulo>')
@login_required()
def seleccionar_modulo(modulo):
    """Guarda el módulo activo en sesión y redirige al dashboard correspondiente."""
    modulos_validos = ['ahorro_credito', 'pos', 'contabilidad', 'ajustes']
    if modulo not in modulos_validos:
        return redirect(url_for('main.index'))
    session['active_module'] = modulo
    session.modified = True
    if modulo == 'pos':
        return redirect(url_for('pos.dashboard'))
    elif modulo == 'contabilidad':
        return redirect(url_for('contabilidad.dashboard'))
    elif modulo == 'ajustes':
        return redirect(url_for('usuarios.usuarios'))
    return redirect(url_for('main.index'))


@bp.route('/portal')
@login_required()
def portal():
    """Pantalla del selector de módulos (App Switcher)."""
    session.pop('active_module', None)
    session.modified = True
    return render_template('portal.html')
