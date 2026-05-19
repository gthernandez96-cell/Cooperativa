from flask import Blueprint, render_template, request, flash
from utils.db import get_db, db_fetchone, db_fetchall
from utils.decorators import login_required

bp = Blueprint('promotora', __name__, url_prefix='/promotora')


@bp.route('/', methods=['GET', 'POST'])
@login_required(['Promotora', 'Administrador'])
def dashboard():
    conn = get_db()
    socio = None
    cuentas = []
    categorias_prestamos = {}   # { nombre_categoria: [prestamo, ...] }

    codigo = request.args.get('codigo', '').strip()
    if request.method == 'POST':
        codigo = request.form.get('codigo', '').strip()

    if codigo:
        socio = db_fetchone(
            conn,
            "SELECT id, codigo, nombre, apellido, estado FROM socios WHERE codigo = ?",
            (codigo,)
        )

        if socio:
            # ── Cuentas de ahorro con últimos 5 movimientos ────────────────
            cuentas_raw = db_fetchall(
                conn,
                "SELECT id, numero, tipo, producto_ahorro, saldo, estado FROM cuentas WHERE socio_id = ?",
                (socio['id'],)
            )
            for c in cuentas_raw:
                cuenta_dict = dict(c)
                txns = db_fetchall(
                    conn,
                    "SELECT fecha, tipo, monto, saldo_despues, descripcion FROM transacciones WHERE cuenta_id = ? ORDER BY id DESC LIMIT 5",
                    (c['id'],)
                )
                cuenta_dict['transacciones'] = txns
                cuentas.append(cuenta_dict)

            # ── Préstamos aprobados o activos, agrupados por categoría ─────
            prestamos_raw = db_fetchall(
                conn,
                '''
                SELECT p.id, p.numero, p.monto_aprobado, p.saldo_pendiente,
                       p.cuota_mensual, p.plazo_meses, p.tasa_interes, p.estado,
                       COALESCE(TRIM(pc.nombre), 'Sin categoría') AS categoria
                FROM prestamos p
                LEFT JOIN prestamo_categorias pc ON p.categoria_id = pc.id
                WHERE p.socio_id = ?
                  AND p.estado IN ('aprobado', 'activo', 'vigente')
                ORDER BY categoria, p.id DESC
                ''',
                (socio['id'],)
            )
            for p in prestamos_raw:
                prestamo_dict = dict(p)
                pagos = db_fetchall(
                    conn,
                    "SELECT fecha, monto, capital, interes, saldo_restante, descripcion FROM pagos_prestamo WHERE prestamo_id = ? ORDER BY id DESC LIMIT 5",
                    (p['id'],)
                )
                prestamo_dict['pagos'] = pagos
                cat = prestamo_dict['categoria']
                categorias_prestamos.setdefault(cat, []).append(prestamo_dict)

        else:
            flash('Asociado no encontrado con el código proporcionado.', 'danger')

    conn.close()
    return render_template(
        'promotora.html',
        socio=socio,
        cuentas=cuentas,
        categorias_prestamos=categorias_prestamos,
        codigo=codigo,
    )
