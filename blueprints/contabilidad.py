"""
blueprints/contabilidad.py — Módulo de Contabilidad General
Gestiona la nomenclatura de cuentas contables (catálogo jerárquico estilo Odoo),
asientos de partida doble, libro mayor y balanza de comprobación.
"""
import csv
import io
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, session, jsonify, Response
)
from datetime import date, datetime
from utils.db import (
    get_db, db_execute, db_fetchone, db_fetchall, db_insert_and_get_id
)
from utils.decorators import login_required
from utils.helpers import log_auditoria_evento

bp = Blueprint('contabilidad', __name__, url_prefix='/contabilidad')


# ── Helpers ────────────────────────────────────────────────────────────────────

def _generar_numero_partida(conn):
    """Genera el siguiente número de partida correlativo."""
    row = db_fetchone(conn, "SELECT COUNT(*) FROM cont_partidas")
    n = (row[0] if row else 0) + 1
    return f"PART-{n:06d}"


def _actualizar_saldos_cuenta(conn, cuenta_id):
    """Recalcula el saldo de una cuenta contable a partir de sus apuntes asentados."""
    cuenta = db_fetchone(conn, "SELECT tipo, naturaleza FROM cont_cuentas WHERE id=?", (cuenta_id,))
    if not cuenta:
        return

    totales = db_fetchone(conn, """
        SELECT COALESCE(SUM(a.debe),0) AS total_debe, COALESCE(SUM(a.haber),0) AS total_haber
        FROM cont_apuntes a
        JOIN cont_partidas p ON a.partida_id = p.id
        WHERE a.cuenta_id=? AND p.estado='asentado'
    """, (cuenta_id,))

    debe = float(totales['total_debe'] or 0)
    haber = float(totales['total_haber'] or 0)

    if cuenta['naturaleza'] == 'deudora':
        saldo = debe - haber
    else:
        saldo = haber - debe

    db_execute(conn, "UPDATE cont_cuentas SET saldo=? WHERE id=?", (round(saldo, 2), cuenta_id))

    # Propagar saldo al padre
    padre = db_fetchone(conn, "SELECT parent_id FROM cont_cuentas WHERE id=?", (cuenta_id,))
    if padre and padre['parent_id']:
        _actualizar_saldos_cuenta(conn, padre['parent_id'])


def _construir_arbol_cuentas(conn, periodo_inicio=None, periodo_fin=None):
    """Construye la lista plana de cuentas para renderizar el árbol Odoo."""
    cuentas = db_fetchall(conn, """
        SELECT c.*,
               COALESCE(SUM(CASE WHEN p.estado='asentado' THEN a.debe ELSE 0 END), 0) AS movimiento_debe,
               COALESCE(SUM(CASE WHEN p.estado='asentado' THEN a.haber ELSE 0 END), 0) AS movimiento_haber
        FROM cont_cuentas c
        LEFT JOIN cont_apuntes a ON a.cuenta_id = c.id
        LEFT JOIN cont_partidas p ON a.partida_id = p.id
            AND (? IS NULL OR date(p.fecha) >= date(?))
            AND (? IS NULL OR date(p.fecha) <= date(?))
        WHERE c.estado='activa'
        GROUP BY c.id
        ORDER BY c.codigo
    """, (periodo_inicio, periodo_inicio, periodo_fin, periodo_fin))
    return [dict(c) for c in cuentas]


# ── Rutas ──────────────────────────────────────────────────────────────────────

@bp.route('/')
@login_required()
def dashboard():
    conn = get_db()
    hoy = date.today().isoformat()
    anio = date.today().year
    inicio_anio = f"{anio}-01-01"

    # KPIs rápidos
    partidas_borrador = db_fetchone(conn,
        "SELECT COUNT(*) FROM cont_partidas WHERE estado='borrador'")[0]
    partidas_asentadas = db_fetchone(conn,
        "SELECT COUNT(*) FROM cont_partidas WHERE estado='asentado'")[0]

    total_activo = db_fetchone(conn, """
        SELECT COALESCE(SUM(c.saldo),0) FROM cont_cuentas c WHERE c.tipo='activo' AND c.nivel=1
    """)[0]
    total_pasivo = db_fetchone(conn, """
        SELECT COALESCE(SUM(c.saldo),0) FROM cont_cuentas c WHERE c.tipo='pasivo' AND c.nivel=1
    """)[0]
    total_patrimonio = db_fetchone(conn, """
        SELECT COALESCE(SUM(c.saldo),0) FROM cont_cuentas c WHERE c.tipo='patrimonio' AND c.nivel=1
    """)[0]

    ingresos_anio = db_fetchone(conn, """
        SELECT COALESCE(SUM(a.haber),0) FROM cont_apuntes a
        JOIN cont_partidas p ON a.partida_id=p.id
        JOIN cont_cuentas c ON a.cuenta_id=c.id
        WHERE p.estado='asentado' AND c.tipo='ingreso' AND date(p.fecha) >= date(?)
    """, (inicio_anio,))[0]

    gastos_anio = db_fetchone(conn, """
        SELECT COALESCE(SUM(a.debe),0) FROM cont_apuntes a
        JOIN cont_partidas p ON a.partida_id=p.id
        JOIN cont_cuentas c ON a.cuenta_id=c.id
        WHERE p.estado='asentado' AND c.tipo='gasto' AND date(p.fecha) >= date(?)
    """, (inicio_anio,))[0]

    # Últimas partidas
    ultimas_partidas = db_fetchall(conn, """
        SELECT p.*,
               (SELECT COUNT(*) FROM cont_apuntes WHERE partida_id=p.id) AS num_apuntes
        FROM cont_partidas p ORDER BY p.id DESC LIMIT 8
    """)

    conn.close()
    return render_template('cont_dashboard.html',
        partidas_borrador=partidas_borrador,
        partidas_asentadas=partidas_asentadas,
        total_activo=total_activo,
        total_pasivo=total_pasivo,
        total_patrimonio=total_patrimonio,
        ingresos_anio=ingresos_anio,
        gastos_anio=gastos_anio,
        utilidad_anio=round(float(ingresos_anio) - float(gastos_anio), 2),
        ultimas_partidas=ultimas_partidas,
    )


@bp.route('/nomenclatura')
@login_required()
def nomenclatura():
    conn = get_db()
    periodo_inicio = request.args.get('inicio', '')
    periodo_fin = request.args.get('fin', '')
    tipo_filtro = request.args.get('tipo', '')

    cuentas = _construir_arbol_cuentas(conn, periodo_inicio or None, periodo_fin or None)
    if tipo_filtro:
        # Mantener cuenta y sus padres para coherencia visual
        codigos_tipo = {c['codigo'] for c in cuentas if c['tipo'] == tipo_filtro}
        cuentas = [c for c in cuentas if c['tipo'] == tipo_filtro or
                   any(c['codigo'].startswith(ct[:1]) for ct in codigos_tipo)]

    conn.close()
    return render_template('cont_nomenclatura.html',
        cuentas=cuentas,
        periodo_inicio=periodo_inicio,
        periodo_fin=periodo_fin,
        tipo_filtro=tipo_filtro,
    )


@bp.route('/nomenclatura/nueva', methods=['GET', 'POST'])
@login_required()
def nueva_cuenta():
    conn = get_db()
    if request.method == 'POST':
        try:
            codigo = request.form.get('codigo', '').strip()
            nombre = request.form.get('nombre', '').strip()
            tipo = request.form.get('tipo', '').strip()
            naturaleza = request.form.get('naturaleza', 'deudora').strip()
            parent_id = request.form.get('parent_id') or None
            acepta_mov = 1 if request.form.get('acepta_movimientos') else 0
            descripcion = request.form.get('descripcion', '').strip()

            if not codigo or not nombre or not tipo:
                flash('Código, nombre y tipo son obligatorios.', 'danger')
                cuentas_todas = db_fetchall(conn, "SELECT id, codigo, nombre, nivel FROM cont_cuentas WHERE estado='activa' ORDER BY codigo")
                conn.close()
                return render_template('cont_cuenta_form.html', cuentas=cuentas_todas, cuenta=None)

            # Calcular nivel
            nivel = 1
            if parent_id:
                padre = db_fetchone(conn, "SELECT nivel FROM cont_cuentas WHERE id=?", (parent_id,))
                if padre:
                    nivel = padre['nivel'] + 1

            db_execute(conn, """
                INSERT INTO cont_cuentas
                (codigo, nombre, tipo, naturaleza, parent_id, nivel, acepta_movimientos, descripcion, estado, fecha_creacion)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (codigo, nombre, tipo, naturaleza, parent_id, nivel, acepta_mov,
                  descripcion, 'activa', date.today().isoformat()))
            conn.commit()
            flash('Cuenta contable creada exitosamente.', 'success')
            return redirect(url_for('contabilidad.nomenclatura'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()

    cuentas_todas = db_fetchall(conn, "SELECT id, codigo, nombre, nivel FROM cont_cuentas WHERE estado='activa' ORDER BY codigo")
    conn.close()
    return render_template('cont_cuenta_form.html', cuentas=cuentas_todas, cuenta=None)


@bp.route('/nomenclatura/<int:cid>/editar', methods=['GET', 'POST'])
@login_required()
def editar_cuenta(cid):
    conn = get_db()
    cuenta = db_fetchone(conn, "SELECT * FROM cont_cuentas WHERE id=?", (cid,))
    if not cuenta:
        conn.close()
        flash('Cuenta no encontrada.', 'danger')
        return redirect(url_for('contabilidad.nomenclatura'))

    if request.method == 'POST':
        try:
            nombre = request.form.get('nombre', '').strip()
            naturaleza = request.form.get('naturaleza', 'deudora')
            acepta_mov = 1 if request.form.get('acepta_movimientos') else 0
            descripcion = request.form.get('descripcion', '').strip()
            estado = request.form.get('estado', 'activa')

            db_execute(conn, """
                UPDATE cont_cuentas SET nombre=?, naturaleza=?, acepta_movimientos=?,
                descripcion=?, estado=? WHERE id=?
            """, (nombre, naturaleza, acepta_mov, descripcion, estado, cid))
            conn.commit()
            flash('Cuenta actualizada correctamente.', 'success')
            return redirect(url_for('contabilidad.nomenclatura'))
        except Exception as e:
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()

    cuentas_todas = db_fetchall(conn, "SELECT id, codigo, nombre, nivel FROM cont_cuentas WHERE estado='activa' ORDER BY codigo")
    conn.close()
    return render_template('cont_cuenta_form.html', cuentas=cuentas_todas, cuenta=dict(cuenta))


# ── Asientos Contables ─────────────────────────────────────────────────────────

@bp.route('/asientos')
@login_required()
def asientos():
    conn = get_db()
    fecha_desde = request.args.get('fecha_desde', date.today().replace(day=1).isoformat())
    fecha_hasta = request.args.get('fecha_hasta', date.today().isoformat())
    estado_filtro = request.args.get('estado', '')
    origen_filtro = request.args.get('origen', '')
    q = request.args.get('q', '').strip()

    params = [fecha_desde, fecha_hasta]
    where_extra = ""
    if estado_filtro:
        where_extra += " AND p.estado=?"
        params.append(estado_filtro)
    if origen_filtro:
        where_extra += " AND p.origen_tipo=?"
        params.append(origen_filtro)
    if q:
        where_extra += " AND (p.numero LIKE ? OR p.descripcion LIKE ?)"
        params += [f'%{q}%', f'%{q}%']

    partidas = db_fetchall(conn, f"""
        SELECT p.*,
               (SELECT COUNT(*) FROM cont_apuntes WHERE partida_id=p.id) AS num_apuntes,
               (SELECT COALESCE(SUM(debe),0) FROM cont_apuntes WHERE partida_id=p.id) AS total_debe,
               (SELECT COALESCE(SUM(haber),0) FROM cont_apuntes WHERE partida_id=p.id) AS total_haber
        FROM cont_partidas p
        WHERE date(p.fecha) BETWEEN date(?) AND date(?) {where_extra}
        ORDER BY p.id DESC
    """, params)

    conn.close()
    return render_template('cont_asientos.html',
        partidas=partidas,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        estado_filtro=estado_filtro,
        origen_filtro=origen_filtro,
        q=q,
    )


@bp.route('/asientos/nuevo', methods=['GET', 'POST'])
@login_required()
def nuevo_asiento():
    conn = get_db()
    if request.method == 'POST':
        try:
            fecha = request.form.get('fecha', date.today().isoformat()).strip()
            descripcion = request.form.get('descripcion', '').strip()
            cuentas_ids = request.form.getlist('cuenta_id[]')
            debes = request.form.getlist('debe[]')
            haberes = request.form.getlist('haber[]')
            descs_apunte = request.form.getlist('descripcion_apunte[]')

            if not descripcion:
                flash('La descripción es obligatoria.', 'danger')
                raise ValueError('Descripción requerida')

            total_debe = sum(float(d or 0) for d in debes)
            total_haber = sum(float(h or 0) for h in haberes)

            if abs(total_debe - total_haber) > 0.01:
                flash(f'El asiento no cuadra. Debe: Q{total_debe:.2f} / Haber: Q{total_haber:.2f}', 'danger')
                raise ValueError('Asiento descuadrado')

            numero = _generar_numero_partida(conn)
            usuario = session.get('username', 'sistema')

            partida_id = db_insert_and_get_id(conn, """
                INSERT INTO cont_partidas (numero, fecha, descripcion, estado, origen_tipo, usuario, fecha_creacion)
                VALUES (?,?,?,?,?,?,?)
            """, (numero, fecha, descripcion, 'borrador', 'manual', usuario, datetime.now().isoformat()))

            for cid, d, h, desc_ap in zip(cuentas_ids, debes, haberes, descs_apunte):
                if not cid:
                    continue
                db_execute(conn, """
                    INSERT INTO cont_apuntes (partida_id, cuenta_id, descripcion, debe, haber)
                    VALUES (?,?,?,?,?)
                """, (partida_id, cid, desc_ap, float(d or 0), float(h or 0)))

            conn.commit()

            log_auditoria_evento(
                modulo='contabilidad', entidad='partida', accion='crear',
                entidad_id=partida_id,
                descripcion=f"Asiento manual {numero}: {descripcion}",
            )
            flash(f'Asiento {numero} creado correctamente en estado Borrador.', 'success')
            return redirect(url_for('contabilidad.asientos'))
        except ValueError:
            conn.rollback()
        except Exception as e:
            conn.rollback()
            flash(f'Error: {e}', 'danger')
        finally:
            conn.close()

    cuentas_detalle = db_fetchall(conn, """
        SELECT id, codigo, nombre, tipo, naturaleza
        FROM cont_cuentas WHERE estado='activa' AND acepta_movimientos=1 ORDER BY codigo
    """)
    cuentas_dict = [dict(row) for row in cuentas_detalle]
    conn.close()
    return render_template('cont_asiento_form.html',
        cuentas=cuentas_dict,
        hoy=date.today().isoformat()
    )


@bp.route('/asientos/<int:pid>')
@login_required()
def detalle_asiento(pid):
    conn = get_db()
    partida = db_fetchone(conn, "SELECT * FROM cont_partidas WHERE id=?", (pid,))
    if not partida:
        conn.close()
        flash('Partida no encontrada.', 'danger')
        return redirect(url_for('contabilidad.asientos'))
    apuntes = db_fetchall(conn, """
        SELECT a.*, c.codigo AS cuenta_codigo, c.nombre AS cuenta_nombre, c.tipo AS cuenta_tipo
        FROM cont_apuntes a JOIN cont_cuentas c ON a.cuenta_id=c.id WHERE a.partida_id=?
        ORDER BY a.id
    """, (pid,))
    conn.close()
    return render_template('cont_detalle_asiento.html', partida=dict(partida), apuntes=apuntes)


@bp.route('/asientos/<int:pid>/asentar', methods=['POST'])
@login_required()
def asentar_partida(pid):
    conn = get_db()
    try:
        partida = db_fetchone(conn, "SELECT * FROM cont_partidas WHERE id=?", (pid,))
        if not partida:
            flash('Partida no encontrada.', 'danger')
            return redirect(url_for('contabilidad.asientos'))
        if partida['estado'] == 'asentado':
            flash('Esta partida ya fue asentada.', 'warning')
            return redirect(url_for('contabilidad.asientos'))

        # Verificar cuadre
        totales = db_fetchone(conn, """
            SELECT COALESCE(SUM(debe),0) AS td, COALESCE(SUM(haber),0) AS th
            FROM cont_apuntes WHERE partida_id=?
        """, (pid,))
        if abs(float(totales['td']) - float(totales['th'])) > 0.01:
            flash('No se puede asentar: el asiento no cuadra (Debe ≠ Haber).', 'danger')
            return redirect(url_for('contabilidad.detalle_asiento', pid=pid))

        db_execute(conn, "UPDATE cont_partidas SET estado='asentado' WHERE id=?", (pid,))

        # Actualizar saldos de cuentas involucradas
        apuntes = db_fetchall(conn, "SELECT DISTINCT cuenta_id FROM cont_apuntes WHERE partida_id=?", (pid,))
        for ap in apuntes:
            _actualizar_saldos_cuenta(conn, ap['cuenta_id'])

        conn.commit()
        log_auditoria_evento(
            modulo='contabilidad', entidad='partida', accion='asentar',
            entidad_id=pid,
            descripcion=f"Partida {partida['numero']} asentada.",
        )
        flash(f"Partida {partida['numero']} asentada correctamente.", 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('contabilidad.asientos'))


@bp.route('/asientos/<int:pid>/anular', methods=['POST'])
@login_required()
def anular_partida(pid):
    conn = get_db()
    try:
        partida = db_fetchone(conn, "SELECT * FROM cont_partidas WHERE id=?", (pid,))
        if not partida:
            flash('Partida no encontrada.', 'danger')
            return redirect(url_for('contabilidad.asientos'))
        db_execute(conn, "UPDATE cont_partidas SET estado='anulado' WHERE id=?", (pid,))
        # Recalcular saldos
        apuntes = db_fetchall(conn, "SELECT DISTINCT cuenta_id FROM cont_apuntes WHERE partida_id=?", (pid,))
        for ap in apuntes:
            _actualizar_saldos_cuenta(conn, ap['cuenta_id'])
        conn.commit()
        flash(f"Partida {partida['numero']} anulada.", 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error: {e}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('contabilidad.asientos'))


# ── Libro Mayor ────────────────────────────────────────────────────────────────

@bp.route('/libro_mayor')
@login_required()
def libro_mayor():
    conn = get_db()
    cuenta_id = request.args.get('cuenta_id', '')
    fecha_desde = request.args.get('fecha_desde', date.today().replace(day=1).isoformat())
    fecha_hasta = request.args.get('fecha_hasta', date.today().isoformat())

    cuentas_detalle = db_fetchall(conn, """
        SELECT id, codigo, nombre, tipo FROM cont_cuentas
        WHERE estado='activa' AND acepta_movimientos=1 ORDER BY codigo
    """)

    cuenta_sel = None
    movimientos = []
    saldo_anterior = 0.0
    saldo_acumulado = 0.0

    if cuenta_id:
        cuenta_sel = db_fetchone(conn, "SELECT * FROM cont_cuentas WHERE id=?", (cuenta_id,))
        if cuenta_sel:
            # Saldo anterior al período
            saldo_ant_row = db_fetchone(conn, """
                SELECT COALESCE(SUM(a.debe),0) AS td, COALESCE(SUM(a.haber),0) AS th
                FROM cont_apuntes a
                JOIN cont_partidas p ON a.partida_id=p.id
                WHERE a.cuenta_id=? AND p.estado='asentado' AND date(p.fecha) < date(?)
            """, (cuenta_id, fecha_desde))
            td = float(saldo_ant_row['td'] or 0)
            th = float(saldo_ant_row['th'] or 0)
            if cuenta_sel['naturaleza'] == 'deudora':
                saldo_anterior = td - th
            else:
                saldo_anterior = th - td

            saldo_acumulado = saldo_anterior
            movs_raw = db_fetchall(conn, """
                SELECT a.*, p.numero AS partida_numero, p.fecha, p.descripcion AS partida_desc,
                       p.origen_tipo
                FROM cont_apuntes a
                JOIN cont_partidas p ON a.partida_id=p.id
                WHERE a.cuenta_id=? AND p.estado='asentado'
                  AND date(p.fecha) BETWEEN date(?) AND date(?)
                ORDER BY p.fecha, p.id, a.id
            """, (cuenta_id, fecha_desde, fecha_hasta))

            for m in movs_raw:
                d = float(m['debe'] or 0)
                h = float(m['haber'] or 0)
                if cuenta_sel['naturaleza'] == 'deudora':
                    saldo_acumulado += d - h
                else:
                    saldo_acumulado += h - d
                mov_dict = dict(m)
                mov_dict['saldo'] = round(saldo_acumulado, 2)
                movimientos.append(mov_dict)

    conn.close()
    return render_template('cont_libro_mayor.html',
        cuentas=cuentas_detalle,
        cuenta_id=cuenta_id,
        cuenta_sel=dict(cuenta_sel) if cuenta_sel else None,
        movimientos=movimientos,
        saldo_anterior=round(saldo_anterior, 2),
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
    )


# ── Balanza de Comprobación ────────────────────────────────────────────────────

@bp.route('/balanza')
@login_required()
def balanza():
    conn = get_db()
    fecha_desde = request.args.get('fecha_desde', date.today().replace(day=1).isoformat())
    fecha_hasta = request.args.get('fecha_hasta', date.today().isoformat())
    exportar = request.args.get('exportar') == '1'

    cuentas = db_fetchall(conn, """
        SELECT c.id, c.codigo, c.nombre, c.tipo, c.naturaleza, c.nivel,
               COALESCE(SUM(CASE WHEN p.estado='asentado' AND date(p.fecha) BETWEEN date(?) AND date(?) THEN a.debe ELSE 0 END),0) AS movimiento_debe,
               COALESCE(SUM(CASE WHEN p.estado='asentado' AND date(p.fecha) BETWEEN date(?) AND date(?) THEN a.haber ELSE 0 END),0) AS movimiento_haber
        FROM cont_cuentas c
        LEFT JOIN cont_apuntes a ON a.cuenta_id=c.id
        LEFT JOIN cont_partidas p ON a.partida_id=p.id
        WHERE c.estado='activa'
        GROUP BY c.id
        HAVING movimiento_debe > 0 OR movimiento_haber > 0
        ORDER BY c.codigo
    """, (fecha_desde, fecha_hasta, fecha_desde, fecha_hasta))

    filas = []
    total_debe = 0.0
    total_haber = 0.0
    for c in cuentas:
        d = float(c['movimiento_debe'] or 0)
        h = float(c['movimiento_haber'] or 0)
        if c['naturaleza'] == 'deudora':
            saldo_d = round(max(0, d - h), 2)
            saldo_h = 0.0
        else:
            saldo_d = 0.0
            saldo_h = round(max(0, h - d), 2)
        total_debe += d
        total_haber += h
        filas.append({**dict(c), 'saldo_deudor': saldo_d, 'saldo_acreedor': saldo_h})

    conn.close()

    if exportar:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Código', 'Nombre', 'Tipo', 'Debe', 'Haber', 'Saldo Deudor', 'Saldo Acreedor'])
        for f in filas:
            writer.writerow([f['codigo'], f['nombre'], f['tipo'],
                             f['movimiento_debe'], f['movimiento_haber'],
                             f['saldo_deudor'], f['saldo_acreedor']])
        writer.writerow(['', 'TOTALES', '', round(total_debe, 2), round(total_haber, 2), '', ''])
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename=Balanza_{fecha_desde}_{fecha_hasta}.csv'}
        )

    return render_template('cont_balanza.html',
        filas=filas,
        total_debe=round(total_debe, 2),
        total_haber=round(total_haber, 2),
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
    )


# ── API JSON ───────────────────────────────────────────────────────────────────

@bp.route('/api/cuentas')
@login_required()
def api_cuentas():
    """Devuelve lista de cuentas de detalle para formularios dinámicos."""
    conn = get_db()
    solo_detalle = request.args.get('detalle', '1') == '1'
    q = request.args.get('q', '').strip()
    where = "WHERE estado='activa'"
    params = []
    if solo_detalle:
        where += " AND acepta_movimientos=1"
    if q:
        where += " AND (codigo LIKE ? OR nombre LIKE ?)"
        params += [f'%{q}%', f'%{q}%']
    cuentas = db_fetchall(conn, f"SELECT id, codigo, nombre, tipo, naturaleza FROM cont_cuentas {where} ORDER BY codigo", params)
    conn.close()
    return jsonify([dict(c) for c in cuentas])
