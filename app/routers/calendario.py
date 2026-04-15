"""
FASE 2 — Solver de Calendario (OR-Tools CP-SAT) v3
MODELO DE SLOTS FIJOS: 20 talleres fijos por semana (14 EF + 6 IT).

Cambio fundamental respecto a v2:
  - Cada taller tiene día + horario fijo (no flexible)
  - Cada semana repite los mismos 20 slots
  - El solver decide QUÉ EMPRESA va a cada slot de cada semana
  - Post-proceso asigna ciudades (talleres ya están fijos)

Variables de decisión:
  assign[empresa, semana, slot] = 1 si empresa ocupa ese slot esa semana

Tablas Prisma (camelCase):
  - frecuencia, planificacion, restriccion, taller,
    "empresaCiudad", "solverLog"
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class CalendarioInput(BaseModel):
    trimestre: str
    timeout_seconds: int = 30
    max_ef: int = 14
    max_it: int = 6
    semanas: int = 13
    peso_equilibrio: int = 10
    peso_no_consecutivas: int = 8
    peso_turno_preferido: int = 3


class SugerenciaContingencia(BaseModel):
    empresa_id: int
    empresa_nombre: str
    motivo: str
    prioridad: int


class SlotCalendario(BaseModel):
    id: int | None = None
    semana: int
    dia: str
    horario: str
    turno: str
    empresa_id: int | None  # nullable for vacancies
    empresa_nombre: str | None  # nullable for vacancies
    programa: str
    taller_id: int
    taller_nombre: str
    ciudad_id: int | None
    ciudad: str | None
    tipo_asignacion: str
    estado: str = "PLANIFICADO"  # PLANIFICADO | CONFIRMADO | OK | CANCELADO | VACANTE
    confirmado: bool = False
    notas: str | None = None
    sugerencias: list[SugerenciaContingencia] | None = None


class CalendarioOutput(BaseModel):
    trimestre: str
    status: str
    tiempo_segundos: float
    total_slots: int
    total_ef: int
    total_it: int
    slots: list[SlotCalendario]
    inviolables_pct: float
    preferentes_pct: float
    warnings: list[str]


class SlotUpdateInput(BaseModel):
    """Input for updating a single slot."""
    estado: str | None = None  # PLANIFICADO | CONFIRMADO | OK | CANCELADO | VACANTE
    confirmado: bool | None = None
    empresa_id: int | None = None  # Can be null to clear (make vacancy)
    notas: str | None = None


class SlotBatchUpdateItem(BaseModel):
    """Single item in a batch update."""
    slot_id: int
    estado: str | None = None
    confirmado: bool | None = None
    empresa_id: int | None = None
    notas: str | None = None


class SlotBatchUpdateInput(BaseModel):
    """Input for batch updating multiple slots."""
    updates: list[SlotBatchUpdateItem]


# ── Helpers ─────────────────────────────────────────────────

def calcular_fecha_slot(trimestre: str, semana: int, dia: str) -> str:
    """Returns formatted date string like '13 Abr 2026' for a slot."""
    from datetime import date, timedelta

    MESES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
             "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

    year = int(trimestre[:4])
    quarter = int(trimestre[-1])
    month_start = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]
    first_day = date(year, month_start, 1)

    # Find first Monday
    days_until_monday = (7 - first_day.weekday()) % 7
    if first_day.weekday() == 0:
        first_monday = first_day
    else:
        first_monday = first_day + timedelta(days=days_until_monday)

    week_start = first_monday + timedelta(weeks=semana - 1)
    dia_offset = {"L": 0, "M": 1, "X": 2, "J": 3, "V": 4}
    fecha = week_start + timedelta(days=dia_offset.get(dia, 0))

    return f"{fecha.day} {MESES[fecha.month - 1]} {fecha.year}"


# ── Endpoints ────────────────────────────────────────────────

@router.post("/generar", response_model=CalendarioOutput)
async def generar_calendario(
    params: CalendarioInput,
    db: AsyncSession = Depends(get_db),
):
    """Ejecuta Fase 2: genera el calendario trimestral usando CP-SAT."""
    trimestre = params.trimestre
    warnings: list[str] = []

    # ── 1. Cargar frecuencias confirmadas (output Fase 1) ────
    freq_result = await db.execute(
        text("""
            SELECT f."empresaId", e.nombre,
                   f."talleresEF", f."talleresIT", f."totalAsignado",
                   f."semaforoCalculado", f."scoreCalculado",
                   f."esNueva",
                   e."esComodin", e."turnoPreferido"
            FROM frecuencia f
            JOIN empresa e ON e.id = f."empresaId"
            WHERE f.trimestre = :trimestre
        """),
        {"trimestre": trimestre},
    )
    frecuencias = [dict(r) for r in freq_result.mappings().all()]

    if not frecuencias:
        raise HTTPException(
            status_code=404,
            detail=f"No hay frecuencias confirmadas para {trimestre}. "
                   "Ejecutar y confirmar Fase 1 primero.",
        )

    # ── 2. Restricciones ─────────────────────────────────────
    rest_result = await db.execute(
        text('SELECT "empresaId", tipo, clave, valor FROM restriccion')
    )
    restricciones = [dict(r) for r in rest_result.mappings().all()]

    # ── 3. Talleres (slots fijos) ─────────────────────────────
    talleres_result = await db.execute(
        text("""
            SELECT id, nombre, programa, "diaSemana", horario, turno
            FROM taller WHERE activo = true
            ORDER BY id
        """)
    )
    talleres = [dict(r) for r in talleres_result.mappings().all()]

    if len(talleres) == 0:
        raise HTTPException(
            status_code=500,
            detail="No hay talleres activos en la BD. Ejecutar migración de talleres.",
        )

    talleres_ef = [t for t in talleres if t["programa"] == "EF"]
    talleres_it = [t for t in talleres if t["programa"] == "IT"]
    warnings.append(
        f"Catálogo: {len(talleres_ef)} EF + {len(talleres_it)} IT = {len(talleres)} talleres/semana"
    )

    # ── 4. Ciudad Madrid (todos los talleres son de Madrid) ──
    madrid_result = await db.execute(
        text("SELECT id, nombre FROM ciudad WHERE UPPER(nombre) = 'MADRID' LIMIT 1")
    )
    madrid_row = madrid_result.mappings().first()
    if not madrid_row:
        warnings.append("⚠ Ciudad MADRID no encontrada en BD — ciudadId será null")
        madrid_id = None
    else:
        madrid_id = madrid_row["id"]

    # ── 5. Disponibilidad de días por empresa ────────────────
    disp_result = await db.execute(
        text("""
            SELECT "empresaId", "disponibilidadDias"
            FROM "configTrimestral"
            WHERE trimestre = :trimestre
        """),
        {"trimestre": trimestre},
    )
    disponibilidad_map: dict[int, list[str]] = {}
    for row in disp_result.mappings().all():
        r = dict(row)
        dias_str = r["disponibilidadDias"] or "L,M,X,J,V"
        disponibilidad_map[r["empresaId"]] = [d.strip() for d in dias_str.split(",")]

    # ── 6. Festivos → días excluidos por semana ──────────────────
    # Returns set of (semana_relativa, dia) tuples to exclude specific slots
    dias_excluidos: set[tuple[int, str]] = set()
    semanas_excluidas: set[int] = set()  # Keep for backward compat (weeks where ALL 5 days are excluded)
    try:
        fest_result = await db.execute(
            text('''
                SELECT semana, dia, motivo FROM festivo
                WHERE trimestre = :trimestre
                ORDER BY semana, dia
            '''),
            {"trimestre": trimestre},
        )
        festivos_rows = [dict(r) for r in fest_result.mappings().all()]

        for row in festivos_rows:
            sem = row["semana"]
            dia = row["dia"]
            if 1 <= sem <= 13 and dia in ("L", "M", "X", "J", "V"):
                dias_excluidos.add((sem, dia))

        # Check if any week has ALL 5 days excluded (full week closure like summer)
        from collections import Counter
        dias_por_semana = Counter(sem for sem, _ in dias_excluidos)
        for sem, count in dias_por_semana.items():
            if count >= 5:
                semanas_excluidas.add(sem)

        if dias_excluidos:
            # Format for warning: group by week
            by_week: dict[int, list[str]] = {}
            for sem, dia in sorted(dias_excluidos):
                by_week.setdefault(sem, []).append(dia)
            parts = []
            for sem in sorted(by_week):
                dias = by_week[sem]
                if len(dias) >= 5:
                    parts.append(f"S{sem} (completa)")
                else:
                    parts.append(f"S{sem}:{','.join(dias)}")
            warnings.append(f"Días excluidos: {', '.join(parts)}")

    except Exception:
        pass  # table may not exist yet

    # ── 7. Ejecutar solver ───────────────────────────────────
    resultado = _ejecutar_solver(
        frecuencias=frecuencias,
        restricciones=restricciones,
        talleres=talleres,
        disponibilidad_map=disponibilidad_map,
        semanas_excluidas=semanas_excluidas,
        dias_excluidos=dias_excluidos,
        params=params,
    )

    if resultado["status"] in ("INFEASIBLE", "TIMEOUT"):
        await _guardar_log(db, trimestre, resultado)
        await db.commit()
        return CalendarioOutput(**resultado, trimestre=trimestre)

    # ── 8. Post-proceso: asignar ciudad Madrid + sugerencias ──
    slots_completos = _asignar_ciudades(
        slots_raw=resultado["slots"],
        frecuencias=frecuencias,
        madrid_id=madrid_id,
        restricciones=restricciones,
        warnings=warnings,
    )

    resultado["slots"] = slots_completos
    resultado["warnings"] = warnings
    resultado["total_ef"] = sum(1 for s in slots_completos if s["programa"] == "EF" and s["empresa_id"] != 0)
    resultado["total_it"] = sum(1 for s in slots_completos if s["programa"] == "IT" and s["empresa_id"] != 0)
    resultado["total_slots"] = sum(1 for s in slots_completos if s["empresa_id"] != 0)

    # ── 9. Persistir ─────────────────────────────────────────
    await db.execute(
        text('DELETE FROM planificacion WHERE trimestre = :tri'),
        {"tri": trimestre},
    )

    for slot in slots_completos:
        # Vacancies: empresa_id=0 -> store as NULL with estado='VACANTE'
        is_vacancy = slot["empresa_id"] == 0
        empresa_id = None if is_vacancy else slot["empresa_id"] 
        estado = "VACANTE" if is_vacancy else "PLANIFICADO"
        tipo_bd = "CONTINGENCIA" if slot["tipo_asignacion"] == "HUECO" else slot["tipo_asignacion"]
        es_contingencia = slot["tipo_asignacion"] == "HUECO"
        await db.execute(
            text("""
                INSERT INTO planificacion
                    (trimestre, semana, dia, horario, turno, "empresaId", "tallerId",
                     "ciudadId", "tipoAsignacion", "esContingencia", estado, confirmado, "updatedAt")
                VALUES (
                    :tri, :sem, :dia, :horario, :turno, :eid, :tid,
                    :cid, :tipo, :contingencia, :estado, false, NOW()
                )
            """),
            {
                "tri": trimestre,
                "sem": slot["semana"],
                "dia": slot["dia"],
                "horario": slot["horario"],
                "turno": slot["turno"],
                "eid": empresa_id,
                "tid": slot["taller_id"],
                "cid": slot.get("ciudad_id"),
                "tipo": tipo_bd,
                "contingencia": es_contingencia,
                "estado": estado,
            },
        )

    await _guardar_log(db, trimestre, resultado)
    await db.commit()

    return CalendarioOutput(**resultado, trimestre=trimestre)


@router.get("/{trimestre}")
async def obtener_calendario(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """Lee el calendario generado de un trimestre (incluye vacantes)."""
    result = await db.execute(
        text("""
            SELECT p.id,
                   p.semana, p.dia, p.horario,
                   COALESCE(p.turno, t.turno) AS turno,
                   p."empresaId" AS empresa_id,
                   e.nombre AS empresa_nombre,
                   t.programa,
                   p."tallerId" AS taller_id,
                   t.nombre AS taller_nombre,
                   p."ciudadId" AS ciudad_id,
                   c.nombre AS ciudad,
                   p."tipoAsignacion" AS tipo_asignacion,
                   p.estado,
                   p.confirmado,
                   p.notas
            FROM planificacion p
            JOIN taller t ON t.id = p."tallerId"
            LEFT JOIN empresa e ON e.id = p."empresaId"
            LEFT JOIN ciudad c ON c.id = p."ciudadId"
            WHERE p.trimestre = :trimestre
            ORDER BY p.semana,
                     CASE p.dia WHEN 'L' THEN 1 WHEN 'M' THEN 2 WHEN 'X' THEN 3 WHEN 'J' THEN 4 WHEN 'V' THEN 5 END,
                     p.horario
        """),
        {"trimestre": trimestre},
    )
    rows = [dict(r) for r in result.mappings().all()]

    # Compute summary stats
    asignados = sum(1 for r in rows if r["estado"] != "VACANTE")
    vacantes = sum(1 for r in rows if r["estado"] == "VACANTE")
    confirmados = sum(1 for r in rows if r["confirmado"])
    ok_count = sum(1 for r in rows if r["estado"] == "OK")
    cancelados = sum(1 for r in rows if r["estado"] == "CANCELADO")

    return {
        "trimestre": trimestre,
        "total_slots": len(rows),
        "asignados": asignados,
        "vacantes": vacantes,
        "confirmados": confirmados,
        "ok": ok_count,
        "cancelados": cancelados,
        "slots": rows,
    }


@router.post("/{trimestre}/exportar-excel")
async def exportar_excel(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Genera Excel del calendario con TODOS los slots (asignados + vacantes).
    Lee estado, confirmado y notas directamente de la tabla planificacion.
    """
    from fastapi.responses import StreamingResponse
    from io import BytesIO
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    # ── 1. Cargar TODOS los slots de planificacion ────────────
    result = await db.execute(
        text("""
            SELECT p.id, p.semana, p.dia, p.horario,
                   COALESCE(p.turno, t.turno) AS turno,
                   e.nombre AS empresa,
                   t.nombre AS taller,
                   t.programa,
                   c.nombre AS ciudad,
                   p."tipoAsignacion" AS tipo,
                   p.estado,
                   p.confirmado,
                   p.notas
            FROM planificacion p
            JOIN taller t ON t.id = p."tallerId"
            LEFT JOIN empresa e ON e.id = p."empresaId"
            LEFT JOIN ciudad c ON c.id = p."ciudadId"
            WHERE p.trimestre = :trimestre
            ORDER BY p.semana,
                     CASE p.dia WHEN 'L' THEN 1 WHEN 'M' THEN 2 WHEN 'X' THEN 3 WHEN 'J' THEN 4 WHEN 'V' THEN 5 END,
                     p.horario
        """),
        {"trimestre": trimestre},
    )
    db_rows = [dict(r) for r in result.mappings().all()]

    if not db_rows:
        raise HTTPException(status_code=404, detail=f"No hay slots para el trimestre {trimestre}")

    # ── 2. Construir filas para Excel ─────────────────────────
    all_rows: list[dict] = []
    has_notas = False

    for row in db_rows:
        estado = row["estado"] or "PLANIFICADO"
        confirmado = row["confirmado"]
        notas = row["notas"]

        if notas:
            has_notas = True

        all_rows.append({
            "semana": row["semana"],
            "fecha": calcular_fecha_slot(trimestre, row["semana"], row["dia"]),
            "dia": row["dia"],
            "horario": row["horario"],
            "turno": row["turno"] or "",
            "empresa": row["empresa"] or "",
            "taller": row["taller"],
            "programa": row["programa"],
            "ciudad": row["ciudad"] or "",
            "tipo": row["tipo"] or "BASE",
            "estado": estado,
            "confirmado": "SÍ" if confirmado else "",
            "notas": notas or "",
        })

    # ── 3. Contar estados para resumen ────────────────────────
    estado_counts = {"OK": 0, "CONFIRMADO": 0, "PLANIFICADO": 0, "CANCELADO": 0, "VACANTE": 0}
    for r in all_rows:
        estado = r["estado"]
        if estado in estado_counts:
            estado_counts[estado] += 1
        else:
            estado_counts["PLANIFICADO"] += 1

    # ── 4. Crear Excel con formato profesional ────────────────
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"Calendario {trimestre}"

    # Estilos
    header_font = Font(name="Arial", bold=True, size=10, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="2D3748")
    header_align = Alignment(horizontal="center", vertical="center")
    thin_border = Border(
        left=Side(style="thin", color="E2E8F0"),
        right=Side(style="thin", color="E2E8F0"),
        top=Side(style="thin", color="E2E8F0"),
        bottom=Side(style="thin", color="E2E8F0"),
    )
    normal_font = Font(name="Arial", size=9)

    # Estado-specific styles
    vacante_fill = PatternFill("solid", fgColor="FEF3C7")  # amarillo suave
    vacante_font = Font(name="Arial", size=9, color="92400E", italic=True)
    ok_fill = PatternFill("solid", fgColor="D1FAE5")  # verde suave
    ok_font = Font(name="Arial", size=9, color="065F46")
    cancelado_fill = PatternFill("solid", fgColor="FEE2E2")  # rojo suave
    cancelado_font = Font(name="Arial", size=9, color="991B1B", strike=True)
    confirmado_fill = PatternFill("solid", fgColor="DBEAFE")  # azul suave
    confirmado_font = Font(name="Arial", size=9, color="1E40AF")

    # Headers
    headers = [
        "Semana", "Fecha", "Día", "Horario", "Turno", "Empresa",
        "Taller", "Programa", "Ciudad", "Tipo", "Estado", "Confirmado",
    ]
    col_widths = [8, 14, 6, 14, 10, 25, 42, 10, 12, 12, 12, 14]

    if has_notas:
        headers.append("Notas")
        col_widths.append(30)

    for col, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border
        ws.column_dimensions[cell.column_letter].width = w

    # Freeze header
    ws.freeze_panes = "A2"
    # Auto-filter (M with notas, L without — one more due to Fecha column)
    last_col = "M" if has_notas else "L"
    ws.auto_filter.ref = f"A1:{last_col}{len(all_rows) + 1}"

    # Data rows
    for i, row in enumerate(all_rows, 2):
        estado = row["estado"]
        is_vacante = estado == "VACANTE"
        is_ok = estado == "OK"
        is_cancelado = estado == "CANCELADO"
        is_confirmado = estado == "CONFIRMADO"

        values = [
            row["semana"], row["fecha"], row["dia"], row["horario"], row["turno"],
            row["empresa"], row["taller"], row["programa"],
            row["ciudad"], row["tipo"], row["estado"], row["confirmado"],
        ]
        if has_notas:
            values.append(row["notas"])

        for col, val in enumerate(values, 1):
            cell = ws.cell(row=i, column=col, value=val)
            cell.border = thin_border
            cell.alignment = Alignment(vertical="center")

            # Apply estado-specific styling
            if is_vacante:
                cell.fill = vacante_fill
                cell.font = vacante_font
            elif is_ok:
                cell.fill = ok_fill
                cell.font = ok_font
            elif is_cancelado:
                cell.fill = cancelado_fill
                # Only strikethrough on empresa column (6 — after adding Fecha)
                if col == 6:
                    cell.font = cancelado_font
                else:
                    cell.font = Font(name="Arial", size=9, color="991B1B")
            elif is_confirmado:
                cell.fill = confirmado_fill
                cell.font = confirmado_font
            else:
                cell.font = normal_font

            # Semana column: center and bold
            if col == 1:
                cell.font = Font(
                    name="Arial", size=9, bold=True,
                    color="92400E" if is_vacante else "065F46" if is_ok else "991B1B" if is_cancelado else "1E40AF" if is_confirmado else "000000",
                )
                cell.alignment = Alignment(horizontal="center", vertical="center")

    # ── 5. Hoja resumen ───────────────────────────────────────
    ws_resumen = wb.create_sheet("Resumen")
    ws_resumen["A1"] = f"Calendario {trimestre}"
    ws_resumen["A1"].font = Font(name="Arial", bold=True, size=14)

    total_slots = len(all_rows)
    resumen_data = [
        ("Total slots", total_slots),
        ("", ""),
        ("ESTADOS", ""),
        ("OK", estado_counts["OK"]),
        ("Confirmados", estado_counts["CONFIRMADO"]),
        ("Planificados", estado_counts["PLANIFICADO"]),
        ("Cancelados", estado_counts["CANCELADO"]),
        ("Vacantes", estado_counts["VACANTE"]),
        ("", ""),
        ("INSTRUCCIONES", ""),
        ("1. Columna 'Empresa'", "Completar vacantes con empresa asignada"),
        ("2. Columna 'Estado'", "OK = completado, CANCELADO = no realizado"),
        ("3. Columna 'Confirmado'", "SÍ = empresa confirmó asistencia"),
        ("4. Al cierre del trimestre", "Importar como histórico en el sistema"),
    ]

    for i, (label, value) in enumerate(resumen_data, 3):
        cell_a = ws_resumen.cell(row=i, column=1, value=label)
        cell_b = ws_resumen.cell(row=i, column=2, value=value)
        is_header = label in ("ESTADOS", "INSTRUCCIONES")
        cell_a.font = Font(name="Arial", size=10, bold=is_header or bool(label))
        cell_b.font = Font(name="Arial", size=10)

    ws_resumen.column_dimensions["A"].width = 30
    ws_resumen.column_dimensions["B"].width = 40

    # ── 6. Devolver ───────────────────────────────────────────
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=calendario_{trimestre}.xlsx"
        },
    )


# ── Slot Operations (Fase 3 — Operación) ────────────────────


@router.patch("/{trimestre}/slots/{slot_id}")
async def actualizar_slot(
    trimestre: str,
    slot_id: int,
    body: SlotUpdateInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Update a slot's estado, confirmado, empresaId, or notas.
    Used for: confirming, cancelling, assigning company to vacancy, adding notes.
    """
    # Verify slot exists and belongs to this trimestre
    check = await db.execute(
        text('SELECT id, "empresaId", estado, semana FROM planificacion WHERE id = :id AND trimestre = :tri'),
        {"id": slot_id, "tri": trimestre},
    )
    existing = check.mappings().first()
    if not existing:
        raise HTTPException(status_code=404, detail=f"Slot {slot_id} not found in {trimestre}")

    # Build dynamic UPDATE
    updates = []
    params = {"id": slot_id}

    # Handle empresa_id changes
    new_empresa_id = body.empresa_id
    if body.empresa_id is not None or (body.estado and body.estado == "VACANTE"):
        # If clearing empresa (setting to vacancy)
        if body.empresa_id is None and body.estado == "VACANTE":
            updates.append('"empresaId" = NULL')
        elif body.empresa_id is not None:
            # Verify company doesn't already have a slot this week (H6 constraint)
            week = existing["semana"]
            conflict = await db.execute(
                text('''
                    SELECT id FROM planificacion
                    WHERE trimestre = :tri AND semana = :week AND "empresaId" = :eid AND id != :slot_id
                '''),
                {"tri": trimestre, "week": week, "eid": body.empresa_id, "slot_id": slot_id},
            )
            if conflict.first():
                raise HTTPException(
                    status_code=400,
                    detail=f"Company {body.empresa_id} already has a slot in week {week}",
                )
            updates.append('"empresaId" = :empresa_id')
            params["empresa_id"] = body.empresa_id

    # Handle estado changes with auto-adjustments
    if body.estado is not None:
        new_estado = body.estado
        # Auto-adjust: if assigning empresa to a VACANTE, change estado to PLANIFICADO
        if body.empresa_id is not None and existing["estado"] == "VACANTE":
            new_estado = "PLANIFICADO"
        # Auto-adjust: if clearing empresa, change estado to VACANTE
        if body.empresa_id is None and body.estado == "VACANTE":
            new_estado = "VACANTE"
        updates.append("estado = :estado")
        params["estado"] = new_estado
    elif body.empresa_id is not None and existing["estado"] == "VACANTE":
        # Auto-transition from VACANTE to PLANIFICADO when assigning empresa
        updates.append("estado = :estado")
        params["estado"] = "PLANIFICADO"

    if body.confirmado is not None:
        updates.append("confirmado = :confirmado")
        params["confirmado"] = body.confirmado

    if body.notas is not None:
        updates.append("notas = :notas")
        params["notas"] = body.notas

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates.append('"updatedAt" = NOW()')
    query = f"UPDATE planificacion SET {', '.join(updates)} WHERE id = :id"
    await db.execute(text(query), params)
    await db.commit()

    # Return updated slot
    result = await db.execute(
        text("""
            SELECT p.id, p.semana, p.dia, p.horario,
                   COALESCE(p.turno, t.turno) AS turno,
                   p."empresaId" AS empresa_id,
                   e.nombre AS empresa_nombre,
                   t.programa,
                   p."tallerId" AS taller_id,
                   t.nombre AS taller_nombre,
                   p."ciudadId" AS ciudad_id,
                   c.nombre AS ciudad,
                   p."tipoAsignacion" AS tipo_asignacion,
                   p.estado,
                   p.confirmado,
                   p.notas
            FROM planificacion p
            JOIN taller t ON t.id = p."tallerId"
            LEFT JOIN empresa e ON e.id = p."empresaId"
            LEFT JOIN ciudad c ON c.id = p."ciudadId"
            WHERE p.id = :id
        """),
        {"id": slot_id},
    )
    row = result.mappings().first()
    return {"slot": dict(row) if row else None}


@router.patch("/{trimestre}/slots-batch")
async def actualizar_slots_batch(
    trimestre: str,
    body: SlotBatchUpdateInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Batch update for confirming/cancelling multiple slots at once.
    Returns: { updated: int, errors: list[str] }
    """
    updated = 0
    errors = []

    for item in body.updates:
        try:
            # Build dynamic UPDATE for each slot
            check = await db.execute(
                text('SELECT id FROM planificacion WHERE id = :id AND trimestre = :tri'),
                {"id": item.slot_id, "tri": trimestre},
            )
            if not check.first():
                errors.append(f"Slot {item.slot_id} not found")
                continue

            updates = []
            params = {"id": item.slot_id}

            if item.estado is not None:
                updates.append("estado = :estado")
                params["estado"] = item.estado

            if item.confirmado is not None:
                updates.append("confirmado = :confirmado")
                params["confirmado"] = item.confirmado

            if item.empresa_id is not None:
                updates.append('"empresaId" = :empresa_id')
                params["empresa_id"] = item.empresa_id

            if item.notas is not None:
                updates.append("notas = :notas")
                params["notas"] = item.notas

            if updates:
                updates.append('"updatedAt" = NOW()')
                query = f"UPDATE planificacion SET {', '.join(updates)} WHERE id = :id"
                await db.execute(text(query), params)
                updated += 1

        except Exception as e:
            errors.append(f"Slot {item.slot_id}: {str(e)}")

    await db.commit()
    return {"updated": updated, "errors": errors}


@router.get("/{trimestre}/resumen")
async def resumen_operacion(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Returns operational summary for the quarter.
    """
    # Overall stats
    result = await db.execute(
        text("""
            SELECT
                COUNT(*) AS total_slots,
                SUM(CASE WHEN estado != 'VACANTE' THEN 1 ELSE 0 END) AS asignados,
                SUM(CASE WHEN estado = 'VACANTE' THEN 1 ELSE 0 END) AS vacantes,
                SUM(CASE WHEN confirmado = true THEN 1 ELSE 0 END) AS confirmados,
                SUM(CASE WHEN estado = 'OK' THEN 1 ELSE 0 END) AS ok,
                SUM(CASE WHEN estado = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados
            FROM planificacion
            WHERE trimestre = :trimestre
        """),
        {"trimestre": trimestre},
    )
    totals = dict(result.mappings().first() or {})

    # By week
    by_week_result = await db.execute(
        text("""
            SELECT
                semana,
                COUNT(*) AS total,
                SUM(CASE WHEN estado != 'VACANTE' THEN 1 ELSE 0 END) AS asignados,
                SUM(CASE WHEN estado = 'VACANTE' THEN 1 ELSE 0 END) AS vacantes,
                SUM(CASE WHEN confirmado = true THEN 1 ELSE 0 END) AS confirmados,
                SUM(CASE WHEN estado = 'OK' THEN 1 ELSE 0 END) AS ok,
                SUM(CASE WHEN estado = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados
            FROM planificacion
            WHERE trimestre = :trimestre
            GROUP BY semana
            ORDER BY semana
        """),
        {"trimestre": trimestre},
    )
    by_week = [dict(r) for r in by_week_result.mappings().all()]

    # By company
    by_company_result = await db.execute(
        text("""
            SELECT
                e.nombre AS empresa,
                COUNT(*) AS total,
                SUM(CASE WHEN p.confirmado = true THEN 1 ELSE 0 END) AS confirmados,
                SUM(CASE WHEN p.estado = 'OK' THEN 1 ELSE 0 END) AS ok,
                SUM(CASE WHEN p.estado = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados
            FROM planificacion p
            JOIN empresa e ON e.id = p."empresaId"
            WHERE p.trimestre = :trimestre AND p."empresaId" IS NOT NULL
            GROUP BY e.nombre
            ORDER BY total DESC
        """),
        {"trimestre": trimestre},
    )
    by_company = [dict(r) for r in by_company_result.mappings().all()]

    # Progress percentage: slots with OK or CANCELADO (finished)
    total = totals.get("total_slots") or 0
    finished = (totals.get("ok") or 0) + (totals.get("cancelados") or 0)
    progress_pct = round((finished / total) * 100, 1) if total > 0 else 0

    return {
        "trimestre": trimestre,
        **totals,
        "progress_pct": progress_pct,
        "by_week": by_week,
        "by_company": by_company,
    }


# ── Quarter Close (Auto-close) ──────────────────────────────


class CerrarTrimestreInput(BaseModel):
    confirmar: bool = False  # Si es False, hace dry run (preview)


# ── Import Excel (re-import edited calendar) ────────────────


class EmpresaCambiada(BaseModel):
    """Detalle de una empresa que cambió en un slot."""
    slot_id: int
    semana: int
    dia: str
    taller_nombre: str
    empresa_anterior: str | None
    empresa_nueva: str


class CambioDetalle(BaseModel):
    """Detalle de un cambio detectado en un slot (estado, confirmado o empresa)."""
    slot_id: int
    semana: int
    dia: str
    taller_nombre: str
    empresa_nombre: str | None
    campo: str  # "estado" | "confirmado" | "empresa"
    valor_anterior: str
    valor_nuevo: str


class ImportarExcelResult(BaseModel):
    """Resultado de importar Excel editado."""
    trimestre: str
    total_procesados: int
    actualizados: int
    sin_cambios: int
    errores: int
    empresas_cambiadas: list[EmpresaCambiada]
    cambios_detalle: list[CambioDetalle]
    warnings: list[str]


class ImportarExcelInput(BaseModel):
    """Input for dry_run mode."""
    dry_run: bool = False


@router.post("/{trimestre}/importar-excel", response_model=ImportarExcelResult)
async def importar_excel(
    trimestre: str,
    dry_run: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """
    Importa un Excel editado de vuelta al calendario (planificacion).

    Matching: trimestre + semana + taller_nombre (slot fijo)
    Updates: empresa (por nombre), estado, confirmado

    El Excel debe tener las mismas columnas que el exportado:
    Semana, Día, Horario, Turno, Empresa, Taller, Programa, Ciudad, Tipo, Estado, Confirmado
    """
    from fastapi import UploadFile, File
    raise HTTPException(
        status_code=501,
        detail="Use POST /{trimestre}/importar-excel-file with file upload",
    )


@router.post("/{trimestre}/importar-excel-file", response_model=ImportarExcelResult)
async def importar_excel_file(
    trimestre: str,
    file: UploadFile = File(...),
    dry_run: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """
    Importa un Excel editado de vuelta al calendario (planificacion).

    Matching: trimestre + semana + taller_nombre (slot fijo)
    Updates: empresa (por nombre), estado, confirmado

    Parámetros:
    - file: Excel con el calendario editado
    - dry_run: Si true, solo muestra qué cambiaría sin aplicar cambios

    El Excel debe tener las mismas columnas que el exportado:
    Semana, Día, Horario, Turno, Empresa, Taller, Programa, Ciudad, Tipo, Estado, Confirmado
    """
    import openpyxl
    from io import BytesIO

    warnings: list[str] = []
    empresas_cambiadas: list[dict] = []
    cambios_detalle: list[dict] = []
    total_procesados = 0
    actualizados = 0
    sin_cambios = 0
    errores = 0

    # ── 1. Leer Excel ─────────────────────────────────────────
    try:
        content = await file.read()
        wb = openpyxl.load_workbook(BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al leer Excel: {str(e)}")

    # ── 2. Validar headers ────────────────────────────────────
    # Include "Fecha" for new format but don't require it (backward compat)
    expected_headers = ["Semana", "Fecha", "Día", "Horario", "Turno", "Empresa",
                        "Taller", "Programa", "Ciudad", "Tipo", "Estado", "Confirmado"]
    actual_headers = [cell.value for cell in ws[1]]

    # Normalize headers (strip whitespace, case-insensitive match)
    actual_normalized = [str(h).strip().lower() if h else "" for h in actual_headers]
    expected_normalized = [h.lower() for h in expected_headers]

    # Find column indices
    col_map = {}
    for i, expected in enumerate(expected_normalized):
        for j, actual in enumerate(actual_normalized):
            if expected == actual:
                col_map[expected_headers[i]] = j
                break

    required = ["Semana", "Taller", "Empresa", "Estado", "Confirmado"]
    missing = [h for h in required if h not in col_map]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Columnas requeridas no encontradas: {', '.join(missing)}. "
                   f"Headers encontrados: {actual_headers[:15]}",
        )

    # ── 3. Cargar mapa de empresas (nombre -> id) ─────────────
    emp_result = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE activa = true")
    )
    empresas_db = {r["nombre"].strip().lower(): r["id"] for r in emp_result.mappings().all()}

    # ── 4. Cargar slots existentes indexados ──────────────────
    slots_result = await db.execute(
        text("""
            SELECT
                p.id, p.semana, p.dia, p.horario,
                p."empresaId" AS empresa_id,
                e.nombre AS empresa_nombre,
                t.nombre AS taller_nombre,
                t.programa,
                p.estado,
                p.confirmado
            FROM planificacion p
            JOIN taller t ON t.id = p."tallerId"
            LEFT JOIN empresa e ON e.id = p."empresaId"
            WHERE p.trimestre = :tri
        """),
        {"tri": trimestre},
    )
    slots_db = [dict(r) for r in slots_result.mappings().all()]

    if not slots_db:
        raise HTTPException(
            status_code=404,
            detail=f"No hay slots de planificacion para {trimestre}",
        )

    # Index: (semana, taller_nombre_lower) -> slot
    slot_index: dict[tuple[int, str], dict] = {}
    for s in slots_db:
        key = (s["semana"], s["taller_nombre"].strip().lower())
        slot_index[key] = s

    # ── 5. Procesar filas del Excel ───────────────────────────
    # Estado normalization map (handles typos)
    ESTADO_NORMALIZE = {
        "OK": "OK",
        "CANCELADO": "CANCELADO",
        "CONFIRMADO": "CONFIRMADO",
        "CONFRIMADO": "CONFIRMADO",  # common typo
        "CONFIRAMDO": "CONFIRMADO",  # another typo
        "PLANIFICADO": "PLANIFICADO",
        "PLANFICADO": "PLANIFICADO",  # typo
        "VACANTE": "VACANTE",
    }

    updates_to_apply: list[dict] = []

    for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        # Skip empty rows
        if not row or all(cell is None for cell in row):
            continue

        total_procesados += 1

        try:
            # Extract values
            semana_val = row[col_map["Semana"]]
            taller_val = row[col_map["Taller"]]
            empresa_val = row[col_map["Empresa"]]
            estado_val = row[col_map["Estado"]]
            confirmado_val = row[col_map["Confirmado"]]

            # Parse semana
            try:
                semana = int(semana_val) if semana_val is not None else None
            except (ValueError, TypeError):
                warnings.append(f"Fila {row_num}: Semana inválida '{semana_val}'")
                errores += 1
                continue

            if semana is None or not taller_val:
                warnings.append(f"Fila {row_num}: Datos incompletos (semana o taller)")
                errores += 1
                continue

            # Normalize taller name
            taller_nombre = str(taller_val).strip().lower()

            # Find matching slot
            key = (semana, taller_nombre)
            slot = slot_index.get(key)

            if not slot:
                warnings.append(f"Fila {row_num}: Slot no encontrado (S{semana}, {taller_val})")
                errores += 1
                continue

            # ── Process empresa ───────────────────────────────
            empresa_nueva = str(empresa_val).strip() if empresa_val else ""
            empresa_nueva_id = None

            if empresa_nueva:
                empresa_nueva_lower = empresa_nueva.lower()
                if empresa_nueva_lower in empresas_db:
                    empresa_nueva_id = empresas_db[empresa_nueva_lower]
                else:
                    warnings.append(f"Fila {row_num}: Empresa '{empresa_nueva}' no encontrada")
                    # Don't skip - we can still update estado/confirmado

            # ── Process estado (with typo normalization) ──────
            estado_nuevo = None
            if estado_val:
                estado_raw = str(estado_val).strip().upper()
                estado_nuevo = ESTADO_NORMALIZE.get(estado_raw, estado_raw)
                # Check if it's valid after normalization
                if estado_nuevo not in ESTADO_NORMALIZE.values():
                    warnings.append(f"Fila {row_num}: Estado '{estado_val}' inválido")
                    estado_nuevo = None

            # ── Process confirmado ────────────────────────────
            confirmado_nuevo = None
            if confirmado_val is not None:
                confirmado_str = str(confirmado_val).strip().upper()
                if confirmado_str in ("SÍ", "SI", "TRUE", "1", "YES", "X"):
                    confirmado_nuevo = True
                elif confirmado_str in ("NO", "FALSE", "0", ""):
                    confirmado_nuevo = False

            # ── Get and normalize DB values ───────────────────
            empresa_anterior_id = slot["empresa_id"]
            empresa_anterior_nombre = slot["empresa_nombre"]
            # Normalize DB estado too (should already be uppercase but be safe)
            estado_anterior_raw = slot["estado"]
            estado_anterior = estado_anterior_raw.strip().upper() if estado_anterior_raw else "PLANIFICADO"
            confirmado_anterior = bool(slot["confirmado"])  # Ensure boolean

            # ── Check if anything changed ─────────────────────
            changes = {}

            # Empresa change
            empresa_changed = False
            if empresa_nueva_id is not None and empresa_nueva_id != empresa_anterior_id:
                changes["empresa_id"] = empresa_nueva_id
                empresa_changed = True
            elif empresa_nueva == "" and empresa_anterior_id is not None:
                # Clearing empresa (making vacancy)
                changes["empresa_id"] = None
                changes["estado"] = "VACANTE"
                empresa_changed = True

            # Estado change (compare normalized values)
            if estado_nuevo is not None and estado_nuevo != estado_anterior:
                # Auto-adjust: if empresa is being assigned to a VACANTE, estado becomes PLANIFICADO
                if "empresa_id" in changes and changes["empresa_id"] is not None and estado_anterior == "VACANTE":
                    if estado_nuevo == "VACANTE":
                        estado_nuevo = "PLANIFICADO"
                changes["estado"] = estado_nuevo

            # Confirmado change (compare as booleans)
            if confirmado_nuevo is not None and confirmado_nuevo != confirmado_anterior:
                changes["confirmado"] = confirmado_nuevo

            if not changes:
                sin_cambios += 1
                continue

            # Record detailed changes for each field that changed
            empresa_nombre_display = empresa_nueva or empresa_anterior_nombre or None

            if "estado" in changes:
                cambios_detalle.append({
                    "slot_id": slot["id"],
                    "semana": semana,
                    "dia": slot["dia"],
                    "taller_nombre": slot["taller_nombre"],
                    "empresa_nombre": empresa_nombre_display,
                    "campo": "estado",
                    "valor_anterior": estado_anterior,
                    "valor_nuevo": changes["estado"],
                })

            if "confirmado" in changes:
                cambios_detalle.append({
                    "slot_id": slot["id"],
                    "semana": semana,
                    "dia": slot["dia"],
                    "taller_nombre": slot["taller_nombre"],
                    "empresa_nombre": empresa_nombre_display,
                    "campo": "confirmado",
                    "valor_anterior": "SÍ" if confirmado_anterior else "NO",
                    "valor_nuevo": "SÍ" if changes["confirmado"] else "NO",
                })

            if empresa_changed:
                cambios_detalle.append({
                    "slot_id": slot["id"],
                    "semana": semana,
                    "dia": slot["dia"],
                    "taller_nombre": slot["taller_nombre"],
                    "empresa_nombre": empresa_nueva or "(vacante)",
                    "campo": "empresa",
                    "valor_anterior": empresa_anterior_nombre or "(vacante)",
                    "valor_nuevo": empresa_nueva or "(vacante)",
                })
                # Also record in empresas_cambiadas for backward compatibility
                empresas_cambiadas.append({
                    "slot_id": slot["id"],
                    "semana": semana,
                    "dia": slot["dia"],
                    "taller_nombre": slot["taller_nombre"],
                    "empresa_anterior": empresa_anterior_nombre,
                    "empresa_nueva": empresa_nueva or "(vacante)",
                })

            updates_to_apply.append({
                "slot_id": slot["id"],
                "changes": changes,
            })
            actualizados += 1

        except Exception as e:
            warnings.append(f"Fila {row_num}: Error procesando: {str(e)}")
            errores += 1

    # ── 6. Apply changes (if not dry_run) ─────────────────────
    if not dry_run and updates_to_apply:
        for upd in updates_to_apply:
            slot_id = upd["slot_id"]
            changes = upd["changes"]

            set_parts = []
            params = {"id": slot_id}

            if "empresa_id" in changes:
                if changes["empresa_id"] is None:
                    set_parts.append('"empresaId" = NULL')
                else:
                    set_parts.append('"empresaId" = :empresa_id')
                    params["empresa_id"] = changes["empresa_id"]

            if "estado" in changes:
                set_parts.append("estado = :estado")
                params["estado"] = changes["estado"]

            if "confirmado" in changes:
                set_parts.append("confirmado = :confirmado")
                params["confirmado"] = changes["confirmado"]

            set_parts.append('"updatedAt" = NOW()')

            query = f"UPDATE planificacion SET {', '.join(set_parts)} WHERE id = :id"
            await db.execute(text(query), params)

        await db.commit()

    wb.close()

    return {
        "trimestre": trimestre,
        "total_procesados": total_procesados,
        "actualizados": actualizados,  # Show count even in dry_run (preview)
        "sin_cambios": sin_cambios,
        "errores": errores,
        "empresas_cambiadas": [EmpresaCambiada(**ec) for ec in empresas_cambiadas],
        "cambios_detalle": [CambioDetalle(**cd) for cd in cambios_detalle],
        "warnings": warnings[:50],  # Limitar warnings
    }


# ── Cerrar Trimestre ─────────────────────────────────────────


class CerrarTrimestreResult(BaseModel):
    trimestre: str
    total_ok: int
    total_cancelado: int
    total_ignorado: int  # VACANTE + PLANIFICADO slots
    preview: bool


@router.post("/{trimestre}/cerrar", response_model=CerrarTrimestreResult)
async def cerrar_trimestre(
    trimestre: str,
    body: CerrarTrimestreInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Cierra el trimestre copiando los datos de planificacion a historicoTaller.

    Logica:
    1. Lee todos los slots de planificacion con estado OK o CANCELADO
    2. Calcula la fecha real desde trimestre + semana + dia
    3. Si confirmar=True, borra historicoTaller existente para ese trimestre
       e inserta los nuevos registros
    4. Si confirmar=False, solo devuelve un preview de lo que se cerraria

    Solo se copian slots con estado OK o CANCELADO.
    Slots VACANTE y PLANIFICADO se ignoran (no ejecutados).
    """
    from datetime import date, timedelta

    # ── 1. Leer slots con estado final (OK o CANCELADO) ──────
    result = await db.execute(
        text("""
            SELECT
                p.id,
                p."empresaId" AS empresa_id,
                p."tallerId" AS taller_id,
                p.semana,
                p.dia,
                p.estado,
                c.nombre AS ciudad
            FROM planificacion p
            LEFT JOIN ciudad c ON c.id = p."ciudadId"
            WHERE p.trimestre = :tri
            AND p.estado IN ('OK', 'CANCELADO')
            AND p."empresaId" IS NOT NULL
        """),
        {"tri": trimestre},
    )
    slots_finales = [dict(r) for r in result.mappings().all()]

    # ── 2. Contar ignorados (VACANTE + PLANIFICADO) ──────────
    ignorados_result = await db.execute(
        text("""
            SELECT COUNT(*) FROM planificacion
            WHERE trimestre = :tri
            AND (estado NOT IN ('OK', 'CANCELADO') OR "empresaId" IS NULL)
        """),
        {"tri": trimestre},
    )
    total_ignorado = ignorados_result.scalar() or 0

    # ── 3. Separar OK y CANCELADO ────────────────────────────
    slots_ok = [s for s in slots_finales if s["estado"] == "OK"]
    slots_cancelado = [s for s in slots_finales if s["estado"] == "CANCELADO"]

    total_ok = len(slots_ok)
    total_cancelado = len(slots_cancelado)

    # ── 4. Si no hay nada que cerrar, advertir ───────────────
    if total_ok == 0 and total_cancelado == 0:
        raise HTTPException(
            status_code=400,
            detail=f"No hay slots con estado OK o CANCELADO en {trimestre}. "
                   "El trimestre parece no estar listo para cerrar.",
        )

    # ── 5. Si es preview, devolver sin modificar ─────────────
    if not body.confirmar:
        return {
            "trimestre": trimestre,
            "total_ok": total_ok,
            "total_cancelado": total_cancelado,
            "total_ignorado": total_ignorado,
            "preview": True,
        }

    # ── 6. Calcular fechas y preparar inserts ────────────────
    def calcular_fecha(trimestre: str, semana: int, dia: str) -> date:
        """
        Calcula la fecha real desde trimestre + semana + dia.
        Q1: primer lunes de Enero, Q2: primer lunes de Abril, etc.
        """
        year = int(trimestre[:4])
        quarter = int(trimestre[-1])
        month_start = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]
        first_day = date(year, month_start, 1)
        # Encontrar primer lunes
        days_until_monday = (7 - first_day.weekday()) % 7
        if days_until_monday == 0 and first_day.weekday() != 0:
            days_until_monday = 7
        first_monday = first_day + timedelta(days=days_until_monday)
        # Si el primer dia del mes es lunes, usar ese
        if first_day.weekday() == 0:
            first_monday = first_day
        # Offset por semana (semana 1 = primera semana)
        week_start = first_monday + timedelta(weeks=semana - 1)
        # Offset por dia
        dia_offset = {"L": 0, "M": 1, "X": 2, "J": 3, "V": 4}
        return week_start + timedelta(days=dia_offset.get(dia, 0))

    # ── 7. Borrar historico existente para este trimestre ────
    await db.execute(
        text('DELETE FROM "historicoTaller" WHERE trimestre = :tri'),
        {"tri": trimestre},
    )

    # ── 8. Insertar nuevos registros ─────────────────────────
    for slot in slots_finales:
        fecha = calcular_fecha(trimestre, slot["semana"], slot["dia"])
        estado_db = "OK" if slot["estado"] == "OK" else "CANCELADO"

        await db.execute(
            text("""
                INSERT INTO "historicoTaller" (
                    "empresaId", "tallerId", fecha, estado, ciudad, trimestre, "createdAt"
                )
                VALUES (:eid, :tid, :fecha, :estado, :ciudad, :tri, NOW())
            """),
            {
                "eid": slot["empresa_id"],
                "tid": slot["taller_id"],
                "fecha": fecha,
                "estado": estado_db,
                "ciudad": slot["ciudad"] or "MADRID",
                "tri": trimestre,
            },
        )

    await db.commit()

    return {
        "trimestre": trimestre,
        "total_ok": total_ok,
        "total_cancelado": total_cancelado,
        "total_ignorado": total_ignorado,
        "preview": False,
    }


# ── Helpers ──────────────────────────────────────────────────

async def _guardar_log(db: AsyncSession, trimestre: str, resultado: dict):
    """Guarda el log del solver."""
    await db.execute(
        text("""
            INSERT INTO "solverLog"
                (trimestre, status, "tiempoSegundos",
                 "inviolablesCumplidas", "preferentesCumplidas", warnings)
            VALUES (:tri, :status, :tiempo, :inv, :pref, :warn)
        """),
        {
            "tri": trimestre,
            "status": resultado["status"],
            "tiempo": resultado["tiempo_segundos"],
            "inv": resultado["inviolables_pct"],
            "pref": resultado["preferentes_pct"],
            "warn": str(resultado.get("warnings", [])),
        },
    )


# ══════════════════════════════════════════════════════════════
# SOLVER CP-SAT — Modelo de slots fijos
# ══════════════════════════════════════════════════════════════

def _ejecutar_solver(
    frecuencias: list[dict],
    restricciones: list[dict],
    talleres: list[dict],
    disponibilidad_map: dict[int, list[str]],
    semanas_excluidas: set[int],
    dias_excluidos: set[tuple[int, str]],
    params: CalendarioInput,
) -> dict:
    """
    Asigna empresas a slots fijos semanales.

    Variables: assign[empresa_id, semana, taller_id] ∈ {0,1}
    Cada taller tiene día y horario fijos → empresa hereda esas propiedades.
    """
    from ortools.sat.python import cp_model
    import time

    model = cp_model.CpModel()
    start_time = time.time()
    warnings: list[str] = []

    SEMANAS = [s for s in range(1, params.semanas + 1) if s not in semanas_excluidas]
    if semanas_excluidas:
        warnings.append(
            f"Solver omite semanas {sorted(semanas_excluidas)} — excluidas del trimestre"
        )

    empresas = {f["empresaId"]: f for f in frecuencias}
    empresa_ids = [e for e in empresas if empresas[e]["totalAsignado"] > 0]

    talleres_ef = [t for t in talleres if t["programa"] == "EF"]
    talleres_it = [t for t in talleres if t["programa"] == "IT"]
    taller_ids_ef = [t["id"] for t in talleres_ef]
    taller_ids_it = [t["id"] for t in talleres_it]
    taller_ids = [t["id"] for t in talleres]
    taller_map = {t["id"]: t for t in talleres}

    # Restricciones indexadas
    rest_por_empresa: dict[int, list[dict]] = {}
    for r in restricciones:
        eid = r["empresaId"]
        if eid not in rest_por_empresa:
            rest_por_empresa[eid] = []
        rest_por_empresa[eid].append(r)

    # Días disponibles por empresa
    dias_disponibles: dict[int, list[str]] = {}
    for eid in empresa_ids:
        solo_dia = None
        for r in rest_por_empresa.get(eid, []):
            if r["clave"] == "solo_dia":
                solo_dia = r["valor"]
        if solo_dia:
            dias_disponibles[eid] = [solo_dia]
        else:
            dias_disponibles[eid] = disponibilidad_map.get(eid, ["L", "M", "X", "J", "V"])

    # Solo_taller por empresa (nombre → taller_ids que coincidan)
    solo_taller_ids: dict[int, list[int]] = {}
    for eid in empresa_ids:
        for r in rest_por_empresa.get(eid, []):
            if r["clave"] == "solo_taller":
                nombre = r["valor"].strip().lower()
                matching = [
                    t["id"] for t in talleres
                    if nombre in t["nombre"].strip().lower()
                    or t["nombre"].strip().lower() in nombre
                ]
                if matching:
                    solo_taller_ids[eid] = matching

    # No_comodin: empresas excluidas de contingencias (para post-proceso y sugerencias)
    no_comodin_ids: set[int] = set()
    for eid in empresa_ids:
        for r in rest_por_empresa.get(eid, []):
            if r["clave"] == "no_comodin":
                no_comodin_ids.add(eid)
 
    # Max_extras por empresa (para validación y post-proceso de contingencias)
    max_extras_map: dict[int, int] = {}
    for eid in empresa_ids:
        for r in rest_por_empresa.get(eid, []):
            if r["clave"] == "max_extras":
                try:
                    max_extras_map[eid] = int(r["valor"])
                except ValueError:
                    pass
 
    if no_comodin_ids:
        warnings.append(
            f"Empresas excluidas de comodín: {sorted(no_comodin_ids)} "
            f"({', '.join(empresas[e]['nombre'] for e in sorted(no_comodin_ids))})"
        )
    if max_extras_map:
        warnings.append(
            f"Límite de extras: {', '.join('{empresas[e][\"nombre\"]}={v}' for e, v in max_extras_map.items())}"
        )

    # ─── Variables de decisión ───────────────────────────────
    assign = {}
    for e in empresa_ids:
        for s in SEMANAS:
            for t_id in taller_ids:
                assign[(e, s, t_id)] = model.new_bool_var(f"a_{e}_{s}_{t_id}")

    # ─── HARD CONSTRAINTS ────────────────────────────────────

    # H1. Cada slot (semana, taller) tiene A LO SUMO 1 empresa
    # (permite slots vacíos si las frecuencias no llenan todo)
    for s in SEMANAS:
        for t_id in taller_ids:
            model.add(
                sum(assign[(e, s, t_id)] for e in empresa_ids) <= 1
            )

    # H1b. Total asignaciones = suma de frecuencias (todo lo confirmado se coloca)
    total_frecuencias = sum(empresas[e]["totalAsignado"] for e in empresa_ids)
    model.add(
        sum(assign[(e, s, t_id)]
            for e in empresa_ids for s in SEMANAS for t_id in taller_ids)
        == total_frecuencias
    )

    # H2. Frecuencia EF por empresa = talleresEF confirmado
    for e in empresa_ids:
        ef_requerido = empresas[e]["talleresEF"]
        model.add(
            sum(assign[(e, s, t_id)]
                for s in SEMANAS for t_id in taller_ids_ef)
            == ef_requerido
        )

    # H3. Frecuencia IT por empresa = talleresIT confirmado
    for e in empresa_ids:
        it_requerido = empresas[e]["talleresIT"]
        model.add(
            sum(assign[(e, s, t_id)]
                for s in SEMANAS for t_id in taller_ids_it)
            == it_requerido
        )

    # H4. Disponibilidad de días: empresa solo puede ir a slots de sus días
    for e in empresa_ids:
        dias_ok = dias_disponibles[e]
        for s in SEMANAS:
            for t_id in taller_ids:
                taller = taller_map[t_id]
                if taller["diaSemana"] not in dias_ok:
                    model.add(assign[(e, s, t_id)] == 0)

    # H5. Solo_taller: empresa solo puede ir a talleres permitidos
    for e in empresa_ids:
        if e in solo_taller_ids:
            allowed = solo_taller_ids[e]
            for s in SEMANAS:
                for t_id in taller_ids:
                    if t_id not in allowed:
                        model.add(assign[(e, s, t_id)] == 0)

    # H6. Max 1 taller por empresa por semana (planificación base)
    # Excepción: empresas con escuela propia pueden tener hasta 20 (toda la semana)
    # Por defecto max 1; la regla dice "ninguna empresa puede impartir dos en misma semana"
    for e in empresa_ids:
        max_per_week = 1  # Default: regla inviolable
        # Si la empresa tiene muchos talleres (>= 6), probablemente es escuela propia
        if empresas[e]["totalAsignado"] >= 6:
            max_per_week = 20  # Sin límite práctico
        for s in SEMANAS:
            model.add(
                sum(assign[(e, s, t_id)] for t_id in taller_ids) <= max_per_week
            )
    
    # H7. Empresas nuevas: no programar en el primer mes (semanas 1-4)
    # Permite onboarding, firma de convenio y preparación de voluntarios
    empresas_nuevas = [e for e in empresa_ids if empresas[e].get("esNueva", False)]
    if empresas_nuevas:
        semanas_bloqueadas = [s for s in SEMANAS if s <= 4]
        for e in empresas_nuevas:
            for s in semanas_bloqueadas:
                for t_id in taller_ids:
                    model.add(assign[(e, s, t_id)] == 0)
        
        # Verificar que hay suficientes slots en semanas 5-13
        semanas_disponibles_nuevas = [s for s in SEMANAS if s > 4]
        slots_disponibles = len(semanas_disponibles_nuevas) * len(taller_ids)
        talleres_nuevas = sum(empresas[e]["totalAsignado"] for e in empresas_nuevas)
        
        nombres_nuevas = [empresas[e]["nombre"] for e in empresas_nuevas]
        warnings.append(
            f"Empresas nuevas ({len(empresas_nuevas)}): {', '.join(nombres_nuevas)} "
            f"→ programadas a partir de semana 5"
        )
        
        if talleres_nuevas > slots_disponibles:
            warnings.append(
                f"⚠ Las empresas nuevas necesitan {talleres_nuevas} slots pero solo hay "
                f"{slots_disponibles} disponibles en semanas 5-13. Riesgo de INFEASIBLE."
            )

    # H8. Festivos: excluir slots específicos por día+semana
    # No empresa puede ser asignada a un slot cuyo día coincide con un festivo
    if dias_excluidos:
        for e in empresa_ids:
            for s in SEMANAS:
                for t_id in taller_ids:
                    taller = taller_map[t_id]
                    if (s, taller["diaSemana"]) in dias_excluidos:
                        model.add(assign[(e, s, t_id)] == 0)

    # ─── SOFT CONSTRAINTS ────────────────────────────────────

    penalties = []

    # S1. Equilibrio mensual: penalizar desequilibrio entre meses
    for e in empresa_ids:
        total = empresas[e]["totalAsignado"]
        if total == 0:
            continue
        ideal_per_month = total / 3.0

        semanas_set = set(SEMANAS)
        for month_start in [1, 5, 9]:
            month_end = min(month_start + 4, params.semanas + 1)
            month_weeks = [s for s in range(month_start, month_end) if s in semanas_set]
            if not month_weeks:
                continue
            month_total = sum(
                assign[(e, s, t_id)]
                for s in month_weeks for t_id in taller_ids
            )
            excess = model.new_int_var(0, 20, f"excess_{e}_{month_start}")
            model.add(excess >= month_total - int(ideal_per_month + 1))
            penalties.append(excess * params.peso_equilibrio)

    # S2. Penalizar semanas consecutivas para misma empresa
    for e in empresa_ids:
        for i in range(len(SEMANAS) - 1):
            s1 = SEMANAS[i]
            s2 = SEMANAS[i + 1]
            # Solo penalizar si realmente son consecutivas (diferencia = 1)
            if s2 - s1 != 1:
                continue

            has_s1_var = model.new_bool_var(f"has_{e}_{s1}")
            has_s2_var = model.new_bool_var(f"has_{e}_{s2}")

            model.add_max_equality(
                has_s1_var,
                [assign[(e, s1, t_id)] for t_id in taller_ids],
            )
            model.add_max_equality(
                has_s2_var,
                [assign[(e, s2, t_id)] for t_id in taller_ids],
            )

            consec = model.new_bool_var(f"consec_{e}_{s1}")
            model.add(consec <= has_s1_var)
            model.add(consec <= has_s2_var)
            model.add(consec >= has_s1_var + has_s2_var - 1)
            penalties.append(consec * params.peso_no_consecutivas)

    # S3. Turno preferido
    for e in empresa_ids:
        turno_pref = empresas[e].get("turnoPreferido")
        if turno_pref:
            for s in SEMANAS:
                for t_id in taller_ids:
                    taller = taller_map[t_id]
                    if taller.get("turno") and taller["turno"] != turno_pref:
                        penalties.append(
                            assign[(e, s, t_id)] * params.peso_turno_preferido
                        )

    if penalties:
        model.minimize(sum(penalties))

    # ─── Solve ───────────────────────────────────────────────

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = params.timeout_seconds
    solver.parameters.num_workers = 4

    status_code = solver.solve(model)
    elapsed = time.time() - start_time

    status_map = {
        cp_model.OPTIMAL: "OPTIMAL",
        cp_model.FEASIBLE: "FEASIBLE",
        cp_model.INFEASIBLE: "INFEASIBLE",
        cp_model.MODEL_INVALID: "INFEASIBLE",
        cp_model.UNKNOWN: "TIMEOUT",
    }
    status = status_map.get(status_code, "TIMEOUT")

    if status in ("INFEASIBLE", "TIMEOUT"):
        return {
            "status": status,
            "tiempo_segundos": round(elapsed, 2),
            "total_slots": 0,
            "total_ef": 0,
            "total_it": 0,
            "slots": [],
            "inviolables_pct": 0,
            "preferentes_pct": 0,
            "warnings": [f"Solver terminó con status: {status}. Revisar restricciones."],
        }

    # ─── Extraer solución ────────────────────────────────────

    slots_raw: list[dict] = []
    vacios = 0
    festivos_skipped = 0
    for s in SEMANAS:
        for t_id in taller_ids:
            taller = taller_map[t_id]

            # Skip festivo slots — they don't exist in the calendar
            if (s, taller["diaSemana"]) in dias_excluidos:
                festivos_skipped += 1
                continue

            assigned = False
            for e in empresa_ids:
                if solver.value(assign[(e, s, t_id)]) == 1:
                    slots_raw.append({
                        "semana": s,
                        "dia": taller["diaSemana"],
                        "horario": taller.get("horario", ""),
                        "turno": taller.get("turno", ""),
                        "empresa_id": e,
                        "empresa_nombre": empresas[e]["nombre"],
                        "programa": taller["programa"],
                        "taller_id": t_id,
                        "taller_nombre": taller["nombre"],
                    })
                    assigned = True
            if not assigned:
                vacios += 1
                # Incluir slot vacío para que el frontend lo muestre
                slots_raw.append({
                    "semana": s,
                    "dia": taller["diaSemana"],
                    "horario": taller.get("horario", ""),
                    "turno": taller.get("turno", ""),
                    "empresa_id": 0,
                    "empresa_nombre": "— Vacante —",
                    "programa": taller["programa"],
                    "taller_id": t_id,
                    "taller_nombre": taller["nombre"],
                })

    total_slots_posibles = len(SEMANAS) * len(taller_ids) - festivos_skipped
    if vacios > 0:
        warnings.append(
            f"{vacios}/{total_slots_posibles} slots vacantes "
            f"({round(vacios/total_slots_posibles*100)}%). "
            f"Añadir más empresas o aumentar frecuencias para llenar."
        )

    # Ordenar por semana, día, horario
    DIA_ORD = {"L": 1, "M": 2, "X": 3, "J": 4, "V": 5}
    slots_raw.sort(key=lambda x: (
        x["semana"],
        DIA_ORD.get(x["dia"], 9),
        x["horario"],
    ))

    # Métricas
    total_penalty = solver.objective_value if penalties else 0
    max_possible = max(1, len(empresa_ids) * params.semanas * params.peso_equilibrio)
    preferentes_pct = round(
        (1 - total_penalty / max_possible) * 100, 1
    )

    return {
        "status": status,
        "tiempo_segundos": round(elapsed, 2),
        "total_slots": len(slots_raw),
        "total_ef": sum(1 for s in slots_raw if s["programa"] == "EF"),
        "total_it": sum(1 for s in slots_raw if s["programa"] == "IT"),
        "slots": slots_raw,
        "inviolables_pct": 100.0,
        "preferentes_pct": max(0, preferentes_pct),
        "warnings": warnings,
    }


# ── Post-proceso: asignar ciudades ───────────────────────────

def _asignar_ciudades(
    slots_raw: list[dict],
    frecuencias: list[dict],
    madrid_id: int | None,
    restricciones: list[dict],
    warnings: list[str],
) -> list[dict]:
    """
    Post-proceso simplificado: todos los talleres son de Madrid.
    Genera sugerencias de contingencia para slots vacantes.
    """
    # Indexar restricciones
    no_comodin_ids: set[int] = set()
    max_extras_map: dict[int, int] = {}
    solo_taller_map: dict[int, str] = {}  # empresaId → nombre del taller permitido
    for r in restricciones:
        if r["clave"] == "no_comodin":
            no_comodin_ids.add(r["empresaId"])
        if r["clave"] == "max_extras":
            try:
                max_extras_map[r["empresaId"]] = int(r["valor"])
            except ValueError:
                pass
        if r["clave"] == "solo_taller":
            solo_taller_map[r["empresaId"]] = r["valor"].strip().lower()
 
    # Empresas comodín: esComodin=true Y no están en no_comodin
    comodines = [
        f for f in frecuencias
        if f.get("esComodin") and f["empresaId"] not in no_comodin_ids
    ]
    # Ordenar por score descendente para priorizar mejores candidatos
    comodines.sort(key=lambda f: f.get("scoreCalculado", 0), reverse=True)

    slots_completos = []
    for slot in slots_raw:
        eid = slot["empresa_id"]
 
        # Sugerencias de contingencia para slots vacantes
        sugerencias = None
        if eid == 0 and comodines:
            sugerencias = []
            taller_nombre_lower = slot.get("taller_nombre", "").strip().lower()
            for com in comodines:
                com_id = com["empresaId"]
                # Filtrar: si el comodín tiene solo_taller, solo sugerirlo
                # para slots cuyo taller coincide con su restricción
                if com_id in solo_taller_map:
                    nombre_permitido = solo_taller_map[com_id]
                    if (nombre_permitido not in taller_nombre_lower
                            and taller_nombre_lower not in nombre_permitido):
                        continue  # No es su taller → no sugerir
                max_ex = max_extras_map.get(com_id)
                motivo = "Comodín disponible"
                if max_ex is not None:
                    motivo += f" (max {max_ex} extras/trimestre)"
                if com_id in solo_taller_map:
                    motivo += f" (solo taller: {solo_taller_map[com_id]})"
                sugerencias.append({
                    "empresa_id": com_id,
                    "empresa_nombre": com["nombre"],
                    "motivo": motivo,
                    "prioridad": len(sugerencias) + 1,
                })
                if len(sugerencias) >= 4:
                    break
 
        slots_completos.append({
            **slot,
            "ciudad_id": madrid_id if eid != 0 else None,
            "ciudad": "MADRID" if eid != 0 else None,
            "tipo_asignacion": "BASE",
            "sugerencias": sugerencias,
        })
 
    return slots_completos