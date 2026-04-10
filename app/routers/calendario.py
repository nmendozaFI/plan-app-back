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

from fastapi import APIRouter, Depends, HTTPException
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
    semana: int
    dia: str
    horario: str
    turno: str
    empresa_id: int
    empresa_nombre: str
    programa: str
    taller_id: int
    taller_nombre: str
    ciudad_id: int | None
    ciudad: str | None
    tipo_asignacion: str
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

    # ── 6. Semanas excluidas → convertir absolutas a relativas (1-13) ──
    semanas_excluidas: set[int] = set()
    try:
        excl_result = await db.execute(
            text('''
                SELECT semana, trimestre FROM "semanaExcluida"
                WHERE trimestre = :trimestre
            '''),
            {"trimestre": trimestre},
        )
        # Mapa: primer semana absoluta del trimestre según convención YYYY-Qn
        # Q1=1, Q2=14, Q3=27, Q4=40  (13 semanas por trimestre)
        Q_OFFSET = {"Q1": 1, "Q2": 14, "Q3": 27, "Q4": 40}
        q_part = trimestre.split("-")[1] if "-" in trimestre else "Q1"
        offset = Q_OFFSET.get(q_part, 1)

        for row in excl_result.mappings().all():
            sem_abs = row["semana"]
            sem_rel = sem_abs - offset + 1  # convertir a relativa 1-13
            if 1 <= sem_rel <= 13:
                semanas_excluidas.add(sem_rel)

        if semanas_excluidas:
            warnings.append(
                f"Semanas excluidas (relativas): {sorted(semanas_excluidas)}"
            )
    except Exception:
        pass  # tabla puede no existir en entornos sin migración

    # ── 7. Ejecutar solver ───────────────────────────────────
    resultado = _ejecutar_solver(
        frecuencias=frecuencias,
        restricciones=restricciones,
        talleres=talleres,
        disponibilidad_map=disponibilidad_map,
        semanas_excluidas=semanas_excluidas,
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
        # No persistir slots vacantes (empresa_id=0) — solo existen en la respuesta JSON
        if slot["empresa_id"] == 0:
            continue
        tipo_bd = "CONTINGENCIA" if slot["tipo_asignacion"] == "HUECO" else slot["tipo_asignacion"]
        es_contingencia = slot["tipo_asignacion"] == "HUECO"
        await db.execute(
            text("""
                INSERT INTO planificacion
                    (trimestre, semana, dia, horario, turno, "empresaId", "tallerId",
                     "ciudadId", "tipoAsignacion", "esContingencia", "updatedAt")
                VALUES (
                    :tri, :sem, :dia, :horario, :turno, :eid, :tid,
                    :cid, :tipo, :contingencia, NOW()
                )
            """),
            {
                "tri": trimestre,
                "sem": slot["semana"],
                "dia": slot["dia"],
                "horario": slot["horario"],
                "turno": slot["turno"],
                "eid": slot["empresa_id"],
                "tid": slot["taller_id"],
                "cid": slot.get("ciudad_id"),
                "tipo": tipo_bd,
                "contingencia": es_contingencia,
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
    """Lee el calendario generado de un trimestre."""
    result = await db.execute(
        text("""
            SELECT p.semana, p.dia, p.horario,
                   COALESCE(p.turno, t.turno) AS turno,
                   p."empresaId" AS empresa_id,
                   e.nombre AS empresa_nombre,
                   t.programa,
                   p."tallerId" AS taller_id,
                   t.nombre AS taller_nombre,
                   p."ciudadId" AS ciudad_id,
                   c.nombre AS ciudad,
                   p."tipoAsignacion" AS tipo_asignacion
            FROM planificacion p
            JOIN empresa e ON e.id = p."empresaId"
            JOIN taller t ON t.id = p."tallerId"
            LEFT JOIN ciudad c ON c.id = p."ciudadId"
            WHERE p.trimestre = :trimestre
            ORDER BY p.semana,
                     CASE p.dia WHEN 'L' THEN 1 WHEN 'M' THEN 2 WHEN 'X' THEN 3 WHEN 'J' THEN 4 WHEN 'V' THEN 5 END,
                     p.horario
        """),
        {"trimestre": trimestre},
    )
    rows = [dict(r) for r in result.mappings().all()]
    return {
        "trimestre": trimestre,
        "total_slots": len(rows),
        "slots": rows,
    }


@router.post("/{trimestre}/exportar-excel")
async def exportar_excel(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Genera Excel del calendario con TODOS los slots (asignados + vacantes).
    Incluye columnas Estado/Confirmado para que el planificador pueda
    ir completando manualmente y luego importar como histórico.
    """
    from fastapi.responses import StreamingResponse
    from io import BytesIO
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    # ── 1. Cargar asignaciones existentes ─────────────────────
    result = await db.execute(
        text("""
            SELECT p.semana, p.dia, p.horario,
                   COALESCE(p.turno, t.turno) AS turno,
                   e.nombre AS empresa,
                   t.nombre AS taller,
                   t.programa,
                   c.nombre AS ciudad,
                   p."tipoAsignacion" AS tipo
            FROM planificacion p
            JOIN empresa e ON e.id = p."empresaId"
            JOIN taller t ON t.id = p."tallerId"
            LEFT JOIN ciudad c ON c.id = p."ciudadId"
            WHERE p.trimestre = :trimestre
            ORDER BY p.semana,
                     CASE p.dia WHEN 'L' THEN 1 WHEN 'M' THEN 2 WHEN 'X' THEN 3 WHEN 'J' THEN 4 WHEN 'V' THEN 5 END,
                     p.horario
        """),
        {"trimestre": trimestre},
    )
    assigned_rows = [dict(r) for r in result.mappings().all()]

    # ── 2. Cargar catálogo de talleres para generar vacantes ──
    talleres_result = await db.execute(
        text("""
            SELECT id, nombre, programa, "diaSemana", horario, turno
            FROM taller WHERE activo = true
            ORDER BY CASE "diaSemana" WHEN 'L' THEN 1 WHEN 'M' THEN 2 WHEN 'X' THEN 3 WHEN 'J' THEN 4 WHEN 'V' THEN 5 END,
                     horario
        """)
    )
    talleres = [dict(r) for r in talleres_result.mappings().all()]

    # ── 3. Semanas excluidas ──────────────────────────────────
    semanas_excluidas: set[int] = set()
    try:
        excl_result = await db.execute(
            text('SELECT semana FROM "semanaExcluida" WHERE trimestre = :tri'),
            {"tri": trimestre},
        )
        Q_OFFSET = {"Q1": 1, "Q2": 14, "Q3": 27, "Q4": 40}
        q_part = trimestre.split("-")[1] if "-" in trimestre else "Q1"
        offset = Q_OFFSET.get(q_part, 1)
        for row in excl_result.mappings().all():
            sem_rel = row["semana"] - offset + 1
            if 1 <= sem_rel <= 13:
                semanas_excluidas.add(sem_rel)
    except Exception:
        pass

    SEMANAS = [s for s in range(1, 14) if s not in semanas_excluidas]

    # ── 4. Índice de slots asignados ──────────────────────────
    assigned_index: set[tuple] = set()
    for r in assigned_rows:
        assigned_index.add((r["semana"], r["dia"], r["horario"], r["programa"]))

    # ── 5. Generar filas completas (asignados + vacantes) ─────
    DIA_ORD = {"L": 1, "M": 2, "X": 3, "J": 4, "V": 5}
    all_rows: list[dict] = []

    for sem in SEMANAS:
        for taller in talleres:
            dia = taller["diaSemana"]
            horario = taller["horario"]
            programa = taller["programa"]
            key = (sem, dia, horario, programa)

            # Buscar si hay asignación
            assigned = None
            for r in assigned_rows:
                if (r["semana"] == sem and r["dia"] == dia
                        and r["horario"] == horario and r["programa"] == programa):
                    assigned = r
                    break

            if assigned:
                all_rows.append({
                    "semana": sem,
                    "dia": dia,
                    "horario": horario,
                    "turno": assigned["turno"],
                    "empresa": assigned["empresa"],
                    "taller": assigned["taller"],
                    "programa": programa,
                    "ciudad": assigned.get("ciudad", "MADRID"),
                    "tipo": assigned["tipo"],
                    "estado": "PLANIFICADO",
                    "confirmado": "",
                })
            else:
                all_rows.append({
                    "semana": sem,
                    "dia": dia,
                    "horario": horario,
                    "turno": taller.get("turno", ""),
                    "empresa": "",
                    "taller": taller["nombre"],
                    "programa": programa,
                    "ciudad": "",
                    "tipo": "VACANTE",
                    "estado": "VACANTE",
                    "confirmado": "",
                })

    # Ordenar
    all_rows.sort(key=lambda r: (
        r["semana"],
        DIA_ORD.get(r["dia"], 9),
        r["horario"],
        0 if r["programa"] == "EF" else 1,
    ))

    # ── 6. Crear Excel con formato profesional ────────────────
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
    vacante_fill = PatternFill("solid", fgColor="FEF3C7")  # amarillo suave
    vacante_font = Font(name="Arial", size=9, color="92400E", italic=True)
    normal_font = Font(name="Arial", size=9)
    ef_fill = PatternFill("solid", fgColor="F1F5F9")  # gris muy claro
    it_fill = PatternFill("solid", fgColor="EDE9FE")  # violeta muy claro
    sem_font = Font(name="Arial", size=9, bold=True)

    # Headers
    headers = [
        "Semana", "Día", "Horario", "Turno", "Empresa",
        "Taller", "Programa", "Ciudad", "Tipo", "Estado", "Confirmado",
    ]
    col_widths = [8, 6, 14, 10, 25, 42, 10, 12, 12, 12, 14]

    for col, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border
        ws.column_dimensions[cell.column_letter].width = w

    # Freeze header
    ws.freeze_panes = "A2"
    # Auto-filter
    ws.auto_filter.ref = f"A1:K{len(all_rows) + 1}"

    # Data rows
    prev_semana = None
    for i, row in enumerate(all_rows, 2):
        is_vacante = row["tipo"] == "VACANTE"
        is_new_semana = row["semana"] != prev_semana
        prev_semana = row["semana"]

        values = [
            row["semana"], row["dia"], row["horario"], row["turno"],
            row["empresa"], row["taller"], row["programa"],
            row["ciudad"], row["tipo"], row["estado"], row["confirmado"],
        ]

        for col, val in enumerate(values, 1):
            cell = ws.cell(row=i, column=col, value=val)
            cell.border = thin_border
            cell.alignment = Alignment(vertical="center")

            if is_vacante:
                cell.fill = vacante_fill
                cell.font = vacante_font
            else:
                cell.font = normal_font
                # Color sutil por programa
                if row["programa"] == "IT":
                    cell.fill = it_fill

            # Semana en bold
            if col == 1:
                cell.font = Font(
                    name="Arial", size=9, bold=True,
                    color="92400E" if is_vacante else "000000",
                )
                cell.alignment = Alignment(horizontal="center", vertical="center")

    # ── 7. Hoja resumen ───────────────────────────────────────
    ws_resumen = wb.create_sheet("Resumen")
    ws_resumen["A1"] = f"Calendario {trimestre}"
    ws_resumen["A1"].font = Font(name="Arial", bold=True, size=14)

    resumen_data = [
        ("Semanas disponibles", len(SEMANAS)),
        ("Semanas excluidas", f"{sorted(semanas_excluidas)}" if semanas_excluidas else "Ninguna"),
        ("Slots por semana", "20 (14 EF + 6 IT)"),
        ("Total slots", len(all_rows)),
        ("Asignados", sum(1 for r in all_rows if r["tipo"] != "VACANTE")),
        ("Vacantes", sum(1 for r in all_rows if r["tipo"] == "VACANTE")),
        ("", ""),
        ("INSTRUCCIONES", ""),
        ("1. Columna 'Empresa'", "Completar vacantes con empresa asignada"),
        ("2. Columna 'Estado'", "Cambiar a OK o CANCELADO según resultado"),
        ("3. Columna 'Confirmado'", "Marcar SÍ cuando empresa confirme"),
        ("4. Al cierre del trimestre", "Importar como histórico en el sistema"),
    ]

    for i, (label, value) in enumerate(resumen_data, 3):
        cell_a = ws_resumen.cell(row=i, column=1, value=label)
        cell_b = ws_resumen.cell(row=i, column=2, value=value)
        cell_a.font = Font(name="Arial", size=10, bold=True if label else False)
        cell_b.font = Font(name="Arial", size=10)

    ws_resumen.column_dimensions["A"].width = 30
    ws_resumen.column_dimensions["B"].width = 40

    # ── 8. Devolver ───────────────────────────────────────────
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
    for s in SEMANAS:
        for t_id in taller_ids:
            taller = taller_map[t_id]
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

    total_slots_posibles = len(SEMANAS) * len(taller_ids)
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