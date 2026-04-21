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

import asyncio
import os

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class CalendarioInput(BaseModel):
    trimestre: str
    timeout_seconds: int = 120
    max_ef: int = 14
    max_it: int = 6
    semanas: int = 13
    peso_equilibrio: int = 10
    peso_no_consecutivas: int = 8
    peso_turno_preferido: int = 3
    peso_intercalar_ef_it: int = 6      # S4: Intercalar EF/IT en meses distintos
    peso_diversidad_talleres: int = 4   # S5: Penalizar repetición del mismo taller


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
    motivo_cambio: str | None = None  # "EMPRESA_CANCELO" | "DECISION_PLANIFICADOR"


class SlotBatchUpdateItem(BaseModel):
    """Single item in a batch update."""
    slot_id: int
    estado: str | None = None
    confirmado: bool | None = None
    empresa_id: int | None = None
    notas: str | None = None
    motivo_cambio: str | None = None  # "EMPRESA_CANCELO" | "DECISION_PLANIFICADOR"


class SlotBatchUpdateInput(BaseModel):
    """Input for batch updating multiple slots."""
    updates: list[SlotBatchUpdateItem]


class ValidarAsignacionInput(BaseModel):
    """Input for validating a company assignment to a slot."""
    slot_id: int
    empresa_id: int


class ValidarAsignacionResult(BaseModel):
    """Result of validating a company assignment."""
    ok: bool  # True if no warnings
    warnings: list[str]
    restricciones_violadas: list[str]  # e.g. ["solo_dia: EY solo puede Viernes, slot es Martes"]


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

    # ── 3. Talleres (slots fijos) with per-week calendar overrides ───
    # Uses SemanaConfig to determine which talleres are available each week.
    # In intensive weeks: EF afternoon OFF, IT afternoon moved to Wednesday morning.
    from app.routers.calendario_anual import cargar_talleres_semana

    # Parse trimestre to get year and week range
    anio = int(trimestre.split("-")[0])
    quarter_num = int(trimestre.split("Q")[1])
    iso_week_start = (quarter_num - 1) * 13 + 1

    # Load talleres for each week of the trimestre
    talleres_por_semana: dict[int, list[dict]] = {}
    all_taller_ids: set[int] = set()
    num_semanas = params.semanas  # typically 13

    for semana_rel in range(1, num_semanas + 1):
        iso_week = iso_week_start + semana_rel - 1
        talleres_semana = await cargar_talleres_semana(db, anio, iso_week)
        # Convert to solver format (id, diaSemana, etc.)
        talleres_por_semana[semana_rel] = [
            {
                "id": t["taller_id"],
                "nombre": t["nombre"],
                "programa": t["programa"],
                "diaSemana": t["dia_semana"],
                "horario": t["horario"],
                "turno": t["turno"],
                "es_extra": t.get("es_extra", False),
                "extra_id": t.get("extra_id"),
            }
            for t in talleres_semana
        ]
        for t in talleres_semana:
            all_taller_ids.add(t["taller_id"])

    # For backward compatibility, create a "union" list of all talleres (for summary)
    # Get base talleres info for the union
    base_talleres_result = await db.execute(
        text("""
            SELECT id, nombre, programa, "diaSemana", horario, turno
            FROM taller WHERE activo = true ORDER BY id
        """)
    )
    base_talleres = [dict(r) for r in base_talleres_result.mappings().all()]
    talleres = [t for t in base_talleres if t["id"] in all_taller_ids]

    if len(talleres) == 0:
        raise HTTPException(
            status_code=500,
            detail="No hay talleres activos para este trimestre. "
                   "Verificar configuración de talleres en /planificacion/talleres.",
        )

    # Count by week type for summary
    normal_weeks = sum(1 for s in range(1, num_semanas + 1) if len(talleres_por_semana[s]) >= 18)
    intensive_weeks = num_semanas - normal_weeks
    avg_talleres = sum(len(talleres_por_semana[s]) for s in range(1, num_semanas + 1)) // num_semanas
    talleres_ef = [t for t in talleres if t["programa"] == "EF"]
    talleres_it = [t for t in talleres if t["programa"] == "IT"]
    warnings.append(
        f"Catálogo {trimestre}: {len(talleres_ef)} EF + {len(talleres_it)} IT base, "
        f"{normal_weeks} semanas normales + {intensive_weeks} intensivas, ~{avg_talleres} talleres/semana promedio"
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

    # ── 7. Ejecutar solver (en thread separado para no bloquear event loop) ───
    resultado = await asyncio.to_thread(
        _ejecutar_solver,
        frecuencias=frecuencias,
        restricciones=restricciones,
        talleres=talleres,
        talleres_por_semana=talleres_por_semana,
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
                    (trimestre, semana, dia, horario, turno, "empresaId", "empresaIdOriginal",
                     "tallerId", "ciudadId", "tipoAsignacion", "esContingencia", estado, confirmado, "updatedAt")
                VALUES (
                    :tri, :sem, :dia, :horario, :turno, :eid, :eid_original,
                    :tid, :cid, :tipo, :contingencia, :estado, false, NOW()
                )
            """),
            {
                "tri": trimestre,
                "sem": slot["semana"],
                "dia": slot["dia"],
                "horario": slot["horario"],
                "turno": slot["turno"],
                "eid": empresa_id,
                "eid_original": empresa_id,  # Same as eid at generation time, NEVER updated later
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
                   p."empresaIdOriginal" AS empresa_id_original,
                   e.nombre AS empresa_nombre,
                   t.programa,
                   p."tallerId" AS taller_id,
                   t.nombre AS taller_nombre,
                   p."ciudadId" AS ciudad_id,
                   c.nombre AS ciudad,
                   p."tipoAsignacion" AS tipo_asignacion,
                   p.estado,
                   p.confirmado,
                   p.notas,
                   p."motivoCambio" AS motivo_cambio
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


@router.post("/{trimestre}/validar-asignacion", response_model=ValidarAsignacionResult)
async def validar_asignacion(
    trimestre: str,
    body: ValidarAsignacionInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Validates if assigning a company to a slot violates any restrictions.
    Returns warnings but does NOT block the assignment.
    Used by the frontend to show warnings before confirming.
    """
    warnings = []
    restricciones_violadas = []

    # 1. Get slot info
    slot_result = await db.execute(
        text("""
            SELECT p.id, p.semana, p.dia, p."tallerId",
                   t.nombre AS taller_nombre, t.programa, t.turno AS taller_turno
            FROM planificacion p
            JOIN taller t ON t.id = p."tallerId"
            WHERE p.id = :slot_id AND p.trimestre = :tri
        """),
        {"slot_id": body.slot_id, "tri": trimestre},
    )
    slot = slot_result.mappings().first()
    if not slot:
        raise HTTPException(404, "Slot no encontrado")

    slot = dict(slot)

    # 2. Get company info
    emp_result = await db.execute(
        text('SELECT id, nombre, "turnoPreferido" FROM empresa WHERE id = :id'),
        {"id": body.empresa_id},
    )
    empresa = emp_result.mappings().first()
    if not empresa:
        raise HTTPException(404, "Empresa no encontrada")
    empresa = dict(empresa)

    # 3. Get restrictions for this company
    rest_result = await db.execute(
        text('SELECT tipo, clave, valor FROM restriccion WHERE "empresaId" = :eid'),
        {"eid": body.empresa_id},
    )
    restricciones = [dict(r) for r in rest_result.mappings().all()]

    # 4. Check solo_dia
    for r in restricciones:
        if r["clave"] == "solo_dia":
            if slot["dia"] != r["valor"]:
                dia_nombres = {"L": "Lunes", "M": "Martes", "X": "Miércoles", "J": "Jueves", "V": "Viernes"}
                restricciones_violadas.append(
                    f"solo_dia: {empresa['nombre']} solo puede {dia_nombres.get(r['valor'], r['valor'])}, "
                    f"pero el slot es {dia_nombres.get(slot['dia'], slot['dia'])}"
                )

    # 5. Check solo_taller
    for r in restricciones:
        if r["clave"] == "solo_taller":
            taller_nombre = slot["taller_nombre"].strip().lower()
            restriccion_valor = r["valor"].strip().lower()
            if restriccion_valor not in taller_nombre and taller_nombre not in restriccion_valor:
                restricciones_violadas.append(
                    f"solo_taller: {empresa['nombre']} solo imparte '{r['valor']}', "
                    f"pero el taller es '{slot['taller_nombre']}'"
                )

    # 6. Check no_comodin (warn that this company shouldn't be used as substitute)
    for r in restricciones:
        if r["clave"] == "no_comodin":
            restricciones_violadas.append(
                f"no_comodin: {empresa['nombre']} no debería usarse como comodín/sustituta"
            )

    # 7. Check max_extras — count how many times this company already appears beyond its frequency
    for r in restricciones:
        if r["clave"] == "max_extras":
            max_extras = int(r["valor"])
            # Count current assignments for this company
            count_result = await db.execute(
                text("""
                    SELECT COUNT(*) AS total FROM planificacion
                    WHERE trimestre = :tri AND "empresaId" = :eid
                    AND estado NOT IN ('CANCELADO', 'VACANTE')
                """),
                {"tri": trimestre, "eid": body.empresa_id},
            )
            current = count_result.scalar() or 0
            # Get original frequency
            freq_result = await db.execute(
                text("""
                    SELECT "totalAsignado" FROM frecuencia
                    WHERE trimestre = :tri AND "empresaId" = :eid
                """),
                {"tri": trimestre, "eid": body.empresa_id},
            )
            freq_row = freq_result.mappings().first()
            original_freq = int(freq_row["totalAsignado"]) if freq_row else 0
            extras_used = max(0, current - original_freq)
            if extras_used >= max_extras:
                restricciones_violadas.append(
                    f"max_extras: {empresa['nombre']} ya tiene {extras_used} extras "
                    f"(máximo permitido: {max_extras})"
                )

    # 8. Check if company already has a slot this week (H6 violation)
    week_result = await db.execute(
        text("""
            SELECT COUNT(*) AS total FROM planificacion
            WHERE trimestre = :tri AND semana = :sem AND "empresaId" = :eid
            AND estado NOT IN ('CANCELADO', 'VACANTE')
            AND id != :slot_id
        """),
        {"tri": trimestre, "sem": slot["semana"], "eid": body.empresa_id, "slot_id": body.slot_id},
    )
    week_count = week_result.scalar() or 0
    if week_count > 0:
        warnings.append(
            f"{empresa['nombre']} ya tiene {week_count} taller(es) en la semana {slot['semana']}"
        )

    # 9. Check turno preference (soft warning)
    turno_pref = empresa.get("turnoPreferido")
    if turno_pref and slot.get("taller_turno") and slot["taller_turno"] != turno_pref:
        turno_nombres = {"M": "Mañana", "T": "Tarde"}
        warnings.append(
            f"{empresa['nombre']} prefiere turno de {turno_nombres.get(turno_pref, turno_pref)}, "
            f"pero el slot es de {turno_nombres.get(slot['taller_turno'], slot['taller_turno'])}"
        )

    # Combine
    all_warnings = restricciones_violadas + warnings

    return {
        "ok": len(all_warnings) == 0,
        "warnings": all_warnings,
        "restricciones_violadas": restricciones_violadas,
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
                   e_orig.nombre AS empresa_original,
                   t.nombre AS taller,
                   t.programa,
                   c.nombre AS ciudad,
                   p."tipoAsignacion" AS tipo,
                   p.estado,
                   p.confirmado,
                   p.notas,
                   p."motivoCambio" AS motivo_cambio
            FROM planificacion p
            JOIN taller t ON t.id = p."tallerId"
            LEFT JOIN empresa e ON e.id = p."empresaId"
            LEFT JOIN empresa e_orig ON e_orig.id = p."empresaIdOriginal"
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
    has_motivo = False
    has_empresa_original = False

    for row in db_rows:
        estado = row["estado"] or "PLANIFICADO"
        confirmado = row["confirmado"]
        notas = row["notas"]
        motivo = row.get("motivo_cambio")
        empresa_original = row.get("empresa_original")

        if notas:
            has_notas = True
        if motivo:
            has_motivo = True
        # Show empresa original column if any slot has a different original empresa
        if empresa_original and empresa_original != row.get("empresa"):
            has_empresa_original = True

        # Format motivo_cambio for display
        motivo_display = ""
        if motivo == "EMPRESA_CANCELO":
            motivo_display = "Empresa canceló"
        elif motivo == "DECISION_PLANIFICADOR":
            motivo_display = "Decisión planificador"

        all_rows.append({
            "semana": row["semana"],
            "fecha": calcular_fecha_slot(trimestre, row["semana"], row["dia"]),
            "dia": row["dia"],
            "horario": row["horario"],
            "turno": row["turno"] or "",
            "empresa": row["empresa"] or "",
            "empresa_original": empresa_original or "",
            "taller": row["taller"],
            "programa": row["programa"],
            "ciudad": row["ciudad"] or "",
            "tipo": row["tipo"] or "BASE",
            "estado": estado,
            "confirmado": "SÍ" if confirmado else "",
            "notas": notas or "",
            "motivo_cambio": motivo_display,
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
    ]
    col_widths = [8, 14, 6, 14, 10, 25]

    if has_empresa_original:
        headers.append("Empresa Original")
        col_widths.append(25)

    headers.extend(["Taller", "Programa", "Ciudad", "Tipo", "Estado", "Confirmado"])
    col_widths.extend([42, 10, 12, 12, 12, 14])

    if has_notas:
        headers.append("Notas")
        col_widths.append(30)

    if has_motivo:
        headers.append("Motivo cambio")
        col_widths.append(20)

    for col, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border
        ws.column_dimensions[cell.column_letter].width = w

    # Freeze header
    ws.freeze_panes = "A2"
    # Auto-filter — calculate last column dynamically
    col_count = len(headers)
    last_col = chr(ord('A') + col_count - 1) if col_count <= 26 else "Z"
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
            row["empresa"],
        ]
        if has_empresa_original:
            values.append(row["empresa_original"])
        values.extend([
            row["taller"], row["programa"],
            row["ciudad"], row["tipo"], row["estado"], row["confirmado"],
        ])
        if has_notas:
            values.append(row["notas"])
        if has_motivo:
            values.append(row["motivo_cambio"])

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
        text('SELECT id, "empresaId", "empresaIdOriginal", estado, semana FROM planificacion WHERE id = :id AND trimestre = :tri'),
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
    # Skip H6 validation when planner explicitly sets a motivo_cambio (either reason = conscious override)
    is_forced_assignment = body.motivo_cambio in ("DECISION_PLANIFICADOR", "EMPRESA_CANCELO")

    if body.empresa_id is not None or (body.estado and body.estado == "VACANTE"):
        # If clearing empresa (setting to vacancy)
        if body.empresa_id is None and body.estado == "VACANTE":
            updates.append('"empresaId" = NULL')
        elif body.empresa_id is not None:
            # Verify company doesn't already have a slot this week (H6 constraint)
            # SKIP validation if motivo_cambio is set (planner consciously overriding)
            if not is_forced_assignment:
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
            # WRITE-ONCE: Set empresaIdOriginal only if it's currently NULL
            # This preserves the original solver assignment for traceability
            if existing["empresaIdOriginal"] is None:
                updates.append('"empresaIdOriginal" = :empresa_id_original')
                params["empresa_id_original"] = body.empresa_id

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

    # Handle motivo_cambio
    if body.motivo_cambio is not None:
        updates.append('"motivoCambio" = :motivo_cambio')
        params["motivo_cambio"] = body.motivo_cambio
    # Auto-set motivo if estado is CANCELADO and no motivo provided (default: empresa cancelled)
    elif body.estado == "CANCELADO" and body.motivo_cambio is None:
        updates.append('"motivoCambio" = :motivo_cambio')
        params["motivo_cambio"] = "EMPRESA_CANCELO"

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
                   p."empresaIdOriginal" AS empresa_id_original,
                   e.nombre AS empresa_nombre,
                   t.programa,
                   p."tallerId" AS taller_id,
                   t.nombre AS taller_nombre,
                   p."ciudadId" AS ciudad_id,
                   c.nombre AS ciudad,
                   p."tipoAsignacion" AS tipo_asignacion,
                   p.estado,
                   p.confirmado,
                   p.notas,
                   p."motivoCambio" AS motivo_cambio
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
            # Build dynamic UPDATE for each slot - include empresaIdOriginal for write-once logic
            check = await db.execute(
                text('SELECT id, "empresaIdOriginal" FROM planificacion WHERE id = :id AND trimestre = :tri'),
                {"id": item.slot_id, "tri": trimestre},
            )
            existing = check.mappings().first()
            if not existing:
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
                # WRITE-ONCE: Set empresaIdOriginal only if it's currently NULL
                if existing["empresaIdOriginal"] is None:
                    updates.append('"empresaIdOriginal" = :empresa_id_original')
                    params["empresa_id_original"] = item.empresa_id

            if item.notas is not None:
                updates.append("notas = :notas")
                params["notas"] = item.notas

            # Handle motivo_cambio
            if item.motivo_cambio is not None:
                updates.append('"motivoCambio" = :motivo_cambio')
                params["motivo_cambio"] = item.motivo_cambio
            # Auto-set motivo if estado is CANCELADO and no motivo provided
            elif item.estado == "CANCELADO" and item.motivo_cambio is None:
                updates.append('"motivoCambio" = :motivo_cambio')
                params["motivo_cambio"] = "EMPRESA_CANCELO"

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


# ── Análisis: Planificado vs Realizado ──────────────────────


class EmpresaAnalisis(BaseModel):
    """Per-company analysis metrics."""
    empresa_id: int
    empresa_nombre: str
    asignados_solver: int
    cumplidos: int
    sustituida: int
    cancelados: int
    pendientes: int
    extras_cubiertos: int
    tasa_cumplimiento: float
    tasa_sustitucion: float
    sugerencia: str  # REDUCIR | REVISAR | MANTENER | SOLO_COMODIN


class CambioSlot(BaseModel):
    """Detail of a slot where company was substituted."""
    semana: int
    dia: str
    taller: str
    programa: str
    empresa_original: str
    empresa_final: str


class AnalisisResumen(BaseModel):
    """Global summary metrics."""
    total_slots_asignados: int
    cumplidos_sin_cambio: int
    sustituidos: int
    cancelados: int
    pendientes: int
    tasa_cumplimiento_global: float
    tasa_sustitucion_global: float


class AnalisisResponse(BaseModel):
    """Full analysis response."""
    trimestre: str
    resumen: AnalisisResumen
    por_empresa: list[EmpresaAnalisis]
    cambios: list[CambioSlot]
    total_empresas: int


@router.get("/{trimestre}/analisis", response_model=AnalisisResponse)
async def analisis_planificado_vs_realizado(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Compares solver's original assignment vs final state.
    Returns per-company metrics: cumplimiento, sustituciones, extras.

    This endpoint helps identify:
    - Companies that were substituted (unreliable)
    - Companies that stepped in as substitutes (reliable/flexible)
    - The real "compliance rate" of each company
    """
    # 1. Get all slots with both current and original empresa
    result = await db.execute(
        text("""
            SELECT
                p.id,
                p.semana,
                p.dia,
                p."tallerId" AS taller_id,
                t.nombre AS taller_nombre,
                t.programa,
                p."empresaId" AS empresa_id_final,
                e_final.nombre AS empresa_final,
                p."empresaIdOriginal" AS empresa_id_original,
                e_orig.nombre AS empresa_original,
                p.estado
            FROM planificacion p
            JOIN taller t ON t.id = p."tallerId"
            LEFT JOIN empresa e_final ON e_final.id = p."empresaId"
            LEFT JOIN empresa e_orig ON e_orig.id = p."empresaIdOriginal"
            WHERE p.trimestre = :trimestre
            ORDER BY p.semana, p.dia
        """),
        {"trimestre": trimestre},
    )
    slots = [dict(r) for r in result.mappings().all()]

    if not slots:
        raise HTTPException(status_code=404, detail=f"No hay datos para {trimestre}")

    # 2. Compute per-company metrics
    empresas_stats: dict[int, dict] = {}

    for slot in slots:
        eid_orig = slot["empresa_id_original"]
        eid_final = slot["empresa_id_final"]
        estado = slot["estado"]

        # Skip vacantes (no company assigned by solver)
        if eid_orig is None and eid_final is None:
            continue

        # Track original assignments
        if eid_orig is not None:
            if eid_orig not in empresas_stats:
                empresas_stats[eid_orig] = {
                    "empresa_id": eid_orig,
                    "empresa_nombre": slot["empresa_original"],
                    "asignados_solver": 0,
                    "cumplidos": 0,       # Slot ended with this company (OK/CONFIRMADO)
                    "sustituida": 0,      # Slot ended with DIFFERENT company
                    "cancelados": 0,      # Slot was CANCELADO
                    "pendientes": 0,      # Slot still PLANIFICADO/CONFIRMADO
                    "extras_cubiertos": 0,  # Slots where this company REPLACED another
                }
            empresas_stats[eid_orig]["asignados_solver"] += 1

            if eid_final == eid_orig and estado in ("OK", "CONFIRMADO"):
                empresas_stats[eid_orig]["cumplidos"] += 1
            elif eid_final != eid_orig and eid_final is not None:
                empresas_stats[eid_orig]["sustituida"] += 1
            elif estado == "CANCELADO":
                empresas_stats[eid_orig]["cancelados"] += 1
            else:
                empresas_stats[eid_orig]["pendientes"] += 1

        # Track substitute assignments (company was NOT in solver but IS in final)
        if eid_final is not None and eid_final != eid_orig:
            if eid_final not in empresas_stats:
                empresas_stats[eid_final] = {
                    "empresa_id": eid_final,
                    "empresa_nombre": slot["empresa_final"],
                    "asignados_solver": 0,
                    "cumplidos": 0,
                    "sustituida": 0,
                    "cancelados": 0,
                    "pendientes": 0,
                    "extras_cubiertos": 0,
                }
            empresas_stats[eid_final]["extras_cubiertos"] += 1

    # 3. Compute rates
    analysis = []
    for eid, stats in empresas_stats.items():
        asignados = stats["asignados_solver"]
        tasa_cumplimiento = round(
            (stats["cumplidos"] / asignados * 100) if asignados > 0 else 0, 1
        )
        tasa_sustitucion = round(
            (stats["sustituida"] / asignados * 100) if asignados > 0 else 0, 1
        )

        analysis.append({
            **stats,
            "tasa_cumplimiento": tasa_cumplimiento,
            "tasa_sustitucion": tasa_sustitucion,
            # For next quarter: if tasa_cumplimiento < 70%, suggest reduction
            "sugerencia": (
                "REDUCIR" if tasa_cumplimiento < 70 and asignados > 0
                else "MANTENER" if tasa_cumplimiento >= 90
                else "REVISAR" if asignados > 0
                else "SOLO_COMODIN"
            ),
        })

    # Sort by tasa_cumplimiento ascending (worst first)
    analysis.sort(key=lambda x: (x["tasa_cumplimiento"], -x["asignados_solver"]))

    # 4. Global summary
    total_slots = len([s for s in slots if s["empresa_id_original"] is not None])
    total_cumplidos = sum(s["cumplidos"] for s in empresas_stats.values())
    total_sustituidos = sum(s["sustituida"] for s in empresas_stats.values())
    total_cancelados = sum(s["cancelados"] for s in empresas_stats.values())
    total_pendientes = sum(s["pendientes"] for s in empresas_stats.values())

    # Slot-level changes detail (for the changes table)
    cambios = []
    for slot in slots:
        eid_orig = slot["empresa_id_original"]
        eid_final = slot["empresa_id_final"]
        if eid_orig is not None and eid_final is not None and eid_orig != eid_final:
            cambios.append({
                "semana": slot["semana"],
                "dia": slot["dia"],
                "taller": slot["taller_nombre"],
                "programa": slot["programa"],
                "empresa_original": slot["empresa_original"],
                "empresa_final": slot["empresa_final"],
            })

    return {
        "trimestre": trimestre,
        "resumen": {
            "total_slots_asignados": total_slots,
            "cumplidos_sin_cambio": total_cumplidos,
            "sustituidos": total_sustituidos,
            "cancelados": total_cancelados,
            "pendientes": total_pendientes,
            "tasa_cumplimiento_global": round(
                (total_cumplidos / total_slots * 100) if total_slots > 0 else 0, 1
            ),
            "tasa_sustitucion_global": round(
                (total_sustituidos / total_slots * 100) if total_slots > 0 else 0, 1
            ),
        },
        "por_empresa": analysis,
        "cambios": cambios,
        "total_empresas": len(analysis),
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
                p."empresaIdOriginal" AS empresa_id_original,
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
                "empresa_id_original": slot.get("empresa_id_original"),  # For write-once logic
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
            current_empresa_id_original = upd.get("empresa_id_original")

            set_parts = []
            params = {"id": slot_id}

            if "empresa_id" in changes:
                if changes["empresa_id"] is None:
                    set_parts.append('"empresaId" = NULL')
                else:
                    set_parts.append('"empresaId" = :empresa_id')
                    params["empresa_id"] = changes["empresa_id"]
                    # WRITE-ONCE: Set empresaIdOriginal only if it's currently NULL
                    if current_empresa_id_original is None:
                        set_parts.append('"empresaIdOriginal" = :empresa_id_original')
                        params["empresa_id_original"] = changes["empresa_id"]

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


# ── Recalcular Scores ────────────────────────────────────────


class RecalcularScoresResult(BaseModel):
    empresas_actualizadas: int
    detalle: list[dict]
    warnings: list[str]


@router.post("/recalcular-scores", response_model=RecalcularScoresResult)
async def recalcular_scores(db: AsyncSession = Depends(get_db)):
    """
    Recalculate all company scores from historical data.
    Can be called manually anytime — useful after importing legacy data.
    """
    from app.routers.scores import calcular_scores_trimestre

    warnings: list[str] = []
    result = await calcular_scores_trimestre(db, "", warnings)
    await db.commit()

    return {
        "empresas_actualizadas": result["empresas_actualizadas"],
        "detalle": result["detalle"],
        "warnings": warnings,
    }


# ── Cerrar Trimestre ─────────────────────────────────────────


class CerrarTrimestreResult(BaseModel):
    trimestre: str
    total_ok: int
    total_cancelado: int
    total_ignorado: int  # VACANTE + PLANIFICADO slots
    preview: bool
    scores_actualizados: int = 0
    score_warnings: list[str] = []


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
                p."empresaIdOriginal" AS empresa_id_original,
                p."tallerId" AS taller_id,
                p.semana,
                p.dia,
                p.estado,
                p."motivoCambio" AS motivo_cambio,
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
                    "empresaId", "empresaIdOriginal", "tallerId", fecha, estado, ciudad, trimestre, "motivoCambio", "createdAt"
                )
                VALUES (:eid, :eid_original, :tid, :fecha, :estado, :ciudad, :tri, :motivo, NOW())
            """),
            {
                "eid": slot["empresa_id"],
                "eid_original": slot.get("empresa_id_original"),
                "tid": slot["taller_id"],
                "fecha": fecha,
                "estado": estado_db,
                "ciudad": slot["ciudad"] or "MADRID",
                "tri": trimestre,
                "motivo": slot.get("motivo_cambio"),
            },
        )

    await db.commit()

    # ── 9. Auto-calculate scores based on all historical data ────
    from app.routers.scores import calcular_scores_trimestre

    score_warnings: list[str] = []
    score_result = await calcular_scores_trimestre(db, trimestre, score_warnings)
    await db.commit()

    return {
        "trimestre": trimestre,
        "total_ok": total_ok,
        "total_cancelado": total_cancelado,
        "total_ignorado": total_ignorado,
        "preview": False,
        "scores_actualizados": score_result["empresas_actualizadas"],
        "score_warnings": score_warnings,
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
# SOLVER CP-SAT — Modelo de slots fijos (OPTIMIZADO V11)
# ══════════════════════════════════════════════════════════════

def _generate_hints(
    possible: set,
    empresa_ids: list[int],
    empresas: dict,
    SEMANAS: list[int],
    taller_ids: list[int],
    taller_map: dict,
    taller_ids_ef: list[int],
    taller_ids_it: list[int],
) -> dict:
    """
    Generates a greedy initial solution to warm-start the solver.
    Returns dict of (empresa, semana, taller) -> 1 for hinted assignments.
    """
    hints = {}

    # Sort companies by most constrained first (fewer possible slots)
    flexibility = {}
    for e in empresa_ids:
        flexibility[e] = sum(1 for s in SEMANAS for t in taller_ids if (e, s, t) in possible)

    companies_by_flexibility = sorted(empresa_ids, key=lambda e: flexibility[e])

    slot_taken = set()  # (semana, taller) already assigned
    empresa_week_used = set()  # (empresa, semana) to enforce max 1/week
    empresa_ef_count = {e: 0 for e in empresa_ids}
    empresa_it_count = {e: 0 for e in empresa_ids}

    for e in companies_by_flexibility:
        ef_needed = int(empresas[e].get("talleresEF", 0) or 0)
        it_needed = int(empresas[e].get("talleresIT", 0) or 0)
        total_needed = int(empresas[e].get("totalAsignado", 0) or 0)

        # Empresas with >= 6 talleres (escuela propia) can have multiple per week
        max_per_week = 20 if total_needed >= 6 else 1

        # Collect available slots for this company
        available_ef = []
        available_it = []
        for s in SEMANAS:
            week_count = sum(1 for (ee, ss, _) in hints if ee == e and ss == s)
            if week_count >= max_per_week:
                continue
            for t in taller_ids:
                if (e, s, t) not in possible:
                    continue
                if (s, t) in slot_taken:
                    continue
                prog = taller_map[t]["programa"]
                if prog == "EF":
                    available_ef.append((s, t))
                else:
                    available_it.append((s, t))

        # Assign EF — spread across weeks
        available_ef.sort(key=lambda x: x[0])
        assigned_ef = 0
        for s, t in available_ef:
            if assigned_ef >= ef_needed:
                break
            week_count = sum(1 for (ee, ss, _) in hints if ee == e and ss == s)
            if week_count >= max_per_week:
                continue
            if (s, t) not in slot_taken:
                hints[(e, s, t)] = 1
                slot_taken.add((s, t))
                assigned_ef += 1

        # Assign IT — spread across weeks
        available_it.sort(key=lambda x: x[0])
        assigned_it = 0
        for s, t in available_it:
            if assigned_it >= it_needed:
                break
            week_count = sum(1 for (ee, ss, _) in hints if ee == e and ss == s)
            if week_count >= max_per_week:
                continue
            if (s, t) not in slot_taken:
                hints[(e, s, t)] = 1
                slot_taken.add((s, t))
                assigned_it += 1

    return hints


def _ejecutar_solver(
    frecuencias: list[dict],
    restricciones: list[dict],
    talleres: list[dict],
    talleres_por_semana: dict[int, list[dict]],  # NEW: per-week taller lists
    disponibilidad_map: dict[int, list[str]],
    semanas_excluidas: set[int],
    dias_excluidos: set[tuple[int, str]],
    params: CalendarioInput,
) -> dict:
    """
    Asigna empresas a slots fijos semanales.
    OPTIMIZADO V11: Pre-filtering + hints + solution callback.

    Variables: assign[empresa_id, semana, taller_id] ∈ {0,1}
    Solo se crean variables para combinaciones POSIBLES.
    """
    from ortools.sat.python import cp_model
    import time
    import math

    model = cp_model.CpModel()
    start_time = time.time()
    warnings: list[str] = []

    # ─── DEBUG: Constraint counters ──────────────────────────
    debug_stats = {
        "empresas": 0,
        "semanas": 0,
        "talleres": 0,
        "talleres_ef": 0,
        "talleres_it": 0,
        "decision_vars_naive": 0,
        "decision_vars_actual": 0,
        "vars_filtered_out": 0,
        "total_frecuencias": 0,
        "total_slots_available": 0,
        "slots_blocked_festivos": 0,
        "H1_constraints": 0,
        "H2_constraints": 0,
        "H3_constraints": 0,
        "H4_filtered": 0,
        "H5_filtered": 0,
        "H6_constraints": 0,
        "H7_filtered": 0,
        "H8_filtered": 0,
        "S1_penalties": 0,
        "S2_penalties": 0,
        "S3_penalties": 0,
        "S4_penalties": 0,
        "S5_penalties": 0,
        "hints_generated": 0,
    }

    SEMANAS = [s for s in range(1, params.semanas + 1) if s not in semanas_excluidas]
    if semanas_excluidas:
        warnings.append(
            f"Solver omite semanas {sorted(semanas_excluidas)} — excluidas del trimestre"
        )

    empresas = {f["empresaId"]: f for f in frecuencias}
    # FIX: Robust filter — handle string "0", None, or missing totalAsignado
    empresa_ids = [e for e in empresas if int(empresas[e].get("totalAsignado", 0) or 0) > 0]

    talleres_ef = [t for t in talleres if t["programa"] == "EF"]
    talleres_it = [t for t in talleres if t["programa"] == "IT"]
    taller_ids_ef = [t["id"] for t in talleres_ef]
    taller_ids_it = [t["id"] for t in talleres_it]
    taller_ids = [t["id"] for t in talleres]
    taller_map = {t["id"]: t for t in talleres}

    # ─── DEBUG: Log initial dimensions ───────────────────────
    debug_stats["empresas"] = len(empresa_ids)
    debug_stats["semanas"] = len(SEMANAS)
    debug_stats["talleres"] = len(taller_ids)
    debug_stats["talleres_ef"] = len(taller_ids_ef)
    debug_stats["talleres_it"] = len(taller_ids_it)
    debug_stats["decision_vars_naive"] = len(empresa_ids) * len(SEMANAS) * len(taller_ids)
    debug_stats["total_frecuencias"] = sum(int(empresas[e].get("totalAsignado", 0) or 0) for e in empresa_ids)
    debug_stats["total_slots_available"] = len(SEMANAS) * len(taller_ids)

    print(f"\n{'='*60}")
    print(f"SOLVER DEBUG — {params.trimestre} [OPTIMIZADO V11]")
    print(f"{'='*60}")
    print(f"Empresas con talleres: {debug_stats['empresas']}")
    print(f"Semanas activas: {debug_stats['semanas']} ({SEMANAS})")
    print(f"Talleres: {debug_stats['talleres']} (EF={debug_stats['talleres_ef']}, IT={debug_stats['talleres_it']})")
    print(f"Naive decision variables: {debug_stats['decision_vars_naive']:,}")
    print(f"Total frecuencias a asignar: {debug_stats['total_frecuencias']}")
    print(f"Total slots disponibles (sin festivos): {debug_stats['total_slots_available']}")
    print(f"Días excluidos (festivos): {len(dias_excluidos)} → {sorted(dias_excluidos)[:10]}{'...' if len(dias_excluidos) > 10 else ''}")

    # Check feasibility early
    if debug_stats["total_frecuencias"] > debug_stats["total_slots_available"]:
        warnings.append(
            f"⚠ INFEASIBLE: frecuencias ({debug_stats['total_frecuencias']}) > slots ({debug_stats['total_slots_available']})"
        )
        print(f"❌ EARLY INFEASIBILITY: {debug_stats['total_frecuencias']} frecuencias > {debug_stats['total_slots_available']} slots")
        return {
            "status": "INFEASIBLE",
            "tiempo_segundos": 0,
            "total_slots": 0,
            "total_ef": 0,
            "total_it": 0,
            "slots": [],
            "inviolables_pct": 0,
            "preferentes_pct": 0,
            "warnings": warnings + [f"Total frecuencias ({debug_stats['total_frecuencias']}) excede slots disponibles ({debug_stats['total_slots_available']})"],
            "debug_stats": debug_stats,
        }

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
        extras_list = [f"{empresas[e]['nombre']}={v}" for e, v in max_extras_map.items()]
        warnings.append(f"Límite de extras: {', '.join(extras_list)}")

    # ═══════════════════════════════════════════════════════════
    # OPTIMIZATION 1: Pre-filter impossible assignments
    # This handles H4, H5, H7, H8 implicitly by NOT creating variables
    # ═══════════════════════════════════════════════════════════

    print(f"\n{'─'*40}")
    print("PRE-FILTERING impossible assignments...")

    # Build a map of available talleres per week (by taller ID)
    talleres_disponibles_semana: dict[int, set[int]] = {}
    taller_map_semana: dict[int, dict[int, dict]] = {}  # semana -> taller_id -> taller_info
    for s in SEMANAS:
        if s in talleres_por_semana:
            talleres_disponibles_semana[s] = {t["id"] for t in talleres_por_semana[s]}
            taller_map_semana[s] = {t["id"]: t for t in talleres_por_semana[s]}
        else:
            # Fallback to base talleres if no per-week config
            talleres_disponibles_semana[s] = set(taller_ids)
            taller_map_semana[s] = taller_map

    possible: set[tuple[int, int, int]] = set()

    for e in empresa_ids:
        dias_ok = set(dias_disponibles[e])
        allowed_talleres = solo_taller_ids.get(e)  # None means all allowed
        is_nueva = empresas[e].get("esNueva", False)
        ef_needed = int(empresas[e].get("talleresEF", 0) or 0)
        it_needed = int(empresas[e].get("talleresIT", 0) or 0)

        for s in SEMANAS:
            # H7: New companies not in weeks 1-4
            if is_nueva and s <= 4:
                debug_stats["H7_filtered"] += len(talleres_disponibles_semana.get(s, taller_ids))
                continue

            # Get talleres available THIS WEEK (from annual calendar)
            talleres_esta_semana = talleres_disponibles_semana.get(s, set(taller_ids))
            taller_info_semana = taller_map_semana.get(s, taller_map)

            for t_id in talleres_esta_semana:
                taller = taller_info_semana.get(t_id, taller_map.get(t_id))
                if not taller:
                    continue

                # H4: Day availability - use the EFFECTIVE day for this week
                effective_day = taller.get("diaSemana") or taller.get("dia_semana")
                if effective_day not in dias_ok:
                    debug_stats["H4_filtered"] += 1
                    continue

                # H8: Festivos - use effective day (could be overridden in intensive weeks)
                if (s, effective_day) in dias_excluidos:
                    debug_stats["H8_filtered"] += 1
                    continue

                # H5: solo_taller
                if allowed_talleres is not None and t_id not in allowed_talleres:
                    debug_stats["H5_filtered"] += 1
                    continue

                # Program type match — no point creating var if empresa doesn't need this program
                if taller["programa"] == "EF" and ef_needed == 0:
                    continue
                if taller["programa"] == "IT" and it_needed == 0:
                    continue

                possible.add((e, s, t_id))

    debug_stats["decision_vars_actual"] = len(possible)
    debug_stats["vars_filtered_out"] = debug_stats["decision_vars_naive"] - debug_stats["decision_vars_actual"]

    print(f"Naive vars: {debug_stats['decision_vars_naive']:,}")
    print(f"Possible vars: {debug_stats['decision_vars_actual']:,}")
    print(f"Filtered out: {debug_stats['vars_filtered_out']:,} ({debug_stats['vars_filtered_out']*100//max(1,debug_stats['decision_vars_naive'])}%)")
    print(f"  H4 (day): {debug_stats['H4_filtered']:,}")
    print(f"  H5 (solo_taller): {debug_stats['H5_filtered']:,}")
    print(f"  H7 (new company): {debug_stats['H7_filtered']:,}")
    print(f"  H8 (festivo): {debug_stats['H8_filtered']:,}")
    print(f"{'─'*40}\n")

    # ─── Helper to get assign var or 0 for impossible ────────
    def get_assign(e: int, s: int, t_id: int):
        """Returns the variable if it exists, or 0 (constant) if impossible."""
        return assign.get((e, s, t_id), 0)

    # ─── Variables de decisión (SOLO para posibles) ──────────
    assign = {}
    for (e, s, t_id) in possible:
        assign[(e, s, t_id)] = model.new_bool_var(f"a_{e}_{s}_{t_id}")

    # ─── HARD CONSTRAINTS ────────────────────────────────────

    # H1. Cada slot (semana, taller) tiene A LO SUMO 1 empresa
    # Only sum over companies that can actually reach this slot
    # UPDATED: iterate over per-week talleres (not global taller_ids)
    for s in SEMANAS:
        talleres_esta_semana = talleres_disponibles_semana.get(s, set(taller_ids))
        taller_info_semana = taller_map_semana.get(s, taller_map)
        for t_id in talleres_esta_semana:
            taller = taller_info_semana.get(t_id, taller_map.get(t_id))
            if not taller:
                continue
            # Skip festivo slots - use effective day for this week
            effective_day = taller.get("diaSemana") or taller.get("dia_semana")
            if (s, effective_day) in dias_excluidos:
                continue
            vars_for_slot = [assign[(e, s, t_id)] for e in empresa_ids if (e, s, t_id) in possible]
            if vars_for_slot:
                model.add(sum(vars_for_slot) <= 1)
                debug_stats["H1_constraints"] += 1

    # H1b. Total asignaciones = suma de frecuencias (todo lo confirmado se coloca)
    total_frecuencias = sum(int(empresas[e].get("totalAsignado", 0) or 0) for e in empresa_ids)
    all_assign_vars = list(assign.values())
    model.add(sum(all_assign_vars) == total_frecuencias)
    print(f"H1: {debug_stats['H1_constraints']} slot constraints + H1b: total={total_frecuencias}")

    # H2. Frecuencia EF por empresa = talleresEF confirmado
    for e in empresa_ids:
        ef_requerido = int(empresas[e].get("talleresEF", 0) or 0)
        ef_vars = [assign[(e, s, t_id)] for s in SEMANAS for t_id in taller_ids_ef if (e, s, t_id) in possible]
        if ef_vars:
            model.add(sum(ef_vars) == ef_requerido)
            debug_stats["H2_constraints"] += 1
        elif ef_requerido > 0:
            # Infeasible: company needs EF but has no possible EF slots
            warnings.append(f"⚠ INFEASIBLE: {empresas[e]['nombre']} necesita {ef_requerido} EF pero no tiene slots posibles")
    print(f"H2: {debug_stats['H2_constraints']} EF frequency constraints")

    # H3. Frecuencia IT por empresa = talleresIT confirmado
    for e in empresa_ids:
        it_requerido = int(empresas[e].get("talleresIT", 0) or 0)
        it_vars = [assign[(e, s, t_id)] for s in SEMANAS for t_id in taller_ids_it if (e, s, t_id) in possible]
        if it_vars:
            model.add(sum(it_vars) == it_requerido)
            debug_stats["H3_constraints"] += 1
        elif it_requerido > 0:
            # Infeasible: company needs IT but has no possible IT slots
            warnings.append(f"⚠ INFEASIBLE: {empresas[e]['nombre']} necesita {it_requerido} IT pero no tiene slots posibles")
    print(f"H3: {debug_stats['H3_constraints']} IT frequency constraints")

    # H4, H5, H7, H8 are handled implicitly by pre-filtering (no variables created)
    print(f"H4, H5, H7, H8: handled by pre-filtering ({debug_stats['vars_filtered_out']:,} vars eliminated)")

    # H6. Max 1 taller por empresa por semana (planificación base)
    # Excepción: empresas con escuela propia pueden tener hasta 20 (toda la semana)
    for e in empresa_ids:
        max_per_week = 1  # Default: regla inviolable
        total_empresa = int(empresas[e].get("totalAsignado", 0) or 0)
        if total_empresa >= 6:
            max_per_week = 20  # Sin límite práctico (escuela propia)
        for s in SEMANAS:
            week_vars = [assign[(e, s, t_id)] for t_id in taller_ids if (e, s, t_id) in possible]
            if week_vars:
                model.add(sum(week_vars) <= max_per_week)
                debug_stats["H6_constraints"] += 1
    print(f"H6: {debug_stats['H6_constraints']} max-per-week constraints")

    # Log empresas nuevas warning (even though H7 is handled by pre-filter)
    empresas_nuevas = [e for e in empresa_ids if empresas[e].get("esNueva", False)]
    if empresas_nuevas:
        nombres_nuevas = [empresas[e]["nombre"] for e in empresas_nuevas]
        warnings.append(
            f"Empresas nuevas ({len(empresas_nuevas)}): {', '.join(nombres_nuevas)} "
            f"→ programadas a partir de semana 5"
        )

    # ─── SOFT CONSTRAINTS ────────────────────────────────────

    penalties = []

    # S1. Equilibrio mensual: penalizar desequilibrio entre meses
    MONTH_WEEKS = {
        1: [s for s in SEMANAS if 1 <= s <= 4],
        2: [s for s in SEMANAS if 5 <= s <= 9],
        3: [s for s in SEMANAS if 10 <= s <= 13],
    }

    for e in empresa_ids:
        total = int(empresas[e].get("totalAsignado", 0) or 0)
        if total == 0:
            continue
        ideal_per_month = total / 3.0

        for month_num, month_weeks in MONTH_WEEKS.items():
            if not month_weeks:
                continue
            month_vars = [assign[(e, s, t_id)] for s in month_weeks for t_id in taller_ids if (e, s, t_id) in possible]
            if not month_vars:
                continue
            month_total = sum(month_vars)
            excess = model.new_int_var(0, 20, f"excess_{e}_{month_num}")
            model.add(excess >= month_total - int(ideal_per_month + 1))
            penalties.append(excess * params.peso_equilibrio)
            debug_stats["S1_penalties"] += 1
    print(f"S1: {debug_stats['S1_penalties']} equilibrio penalties")

    # S2. Penalizar semanas consecutivas para misma empresa
    # OPTIMIZED: Linear formulation, only for companies with >= 2 talleres
    s2_empresas = 0
    for e in empresa_ids:
        total = int(empresas[e].get("totalAsignado", 0) or 0)
        if total < 2:  # Can't have consecutives with 0-1 talleres
            continue
        s2_empresas += 1
        for i in range(len(SEMANAS) - 1):
            s1 = SEMANAS[i]
            s2 = SEMANAS[i + 1]
            # Solo penalizar si realmente son consecutivas (diferencia = 1)
            if s2 - s1 != 1:
                continue

            # Sum of assignments in both weeks (using only possible vars)
            week1_vars = [assign[(e, s1, t)] for t in taller_ids if (e, s1, t) in possible]
            week2_vars = [assign[(e, s2, t)] for t in taller_ids if (e, s2, t) in possible]

            if not week1_vars or not week2_vars:
                continue  # Can't have both weeks if one has no possible slots

            sum_both = sum(week1_vars) + sum(week2_vars)
            consec_penalty = model.new_int_var(0, 2, f"consec_{e}_{s1}")
            model.add(consec_penalty >= sum_both - 1)
            penalties.append(consec_penalty * params.peso_no_consecutivas)
            debug_stats["S2_penalties"] += 1
    print(f"S2: {debug_stats['S2_penalties']} consecutivas penalties ({s2_empresas} empresas with >=2 talleres)")

    # S3. Turno preferido — use assign vars directly (no new penalty vars needed)
    # OPTIMIZED: Only iterate over possible combinations
    for e in empresa_ids:
        turno_pref = empresas[e].get("turnoPreferido")
        if not turno_pref:
            continue
        for s in SEMANAS:
            for t_id in taller_ids:
                if (e, s, t_id) not in possible:
                    continue  # Skip impossible assignments
                taller = taller_map[t_id]
                if taller.get("turno") and taller["turno"] != turno_pref:
                    penalties.append(assign[(e, s, t_id)] * params.peso_turno_preferido)
                    debug_stats["S3_penalties"] += 1
    print(f"S3: {debug_stats['S3_penalties']} turno preferido penalties [OPTIMIZED]")

    # S4. Intercalar EF/IT en meses distintos
    # OPTIMIZED: Only for companies with EF >= 2 AND IT >= 1
    empresas_mixtas = 0
    for e in empresa_ids:
        ef_total = int(empresas[e].get("talleresEF", 0) or 0)
        it_total = int(empresas[e].get("talleresIT", 0) or 0)

        # Solo aplicar si empresa tiene AMBOS EF >= 2 e IT >= 1
        if ef_total < 2 or it_total < 1:
            continue

        empresas_mixtas += 1

        for month_num, month_weeks in MONTH_WEEKS.items():
            if not month_weeks:
                continue

            # Contar asignaciones EF en este mes (only possible vars)
            ef_vars = [assign[(e, s, t_id)] for s in month_weeks for t_id in taller_ids_ef if (e, s, t_id) in possible]
            # Contar asignaciones IT en este mes (only possible vars)
            it_vars = [assign[(e, s, t_id)] for s in month_weeks for t_id in taller_ids_it if (e, s, t_id) in possible]

            max_ef_per_month = math.ceil(ef_total / 3)
            max_it_per_month = math.ceil(it_total / 3)

            if ef_vars:
                ef_excess = model.new_int_var(0, 20, f"ef_excess_{e}_{month_num}")
                model.add(ef_excess >= sum(ef_vars) - max_ef_per_month)
                penalties.append(ef_excess * params.peso_intercalar_ef_it)
                debug_stats["S4_penalties"] += 1

            if it_vars:
                it_excess = model.new_int_var(0, 20, f"it_excess_{e}_{month_num}")
                model.add(it_excess >= sum(it_vars) - max_it_per_month)
                penalties.append(it_excess * params.peso_intercalar_ef_it)
                debug_stats["S4_penalties"] += 1

    print(f"S4: {debug_stats['S4_penalties']} intercalar EF/IT penalties ({empresas_mixtas} empresas mixtas)")
    if empresas_mixtas > 0:
        warnings.append(f"S4 Intercalar EF/IT: {empresas_mixtas} empresas con ambos programas")

    # S5. Diversidad de talleres — penalizar repetición del mismo taller
    # OPTIMIZED: Only check REACHABLE talleres for each company
    empresas_diversidad = 0
    for e in empresa_ids:
        total = int(empresas[e].get("totalAsignado", 0) or 0)
        # Skip empresas with < 3 talleres (1-2 talleres can't meaningfully repeat anyway)
        if total < 3:
            continue
        # Excluir escuelas propias / alta frecuencia (>=6 talleres) — repiten por diseño
        if total >= 6:
            continue

        empresas_diversidad += 1
        ef = int(empresas[e].get("talleresEF", 0) or 0)
        it = int(empresas[e].get("talleresIT", 0) or 0)

        # Only penalize EF repetition if empresa has 2+ EF talleres
        if ef >= 2:
            # Only check EF talleres this company can actually reach
            reachable_ef = [t_id for t_id in taller_ids_ef
                          if any((e, s, t_id) in possible for s in SEMANAS)]
            for t_id in reachable_ef:
                times_vars = [assign[(e, s, t_id)] for s in SEMANAS if (e, s, t_id) in possible]
                if len(times_vars) <= 1:
                    continue  # Can only visit once, no repetition possible
                times_at_taller = sum(times_vars)
                repeat = model.new_int_var(0, 13, f"rep_{e}_{t_id}")
                model.add(repeat >= times_at_taller - 1)
                penalties.append(repeat * params.peso_diversidad_talleres)
                debug_stats["S5_penalties"] += 1

        # Only penalize IT repetition if empresa has 2+ IT talleres
        if it >= 2:
            # Only check IT talleres this company can actually reach
            reachable_it = [t_id for t_id in taller_ids_it
                          if any((e, s, t_id) in possible for s in SEMANAS)]
            for t_id in reachable_it:
                times_vars = [assign[(e, s, t_id)] for s in SEMANAS if (e, s, t_id) in possible]
                if len(times_vars) <= 1:
                    continue  # Can only visit once, no repetition possible
                times_at_taller = sum(times_vars)
                repeat = model.new_int_var(0, 13, f"rep_{e}_{t_id}")
                model.add(repeat >= times_at_taller - 1)
                penalties.append(repeat * params.peso_diversidad_talleres)
                debug_stats["S5_penalties"] += 1

    print(f"S5: {debug_stats['S5_penalties']} diversidad penalties ({empresas_diversidad} empresas 3-5 talleres) [OPTIMIZED]")
    if empresas_diversidad > 0:
        warnings.append(f"S5 Diversidad talleres: {empresas_diversidad} empresas (excluye escuelas propias >=6 talleres)")

    # ═══════════════════════════════════════════════════════════
    # OPTIMIZATION 2: Generate solver hints (warm-start)
    # ═══════════════════════════════════════════════════════════

    print(f"\n{'─'*40}")
    print("Generating solver hints...")

    hints = _generate_hints(
        possible=possible,
        empresa_ids=empresa_ids,
        empresas=empresas,
        SEMANAS=SEMANAS,
        taller_ids=taller_ids,
        taller_map=taller_map,
        taller_ids_ef=taller_ids_ef,
        taller_ids_it=taller_ids_it,
    )
    debug_stats["hints_generated"] = len(hints)

    # Apply hints to model
    for (e, s, t_id), val in hints.items():
        if (e, s, t_id) in assign:
            model.add_hint(assign[(e, s, t_id)], val)

    # Also hint 0 for non-hinted possible vars (helps solver)
    for (e, s, t_id) in possible:
        if (e, s, t_id) not in hints:
            model.add_hint(assign[(e, s, t_id)], 0)

    print(f"Hints generated: {debug_stats['hints_generated']} assignments")
    print(f"{'─'*40}\n")

    # ─── Summary before solve ────────────────────────────────
    total_penalties = len(penalties)
    build_time = time.time() - start_time
    print(f"\n{'─'*40}")
    print(f"MODEL SUMMARY")
    print(f"  Variables: {debug_stats['decision_vars_actual']:,} (was {debug_stats['decision_vars_naive']:,})")
    print(f"  Penalties: {total_penalties}")
    print(f"  Hints: {debug_stats['hints_generated']}")
    print(f"  Build time: {build_time:.2f}s")
    print(f"{'─'*40}\n")

    if penalties:
        model.minimize(sum(penalties))

    # ═══════════════════════════════════════════════════════════
    # OPTIMIZATION 3: Solve with solution callback + gap limit
    # ═══════════════════════════════════════════════════════════

    solver = cp_model.CpSolver()
    # Timeout y workers configurables por env var (Render suele ir con menos CPU que local)
    solver_workers = int(os.getenv("SOLVER_WORKERS", "8"))
    solver_timeout = int(os.getenv("SOLVER_TIMEOUT", str(params.timeout_seconds)))
    solver.parameters.max_time_in_seconds = solver_timeout
    solver.parameters.num_workers = solver_workers

    # Accept solutions within 5% of optimal (don't insist on OPTIMAL)
    solver.parameters.relative_gap_limit = 0.05

    # Solution callback for logging progress
    class SolutionCallback(cp_model.CpSolverSolutionCallback):
        def __init__(self):
            super().__init__()
            self.solution_count = 0
            self.best_objective = float('inf')

        def on_solution_callback(self):
            self.solution_count += 1
            obj = self.objective_value
            if obj < self.best_objective:
                self.best_objective = obj
            elapsed = self.wall_time
            print(f"  Solution #{self.solution_count}: objective={obj:.0f}, elapsed={elapsed:.1f}s")

    print(f"Starting solver with timeout={solver_timeout}s, workers={solver_workers}, gap_limit=5%...")
    callback = SolutionCallback()
    status_code = solver.solve(model, callback)
    elapsed = time.time() - start_time

    status_map = {
        cp_model.OPTIMAL: "OPTIMAL",
        cp_model.FEASIBLE: "FEASIBLE",
        cp_model.INFEASIBLE: "INFEASIBLE",
        cp_model.MODEL_INVALID: "INFEASIBLE",
        cp_model.UNKNOWN: "TIMEOUT",
    }
    status = status_map.get(status_code, "TIMEOUT")
    print(f"Solver finished: status={status}, solutions={callback.solution_count}, elapsed={elapsed:.2f}s")

    if status in ("INFEASIBLE", "TIMEOUT"):
        print(f"❌ SOLVER FAILED: {status}")
        print(f"Debug stats: {debug_stats}")
        return {
            "status": status,
            "tiempo_segundos": round(elapsed, 2),
            "total_slots": 0,
            "total_ef": 0,
            "total_it": 0,
            "slots": [],
            "inviolables_pct": 0,
            "preferentes_pct": 0,
            "warnings": [f"Solver terminó con status: {status}. Revisar restricciones."] + warnings,
            "debug_stats": debug_stats,
        }

    # ─── Extraer solución ────────────────────────────────────
    # UPDATED: iterate over per-week talleres (not global taller_ids)
    # This respects intensive weeks where some talleres are OFF or have different day/horario

    slots_raw: list[dict] = []
    vacios = 0
    festivos_skipped = 0
    total_slots_posibles = 0

    for s in SEMANAS:
        talleres_esta_semana = talleres_disponibles_semana.get(s, set(taller_ids))
        taller_info_semana = taller_map_semana.get(s, taller_map)

        for t_id in talleres_esta_semana:
            taller = taller_info_semana.get(t_id, taller_map.get(t_id))
            if not taller:
                continue

            # Use effective day/horario for this specific week
            effective_day = taller.get("diaSemana") or taller.get("dia_semana")
            effective_horario = taller.get("horario", "")
            effective_turno = taller.get("turno", "")

            # Skip festivo slots — they don't exist in the calendar
            if (s, effective_day) in dias_excluidos:
                festivos_skipped += 1
                continue

            total_slots_posibles += 1
            assigned = False
            for e in empresa_ids:
                # Check dict membership directly — don't compare BoolVar to int
                if (e, s, t_id) in assign and solver.value(assign[(e, s, t_id)]) == 1:
                    slots_raw.append({
                        "semana": s,
                        "dia": effective_day,
                        "horario": effective_horario,
                        "turno": effective_turno,
                        "empresa_id": e,
                        "empresa_nombre": empresas[e]["nombre"],
                        "programa": taller["programa"],
                        "taller_id": t_id,
                        "taller_nombre": taller["nombre"],
                    })
                    assigned = True
                    break  # Only one company per slot (H1 constraint)

            if not assigned:
                vacios += 1
                # Incluir slot vacío para que el frontend lo muestre
                slots_raw.append({
                    "semana": s,
                    "dia": effective_day,
                    "horario": effective_horario,
                    "turno": effective_turno,
                    "empresa_id": 0,
                    "empresa_nombre": "— Vacante —",
                    "programa": taller["programa"],
                    "taller_id": t_id,
                    "taller_nombre": taller["nombre"],
                })
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

    print(f"\n{'='*60}")
    print(f"SOLVER SUCCESS — {status}")
    print(f"  Time: {elapsed:.2f}s")
    print(f"  Solutions found: {callback.solution_count}")
    print(f"  Objective: {total_penalty:.0f}")
    print(f"  Slots: {len(slots_raw)} ({vacios} vacantes)")
    print(f"{'='*60}\n")

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
        "debug_stats": debug_stats,
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