"""
Calendario Anual de Talleres — Configuracion semanal por ano.

Reemplaza el sistema TallerTrimestre con un calendario anual donde cada
semana del ano puede ser "normal" (20 talleres base) o "intensiva"
(EF tarde desactivado, IT tarde movido a miercoles manana).

Tablas: semana_config, semana_extra_slot
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel
from datetime import date, timedelta

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class SemanaConfigOut(BaseModel):
    id: int | None
    anio: int
    semana: int
    tipo: str
    notas: str | None
    extras_count: int


class CalendarioAnualResponse(BaseModel):
    anio: int
    semanas: list[SemanaConfigOut]
    resumen: dict


class SemanaUpdateInput(BaseModel):
    tipo: str  # "normal" | "intensiva"
    notas: str | None = None


class SemanaBatchUpdateItem(BaseModel):
    """Individual week update (used by frontend)."""
    semana: int
    tipo: str  # "normal" | "intensiva"
    notas: str | None = None


class SemanaBatchUpdateInput(BaseModel):
    updates: list[SemanaBatchUpdateItem]


class TallerEfectivoOut(BaseModel):
    taller_id: int
    nombre: str
    programa: str
    dia_semana: str | None
    horario: str | None
    turno: str | None
    es_extra: bool
    extra_id: int | None = None
    override: bool = False  # True if day/horario differs from base


class SemanaDetalleResponse(BaseModel):
    semana: int
    tipo: str
    notas: str | None
    talleres: list[TallerEfectivoOut]
    total_slots: int
    total_ef: int
    total_it: int
    resumen: str


class ExtraSlotInput(BaseModel):
    taller_id: int
    dia_semana: str | None = None
    horario: str | None = None
    notas: str | None = None


class ExtraSlotOut(BaseModel):
    id: int
    taller_id: int
    taller_nombre: str
    taller_programa: str
    dia_semana: str | None
    horario: str | None
    notas: str | None


class TrimestreResumenResponse(BaseModel):
    trimestre: str
    anio: int
    semanas_normales: int
    semanas_intensivas: int
    total_slots_ef: int
    total_slots_it: int
    total_slots: int
    semanas_detalle: list[dict]


# ── Helpers ─────────────────────────────────────────────────

def get_iso_week(d: date) -> int:
    """Get ISO week number for a date."""
    return d.isocalendar()[1]


def get_month_for_week(anio: int, semana: int) -> int:
    """Get the month that contains the Thursday of this ISO week."""
    jan4 = date(anio, 1, 4)  # Jan 4 is always in week 1
    days_since_jan4 = (semana - 1) * 7
    week_thursday = jan4 + timedelta(days=days_since_jan4 - jan4.weekday() + 3)
    return week_thursday.month


def get_week_date_range(anio: int, semana: int) -> tuple[date, date]:
    """Get the Monday-Sunday date range for an ISO week."""
    jan4 = date(anio, 1, 4)
    days_since_jan4 = (semana - 1) * 7
    monday = jan4 + timedelta(days=days_since_jan4 - jan4.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def trimestre_to_weeks(trimestre: str) -> tuple[int, int, int]:
    """
    Convert trimestre string to (anio, week_start, week_end).
    Q1=weeks 1-13, Q2=14-26, Q3=27-39, Q4=40-52
    """
    anio = int(trimestre.split("-")[0])
    quarter_num = int(trimestre.split("Q")[1])
    week_start = (quarter_num - 1) * 13 + 1
    week_end = week_start + 12
    return anio, week_start, week_end


async def cargar_talleres_semana(
    db: AsyncSession,
    anio: int,
    semana: int,
) -> list[dict]:
    """
    Returns the effective list of talleres for a specific week,
    applying tipo rules and adding extra slots.

    This is the core function used by the solver and frequency calculator.
    """
    # 1. Get week config
    result = await db.execute(
        text("""
            SELECT id, tipo, notas FROM semana_config
            WHERE anio = :anio AND semana = :semana
        """),
        {"anio": anio, "semana": semana},
    )
    config_row = result.mappings().first()
    config_id = config_row["id"] if config_row else None
    tipo = config_row["tipo"] if config_row else "normal"

    # 2. Load all active base talleres
    result = await db.execute(
        text("""
            SELECT id, nombre, programa, "diaSemana", horario, turno
            FROM taller
            WHERE activo = true
            ORDER BY id
        """)
    )
    base_talleres = [dict(r) for r in result.mappings().all()]

    # 3. Apply tipo rules
    talleres_efectivos = []
    for t in base_talleres:
        is_tarde = (t["turno"] == "T") or (t["horario"] and t["horario"].startswith("15:"))

        if tipo == "intensiva":
            if t["programa"] == "EF" and is_tarde:
                continue  # Skip EF afternoon in intensive weeks
            elif t["programa"] == "IT" and is_tarde:
                # Move IT afternoon to Wednesday morning
                talleres_efectivos.append({
                    "taller_id": t["id"],
                    "nombre": t["nombre"],
                    "programa": t["programa"],
                    "dia_semana": "X",           # Wednesday
                    "horario": "09:30-11:30",    # Morning
                    "turno": "M",
                    "es_extra": False,
                    "extra_id": None,
                    "override": True,
                })
                continue

        # Normal week or taller not affected
        talleres_efectivos.append({
            "taller_id": t["id"],
            "nombre": t["nombre"],
            "programa": t["programa"],
            "dia_semana": t["diaSemana"],
            "horario": t["horario"],
            "turno": t["turno"],
            "es_extra": False,
            "extra_id": None,
            "override": False,
        })

    # 4. Add extra slots
    if config_id:
        result = await db.execute(
            text("""
                SELECT ses.id, ses."tallerId", ses."diaSemana", ses.horario, ses.notas,
                       t.nombre, t.programa, t."diaSemana" AS taller_dia, t.horario AS taller_horario, t.turno
                FROM semana_extra_slot ses
                JOIN taller t ON t.id = ses."tallerId"
                WHERE ses."semanaConfigId" = :config_id
            """),
            {"config_id": config_id},
        )
        for row in result.mappings().all():
            r = dict(row)
            eff_dia = r["diaSemana"] or r["taller_dia"]
            eff_horario = r["horario"] or r["taller_horario"]
            talleres_efectivos.append({
                "taller_id": r["tallerId"],
                "nombre": r["nombre"],
                "programa": r["programa"],
                "dia_semana": eff_dia,
                "horario": eff_horario,
                "turno": r["turno"],
                "es_extra": True,
                "extra_id": r["id"],
                "override": (eff_dia != r["taller_dia"]) or (eff_horario != r["taller_horario"]),
            })

    return talleres_efectivos


# ── Endpoints ────────────────────────────────────────────────

@router.get("/{anio}", response_model=CalendarioAnualResponse)
async def obtener_calendario_anual(
    anio: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Returns all 52 weeks of a year with their tipo and extra slot count.
    Weeks without explicit config are treated as "normal".
    """
    # Get all configured weeks for this year
    result = await db.execute(
        text("""
            SELECT sc.id, sc.semana, sc.tipo, sc.notas,
                   COUNT(ses.id) AS extras_count
            FROM semana_config sc
            LEFT JOIN semana_extra_slot ses ON ses."semanaConfigId" = sc.id
            WHERE sc.anio = :anio
            GROUP BY sc.id, sc.semana, sc.tipo, sc.notas
            ORDER BY sc.semana
        """),
        {"anio": anio},
    )
    configured = {r["semana"]: dict(r) for r in result.mappings().all()}

    # Build all 52 weeks
    semanas = []
    normales = 0
    intensivas = 0
    con_extras = 0

    for sem in range(1, 53):
        if sem in configured:
            cfg = configured[sem]
            semanas.append(SemanaConfigOut(
                id=cfg["id"],
                anio=anio,
                semana=sem,
                tipo=cfg["tipo"],
                notas=cfg["notas"],
                extras_count=cfg["extras_count"],
            ))
            if cfg["tipo"] == "intensiva":
                intensivas += 1
            else:
                normales += 1
            if cfg["extras_count"] > 0:
                con_extras += 1
        else:
            semanas.append(SemanaConfigOut(
                id=None,
                anio=anio,
                semana=sem,
                tipo="normal",
                notas=None,
                extras_count=0,
            ))
            normales += 1

    return CalendarioAnualResponse(
        anio=anio,
        semanas=semanas,
        resumen={
            "normales": normales,
            "intensivas": intensivas,
            "con_extras": con_extras,
        },
    )


@router.post("/{anio}/init")
async def inicializar_calendario_anual(
    anio: int,
    template: str | None = None,  # "estandar_madrid" | None
    db: AsyncSession = Depends(get_db),
):
    """
    Initialize all 52 weeks for a year.

    Templates:
    - "estandar_madrid": Weeks 27-35 (Jul-Aug) and 51-52 (Christmas) as "intensiva"
    - None: All weeks as "normal"

    If config already exists, it will be REPLACED when template is provided.
    """
    # Check if config exists
    existing = await db.execute(
        text("SELECT COUNT(*) FROM semana_config WHERE anio = :anio"),
        {"anio": anio},
    )
    count = existing.scalar()

    if template:
        # Delete existing config
        await db.execute(
            text("DELETE FROM semana_config WHERE anio = :anio"),
            {"anio": anio},
        )
    elif count and count > 0:
        return {
            "ok": True,
            "message": f"Ya existe configuracion para {anio} ({count} semanas)",
            "created": 0,
            "template_applied": None,
        }

    # Define intensive weeks based on template
    intensive_weeks = set()
    if template == "estandar_madrid":
        # July-August (weeks 27-35) + Christmas (weeks 51-52)
        intensive_weeks = set(range(27, 36)) | {51, 52}

    # Create 52 weeks
    for sem in range(1, 53):
        tipo = "intensiva" if sem in intensive_weeks else "normal"
        notas = None
        if sem in range(27, 36):
            notas = "Julio-Agosto - Jornada intensiva"
        elif sem in {51, 52}:
            notas = "Navidad - Jornada intensiva"

        await db.execute(
            text("""
                INSERT INTO semana_config (anio, semana, tipo, notas, "createdAt", "updatedAt")
                VALUES (:anio, :semana, :tipo, :notas, NOW(), NOW())
                ON CONFLICT (anio, semana)
                DO UPDATE SET tipo = :tipo, notas = :notas, "updatedAt" = NOW()
            """),
            {"anio": anio, "semana": sem, "tipo": tipo, "notas": notas},
        )

    await db.commit()

    template_desc = ""
    if template == "estandar_madrid":
        template_desc = " - semanas 27-35 y 51-52 como intensivas"

    return {
        "ok": True,
        "message": f"Calendario {anio} inicializado (52 semanas){template_desc}",
        "created": 52,
        "template_applied": template,
    }


@router.put("/{anio}/semana/{semana}", response_model=SemanaConfigOut)
async def actualizar_semana(
    anio: int,
    semana: int,
    data: SemanaUpdateInput,
    db: AsyncSession = Depends(get_db),
):
    """Update a single week's configuration."""
    if semana < 1 or semana > 52:
        raise HTTPException(status_code=400, detail="Semana debe ser entre 1 y 52")
    if data.tipo not in ("normal", "intensiva"):
        raise HTTPException(status_code=400, detail="Tipo debe ser 'normal' o 'intensiva'")

    result = await db.execute(
        text("""
            INSERT INTO semana_config (anio, semana, tipo, notas, "createdAt", "updatedAt")
            VALUES (:anio, :semana, :tipo, :notas, NOW(), NOW())
            ON CONFLICT (anio, semana)
            DO UPDATE SET tipo = :tipo, notas = :notas, "updatedAt" = NOW()
            RETURNING id, semana, tipo, notas
        """),
        {"anio": anio, "semana": semana, "tipo": data.tipo, "notas": data.notas},
    )
    await db.commit()
    row = result.mappings().first()

    # Count extras for this week
    extras_result = await db.execute(
        text("""
            SELECT COUNT(*) as cnt FROM semana_extra_slot ses
            JOIN semana_config sc ON sc.id = ses."semanaConfigId"
            WHERE sc.anio = :anio AND sc.semana = :semana
        """),
        {"anio": anio, "semana": semana},
    )
    extras_count = extras_result.scalar() or 0

    return SemanaConfigOut(
        id=row["id"],
        anio=anio,
        semana=row["semana"],
        tipo=row["tipo"],
        notas=row["notas"],
        extras_count=extras_count,
    )


@router.put("/{anio}/batch")
async def actualizar_semanas_batch(
    anio: int,
    data: SemanaBatchUpdateInput,
    db: AsyncSession = Depends(get_db),
):
    """Update multiple individual weeks at once."""
    updated = 0
    errors = []

    for item in data.updates:
        if item.semana < 1 or item.semana > 52:
            errors.append(f"Semana invalida: {item.semana}")
            continue
        if item.tipo not in ("normal", "intensiva"):
            errors.append(f"Tipo invalido: {item.tipo}")
            continue

        await db.execute(
            text("""
                INSERT INTO semana_config (anio, semana, tipo, notas, "createdAt", "updatedAt")
                VALUES (:anio, :semana, :tipo, :notas, NOW(), NOW())
                ON CONFLICT (anio, semana)
                DO UPDATE SET tipo = :tipo, notas = :notas, "updatedAt" = NOW()
            """),
            {"anio": anio, "semana": item.semana, "tipo": item.tipo, "notas": item.notas},
        )
        updated += 1

    await db.commit()

    return {"updated": updated, "message": f"{updated} semanas actualizadas"}


@router.get("/{anio}/semana/{semana}/detalle", response_model=SemanaDetalleResponse)
async def obtener_detalle_semana(
    anio: int,
    semana: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Returns the EFFECTIVE talleres for a specific week.
    Shows exactly what the solver would use.
    """
    if semana < 1 or semana > 52:
        raise HTTPException(status_code=400, detail="Semana debe ser entre 1 y 52")

    # Get week config
    result = await db.execute(
        text("SELECT tipo, notas FROM semana_config WHERE anio = :anio AND semana = :semana"),
        {"anio": anio, "semana": semana},
    )
    config_row = result.mappings().first()
    tipo = config_row["tipo"] if config_row else "normal"
    notas = config_row["notas"] if config_row else None

    # Load effective talleres
    talleres = await cargar_talleres_semana(db, anio, semana)

    # Count by programa
    total_ef = sum(1 for t in talleres if t["programa"] == "EF")
    total_it = sum(1 for t in talleres if t["programa"] == "IT")
    total = len(talleres)

    tipo_label = "intensiva" if tipo == "intensiva" else "normal"
    resumen = f"{total_ef} EF + {total_it} IT = {total} slots ({tipo_label})"

    return SemanaDetalleResponse(
        semana=semana,
        tipo=tipo,
        notas=notas,
        talleres=[TallerEfectivoOut(**t) for t in talleres],
        total_slots=total,
        total_ef=total_ef,
        total_it=total_it,
        resumen=resumen,
    )


@router.get("/{anio}/semana/{semana}/extras", response_model=list[ExtraSlotOut])
async def obtener_extras_semana(
    anio: int,
    semana: int,
    db: AsyncSession = Depends(get_db),
):
    """Get all extra slots for a specific week."""
    result = await db.execute(
        text("""
            SELECT ses.id, ses."tallerId" AS taller_id, ses."diaSemana" AS dia_semana,
                   ses.horario, ses.notas,
                   t.nombre AS taller_nombre, t.programa AS taller_programa
            FROM semana_extra_slot ses
            JOIN semana_config sc ON sc.id = ses."semanaConfigId"
            JOIN taller t ON t.id = ses."tallerId"
            WHERE sc.anio = :anio AND sc.semana = :semana
        """),
        {"anio": anio, "semana": semana},
    )
    return [ExtraSlotOut(**dict(r)) for r in result.mappings().all()]


@router.post("/{anio}/semana/{semana}/extras", response_model=ExtraSlotOut)
async def agregar_extra_slot(
    anio: int,
    semana: int,
    data: ExtraSlotInput,
    db: AsyncSession = Depends(get_db),
):
    """Add an extra taller slot to a specific week."""
    if semana < 1 or semana > 52:
        raise HTTPException(status_code=400, detail="Semana debe ser entre 1 y 52")

    # Validate taller exists
    taller_result = await db.execute(
        text("SELECT id, nombre, programa FROM taller WHERE id = :id AND activo = true"),
        {"id": data.taller_id},
    )
    taller = taller_result.mappings().first()
    if not taller:
        raise HTTPException(status_code=404, detail=f"Taller {data.taller_id} no encontrado o inactivo")

    # Ensure semana_config exists
    config_result = await db.execute(
        text("SELECT id FROM semana_config WHERE anio = :anio AND semana = :semana"),
        {"anio": anio, "semana": semana},
    )
    config = config_result.mappings().first()

    if not config:
        # Create the semana_config row first
        await db.execute(
            text("""
                INSERT INTO semana_config (anio, semana, tipo, "createdAt", "updatedAt")
                VALUES (:anio, :semana, 'normal', NOW(), NOW())
            """),
            {"anio": anio, "semana": semana},
        )
        config_result = await db.execute(
            text("SELECT id FROM semana_config WHERE anio = :anio AND semana = :semana"),
            {"anio": anio, "semana": semana},
        )
        config = config_result.mappings().first()

    # Insert extra slot
    result = await db.execute(
        text("""
            INSERT INTO semana_extra_slot ("semanaConfigId", "tallerId", "diaSemana", horario, notas, "createdAt")
            VALUES (:config_id, :taller_id, :dia_semana, :horario, :notas, NOW())
            RETURNING id
        """),
        {
            "config_id": config["id"],
            "taller_id": data.taller_id,
            "dia_semana": data.dia_semana,
            "horario": data.horario,
            "notas": data.notas,
        },
    )
    new_id = result.scalar()
    await db.commit()

    return ExtraSlotOut(
        id=new_id,
        taller_id=data.taller_id,
        taller_nombre=taller["nombre"],
        taller_programa=taller["programa"],
        dia_semana=data.dia_semana,
        horario=data.horario,
        notas=data.notas,
    )


@router.delete("/{anio}/semana/{semana}/extras/{extra_id}")
async def eliminar_extra_slot_full_path(
    anio: int,
    semana: int,
    extra_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Remove an extra slot (full path with anio/semana for verification)."""
    result = await db.execute(
        text("""
            DELETE FROM semana_extra_slot
            WHERE id = :extra_id
            AND "semanaConfigId" IN (
                SELECT id FROM semana_config WHERE anio = :anio AND semana = :semana
            )
            RETURNING id
        """),
        {"extra_id": extra_id, "anio": anio, "semana": semana},
    )
    deleted = result.scalar()
    await db.commit()

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Extra slot {extra_id} no encontrado")

    return {"ok": True, "deleted_id": extra_id}


@router.delete("/extras/{extra_id}")
async def eliminar_extra_slot_by_id(
    extra_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Remove an extra slot by ID only (simpler endpoint for frontend)."""
    result = await db.execute(
        text("DELETE FROM semana_extra_slot WHERE id = :extra_id RETURNING id"),
        {"extra_id": extra_id},
    )
    deleted = result.scalar()
    await db.commit()

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Extra slot {extra_id} no encontrado")

    return {"ok": True}


@router.get("/{anio}/trimestre/{trimestre}/resumen", response_model=TrimestreResumenResponse)
async def obtener_resumen_trimestre(
    anio: int,
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Summary for a specific quarter: normal/intensive weeks, total slots.
    Used by frequency calculator and planning dashboard.
    """
    # Validate trimestre format
    if not trimestre.startswith(f"{anio}-Q"):
        raise HTTPException(status_code=400, detail=f"Trimestre debe empezar con '{anio}-Q'")

    tri_anio, week_start, week_end = trimestre_to_weeks(trimestre)

    semanas_normales = 0
    semanas_intensivas = 0
    total_ef = 0
    total_it = 0
    semanas_detalle = []

    for sem in range(week_start, week_end + 1):
        talleres = await cargar_talleres_semana(db, anio, sem)
        ef_count = sum(1 for t in talleres if t["programa"] == "EF")
        it_count = sum(1 for t in talleres if t["programa"] == "IT")

        # Get tipo
        result = await db.execute(
            text("SELECT tipo FROM semana_config WHERE anio = :anio AND semana = :semana"),
            {"anio": anio, "semana": sem},
        )
        row = result.mappings().first()
        tipo = row["tipo"] if row else "normal"

        if tipo == "intensiva":
            semanas_intensivas += 1
        else:
            semanas_normales += 1

        total_ef += ef_count
        total_it += it_count

        semanas_detalle.append({
            "semana_iso": sem,
            "semana_rel": sem - week_start + 1,
            "tipo": tipo,
            "ef": ef_count,
            "it": it_count,
            "total": ef_count + it_count,
        })

    return TrimestreResumenResponse(
        trimestre=trimestre,
        anio=anio,
        semanas_normales=semanas_normales,
        semanas_intensivas=semanas_intensivas,
        total_slots_ef=total_ef,
        total_slots_it=total_it,
        total_slots=total_ef + total_it,
        semanas_detalle=semanas_detalle,
    )
