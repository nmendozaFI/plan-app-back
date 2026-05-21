"""V22 (Cambio A): CRUD for DOBLE slots (ad-hoc semana intensiva).
V25 Cambio C (Capa 3): gate de elegibilidad migrado de CT.escuelaPropia al
flag estructural empresa.puedeSerDoble. DOBLE y EP son ahora conceptos
ortogonales — una empresa puede serDoble sin serEP y viceversa.

DOBLE = 2+ talleres misma empresa misma semana. Conceptually separate from
EXTRA — the planner adds talleres on top of the regular schedule for one
specific week, without the constraints that apply to EXTRA:

  - NO collision check (decision 4): a DOBLE can land on a slot already used
    by another empresa; that's the whole point.
  - NO programa coherence check: the taller's programa is used as-is.
  - NO duplicate check: the planner may legitimately need multiple DOBLE rows
    at the same (semana, día, horario) for the same empresa.
  - NO permiteExtras gate: only empresa.puedeSerDoble=true matters.

Endpoints:
  POST   /{trimestre}/doble           → create one DOBLE
  GET    /{trimestre}/dobles          → list (optional ?semana=&empresa_id=)
  PATCH  /{slot_id}/doble             → edit empresa/taller/semana/día/horario/notas
  DELETE /{slot_id}/doble             → delete (guarded to tipoAsignacion=DOBLE)
  DELETE /{trimestre}/extras-doble    → bulk cleanup of EXTRA+DOBLE rows (V22)

Mounted in main.py under prefix `/api/planificacion`, side-by-side with the
EXTRA endpoints in `planificacion.py`. The path suffixes (`/doble`,
`/dobles`, `/extras-doble`) keep routes unambiguous despite the shared prefix.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.schemas.calendario import (
    CleanupExtrasDobleResult,
    CrearSlotDobleInput,
    EditarSlotDobleInput,
    ListaDoblesResponse,
    SlotDobleResponse,
)
from app.services.empresas.checks import (
    check_empresa_activa,
    check_empresa_puede_ser_doble,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Helpers ─────────────────────────────────────────────────────


async def _fetch_doble_response(db: AsyncSession, slot_id: int) -> SlotDobleResponse:
    """Build SlotDobleResponse for a given Planificacion id (joins empresa/taller)."""
    res = await db.execute(
        text(
            '''
            SELECT
                p.id,
                p.semana,
                p.dia,
                p.horario,
                p."empresaId"   AS empresa_id,
                p."tallerId"    AS taller_id,
                p.estado,
                p.confirmado,
                p.notas,
                p."createdAt"   AS created_at,
                e.nombre        AS empresa_nombre,
                t.nombre        AS taller_nombre,
                t.programa      AS programa
              FROM planificacion p
              LEFT JOIN empresa e ON e.id = p."empresaId"
              LEFT JOIN taller  t ON t.id = p."tallerId"
             WHERE p.id = :id
            '''
        ),
        {"id": slot_id},
    )
    r = res.mappings().first()
    if r is None:
        raise HTTPException(
            status_code=500,
            detail=f"Planificacion id={slot_id} desapareció tras la operación",
        )
    return SlotDobleResponse(
        id=r["id"],
        semana=r["semana"],
        dia=r["dia"],
        horario=r["horario"] or "",
        taller_id=r["taller_id"],
        taller_nombre=r["taller_nombre"] or "",
        programa=(r["programa"] or "").strip().upper(),
        empresa_id=r["empresa_id"],
        empresa_nombre=r["empresa_nombre"],
        estado=r["estado"],
        confirmado=bool(r["confirmado"]),
        notas=r["notas"],
        created_at=r["created_at"],
    )


async def _fetch_taller_or_404(db: AsyncSession, taller_id: int) -> dict:
    """Return the taller row or raise 404. Used by POST and PATCH."""
    row = await db.execute(
        text('SELECT id, nombre, programa, turno FROM taller WHERE id = :id'),
        {"id": taller_id},
    )
    rec = row.mappings().first()
    if rec is None:
        raise HTTPException(
            status_code=404, detail=f"Taller id={taller_id} no existe"
        )
    return dict(rec)


# ── POST DOBLE ──────────────────────────────────────────────────


@router.post("/{trimestre}/doble", response_model=SlotDobleResponse)
async def crear_doble(
    trimestre: str,
    body: CrearSlotDobleInput,
    db: AsyncSession = Depends(get_db),
):
    """V22 (Cambio A): create one DOBLE slot.

    Validation order (first failure wins):
      1. Empresa exists (404) and is activa (422).
      2. Empresa has puedeSerDoble=true en su ficha (422).
         V25 Cambio C (Capa 3): gate migrado de CT.escuelaPropia al flag
         estructural empresa.puedeSerDoble. Doble dejó de mezclarse con EP.
      3. Taller exists (404).

    NO collision check, NO programa check, NO duplicate check (decision 4).
    """
    empresa_nombre = await check_empresa_activa(db, body.empresa_id)
    await check_empresa_puede_ser_doble(db, body.empresa_id, empresa_nombre)
    taller = await _fetch_taller_or_404(db, body.taller_id)

    notas_val = body.notas if (body.notas is not None and body.notas.strip()) else None
    res = await db.execute(
        text(
            '''
            INSERT INTO planificacion (
                trimestre, semana, dia, horario, turno,
                "empresaId", "empresaIdOriginal", "tallerId", "ciudadId",
                "tipoAsignacion", "esContingencia", estado, confirmado,
                notas, "motivoCambio", "updatedAt"
            ) VALUES (
                :tri, :sem, :dia, :horario, :turno,
                :eid, :eid, :tid, NULL,
                'DOBLE', false, 'PLANIFICADO', false,
                :notas, NULL, NOW()
            )
            RETURNING id
            '''
        ),
        {
            "tri": trimestre,
            "sem": body.semana,
            "dia": body.dia,
            "horario": body.horario,
            "turno": taller["turno"] or "",
            "eid": body.empresa_id,
            "tid": body.taller_id,
            "notas": notas_val,
        },
    )
    new_id = res.scalar()
    await db.commit()
    logger.info(
        "POST doble OK — id=%s empresa=%s trimestre=%s sem=%s %s %s taller=%s",
        new_id, empresa_nombre, trimestre,
        body.semana, body.dia, body.horario, taller["nombre"],
    )

    return await _fetch_doble_response(db, new_id)


# ── GET DOBLES ──────────────────────────────────────────────────


@router.get("/{trimestre}/dobles", response_model=ListaDoblesResponse)
async def listar_dobles(
    trimestre: str,
    semana: int | None = Query(default=None, ge=1, le=13),
    empresa_id: int | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """V22 (Cambio A): list DOBLE slots for a trimestre with optional filters.

    Order: (semana, dia, horario, empresa_nombre).
    Trimestre inexistente → 200 con lista vacía.
    """
    where = ['p.trimestre = :tri', 'p."tipoAsignacion" = \'DOBLE\'']
    params: dict = {"tri": trimestre}
    if semana is not None:
        where.append("p.semana = :sem")
        params["sem"] = semana
    if empresa_id is not None:
        where.append('p."empresaId" = :eid')
        params["eid"] = empresa_id

    sql = (
        '''
        SELECT
            p.id, p.semana, p.dia, p.horario,
            p."empresaId" AS empresa_id,
            p."tallerId"  AS taller_id,
            p.estado, p.confirmado, p.notas,
            p."createdAt" AS created_at,
            e.nombre      AS empresa_nombre,
            t.nombre      AS taller_nombre,
            t.programa    AS programa
          FROM planificacion p
          LEFT JOIN empresa e ON e.id = p."empresaId"
          LEFT JOIN taller  t ON t.id = p."tallerId"
         WHERE '''
        + " AND ".join(where)
        + " ORDER BY p.semana, p.dia, p.horario, e.nombre"
    )
    res = await db.execute(text(sql), params)
    rows = res.mappings().all()

    dobles = [
        SlotDobleResponse(
            id=r["id"],
            semana=r["semana"],
            dia=r["dia"],
            horario=r["horario"] or "",
            taller_id=r["taller_id"],
            taller_nombre=r["taller_nombre"] or "",
            programa=(r["programa"] or "").strip().upper(),
            empresa_id=r["empresa_id"],
            empresa_nombre=r["empresa_nombre"],
            estado=r["estado"],
            confirmado=bool(r["confirmado"]),
            notas=r["notas"],
            created_at=r["created_at"],
        )
        for r in rows
    ]
    return ListaDoblesResponse(trimestre=trimestre, total=len(dobles), dobles=dobles)


# ── PATCH DOBLE ─────────────────────────────────────────────────


@router.patch("/{slot_id}/doble", response_model=SlotDobleResponse)
async def editar_doble(
    slot_id: int,
    body: EditarSlotDobleInput,
    db: AsyncSession = Depends(get_db),
):
    """V22 (Cambio A): edit a DOBLE slot's fields (libertad ad-hoc).

    Editable: empresa_id, taller_id, semana, día, horario, notas.
    Body must have at least one field (422 otherwise).
    Slot must exist (404) and be DOBLE (400).
    If empresa_id provided: must exist+activa and have puedeSerDoble=true.
        (V25 Cambio C, Capa 3: gate migrado de CT.escuelaPropia a la ficha.)
    If taller_id provided: must exist.

    Never touches empresaIdOriginal, tipoAsignacion, estado, confirmado,
    motivoCambio, trimestre, turno, ciudadId.
    """
    fields = body.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(
            status_code=422,
            detail="Debe especificar al menos un campo a editar.",
        )

    row = await db.execute(
        text(
            'SELECT id, trimestre, "tipoAsignacion" '
            'FROM planificacion WHERE id = :id'
        ),
        {"id": slot_id},
    )
    rec = row.mappings().first()
    if rec is None:
        raise HTTPException(
            status_code=404, detail=f"Planificacion id={slot_id} no existe"
        )

    tipo = rec["tipoAsignacion"]
    if tipo != "DOBLE":
        raise HTTPException(
            status_code=400,
            detail=(
                f"El slot {slot_id} no es DOBLE (es {tipo}). "
                "Solo se pueden editar slots DOBLE con este endpoint."
            ),
        )

    trimestre = rec["trimestre"]  # solo para logging; el gate ya no depende de él.

    # Validate new empresa if provided.
    # V25 Cambio C (Capa 3): gate ahora por empresa.puedeSerDoble (ficha),
    # no por CT.escuelaPropia del trimestre.
    if "empresa_id" in fields and fields["empresa_id"] is not None:
        nueva_nombre = await check_empresa_activa(db, fields["empresa_id"])
        await check_empresa_puede_ser_doble(db, fields["empresa_id"], nueva_nombre)

    # Validate new taller if provided.
    if "taller_id" in fields and fields["taller_id"] is not None:
        await _fetch_taller_or_404(db, fields["taller_id"])

    # Build dynamic UPDATE. Map body field → SQL column.
    column_map = {
        "empresa_id": '"empresaId"',
        "taller_id": '"tallerId"',
        "semana": "semana",
        "dia": "dia",
        "horario": "horario",
        "notas": "notas",
    }
    sets: list[str] = ['"updatedAt" = NOW()']
    params: dict = {"id": slot_id}
    for key, value in fields.items():
        col = column_map.get(key)
        if col is None:
            continue  # Shouldn't happen — Pydantic gates the field set.
        sets.append(f"{col} = :{key}")
        params[key] = value

    sql = f"UPDATE planificacion SET {', '.join(sets)} WHERE id = :id"
    await db.execute(text(sql), params)
    await db.commit()
    logger.info(
        "PATCH doble OK — id=%s trimestre=%s fields=%s",
        slot_id, trimestre, list(fields.keys()),
    )

    return await _fetch_doble_response(db, slot_id)


# ── DELETE DOBLE (single) ───────────────────────────────────────


@router.delete("/{slot_id}/doble")
async def eliminar_doble(
    slot_id: int,
    db: AsyncSession = Depends(get_db),
):
    """V22 (Cambio A): delete a Planificacion row only if its tipoAsignacion='DOBLE'.

    - 404 if slot_id does not exist.
    - 400 if it exists but is not a DOBLE (guards BASE/EXTRA/CONTINGENCIA).
    """
    row = await db.execute(
        text('SELECT id, "tipoAsignacion" FROM planificacion WHERE id = :id'),
        {"id": slot_id},
    )
    rec = row.mappings().first()
    if rec is None:
        raise HTTPException(
            status_code=404, detail=f"Planificacion id={slot_id} no existe"
        )

    tipo = rec["tipoAsignacion"]
    if tipo != "DOBLE":
        raise HTTPException(
            status_code=400,
            detail=(
                f"Planificacion id={slot_id} no es DOBLE (tipoAsignacion={tipo}). "
                "Este endpoint solo elimina filas DOBLE."
            ),
        )

    await db.execute(
        text("DELETE FROM planificacion WHERE id = :id"),
        {"id": slot_id},
    )
    await db.commit()
    logger.info("DELETE doble OK — id=%s", slot_id)
    return {"deleted_id": slot_id, "tipo_asignacion": "DOBLE"}


# ── DELETE bulk cleanup of EXTRA + DOBLE (V22) ──────────────────


@router.delete(
    "/{trimestre}/extras-doble",
    response_model=CleanupExtrasDobleResult,
)
async def cleanup_extras_doble(
    trimestre: str,
    confirmar: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
):
    """V22 (Cambio A): wipe every EXTRA and DOBLE row from a trimestre.

    Used in Fase 7 to drop the 39 legacy-EXTRA rows from Q2 before the planner
    re-imports the Excel with the proper EXTRA→DOBLE classification.

    Requires `?confirmar=true` — without it returns 400. BASE/CONTINGENCIA rows
    are never touched.
    """
    if not confirmar:
        raise HTTPException(
            status_code=400,
            detail=(
                "Operación destructiva: pasa ?confirmar=true para confirmar el "
                "borrado masivo de EXTRA+DOBLE en este trimestre."
            ),
        )

    # Count first (informational) — single transaction with the DELETE so the
    # numbers reflect what was actually deleted.
    pre = await db.execute(
        text(
            'SELECT "tipoAsignacion", COUNT(*) AS n FROM planificacion '
            'WHERE trimestre = :tri AND "tipoAsignacion" IN (\'EXTRA\', \'DOBLE\') '
            'GROUP BY "tipoAsignacion"'
        ),
        {"tri": trimestre},
    )
    counts: dict[str, int] = {r["tipoAsignacion"]: r["n"] for r in pre.mappings().all()}
    extras_n = counts.get("EXTRA", 0)
    dobles_n = counts.get("DOBLE", 0)

    await db.execute(
        text(
            'DELETE FROM planificacion '
            'WHERE trimestre = :tri AND "tipoAsignacion" IN (\'EXTRA\', \'DOBLE\')'
        ),
        {"tri": trimestre},
    )
    await db.commit()
    logger.info(
        "DELETE extras-doble OK — trimestre=%s extras=%s dobles=%s",
        trimestre, extras_n, dobles_n,
    )

    return CleanupExtrasDobleResult(
        trimestre=trimestre,
        confirmar=True,
        extras_eliminados=extras_n,
        dobles_eliminados=dobles_n,
    )
