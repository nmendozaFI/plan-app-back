"""V20+V21: planificacion-level endpoints (separate from calendario).

V20: DELETE /{slot_id}/extra — guarded delete (only EXTRA rows).
V21: POST /{trimestre}/extra — create one EXTRA slot, validating the AND-rule.
V21: PATCH /{slot_id}/extra — edit empresa and/or notas of an existing EXTRA.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.schemas.calendario import (
    CrearSlotExtraInput,
    EditarSlotExtraInput,
    SlotExtraResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Helpers ─────────────────────────────────────────────────────


async def _fetch_extra_response(db: AsyncSession, slot_id: int) -> SlotExtraResponse:
    """Build SlotExtraResponse for a given Planificacion id (joins empresa/taller)."""
    res = await db.execute(
        text(
            '''
            SELECT
                p.id,
                p.semana,
                p.dia,
                p.horario,
                p."empresaId"   AS empresa_id,
                p.estado,
                p.confirmado,
                p.notas,
                p."motivoCambio" AS motivo_cambio,
                p."createdAt"   AS created_at,
                e.nombre        AS empresa_nombre,
                t.nombre        AS taller_nombre
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
        # Should not happen: caller just inserted/updated this id.
        raise HTTPException(
            status_code=500,
            detail=f"Planificacion id={slot_id} desapareció tras la operación",
        )
    return SlotExtraResponse(
        id=r["id"],
        semana=r["semana"],
        dia=r["dia"],
        horario=r["horario"] or "",
        taller_nombre=r["taller_nombre"] or "",
        empresa_id=r["empresa_id"],
        empresa_nombre=r["empresa_nombre"],
        estado=r["estado"],
        confirmado=bool(r["confirmado"]),
        notas=r["notas"],
        motivo_cambio=r["motivo_cambio"],
        created_at=r["created_at"],
    )


async def _check_empresa_activa(db: AsyncSession, empresa_id: int) -> str:
    """Return empresa.nombre if exists & activa; raise 404/422 otherwise."""
    row = await db.execute(
        text("SELECT id, nombre, activa FROM empresa WHERE id = :id"),
        {"id": empresa_id},
    )
    rec = row.mappings().first()
    if rec is None:
        raise HTTPException(
            status_code=404, detail=f"Empresa id={empresa_id} no existe"
        )
    if not rec["activa"]:
        raise HTTPException(
            status_code=422,
            detail=f"Empresa '{rec['nombre']}' (id={empresa_id}) está inactiva",
        )
    return rec["nombre"]


async def _check_empresa_es_ep(
    db: AsyncSession, empresa_id: int, empresa_nombre: str, trimestre: str
) -> None:
    """Raise 422 if empresa is not escuelaPropia=true in this trimestre."""
    row = await db.execute(
        text(
            'SELECT "escuelaPropia" FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": empresa_id, "tri": trimestre},
    )
    rec = row.mappings().first()
    if rec is None or not rec["escuelaPropia"]:
        raise HTTPException(
            status_code=422,
            detail=(
                f"La empresa {empresa_nombre} no tiene escuela propia activada "
                f"en {trimestre}. Solo empresas EP pueden tener slots EXTRA."
            ),
        )


# ── DELETE EXTRA (V20) ──────────────────────────────────────────


@router.delete("/{slot_id}/extra")
async def eliminar_extra(
    slot_id: int,
    db: AsyncSession = Depends(get_db),
):
    """V20: delete a Planificacion row only if its tipoAsignacion='EXTRA'.

    - 404 if slot_id does not exist.
    - 400 if it exists but is not an EXTRA (guards BASE / CONTINGENCIA from
      being deleted via this endpoint).
    """
    row = await db.execute(
        text('SELECT id, "tipoAsignacion" FROM planificacion WHERE id = :id'),
        {"id": slot_id},
    )
    rec = row.mappings().first()
    if rec is None:
        raise HTTPException(status_code=404, detail=f"Planificacion id={slot_id} no existe")

    tipo = rec["tipoAsignacion"]
    if tipo != "EXTRA":
        raise HTTPException(
            status_code=400,
            detail=(
                f"Planificacion id={slot_id} no es EXTRA (tipoAsignacion={tipo}). "
                "Este endpoint solo elimina filas EXTRA."
            ),
        )

    await db.execute(
        text("DELETE FROM planificacion WHERE id = :id"),
        {"id": slot_id},
    )
    await db.commit()
    return {"deleted_id": slot_id, "tipo_asignacion": "EXTRA"}


# ── POST EXTRA (V21) ────────────────────────────────────────────


@router.post("/{trimestre}/extra", response_model=SlotExtraResponse)
async def crear_extra(
    trimestre: str,
    body: CrearSlotExtraInput,
    db: AsyncSession = Depends(get_db),
):
    """V21: create one EXTRA slot, validating the AND-rule.

    Validation order (first failure wins):
      1. Empresa exists (404) and is activa (422).
      2. Taller exists (404).
      3. Empresa has escuelaPropia=true in this trimestre (422).
      4. There is at least one other slot in (trimestre, semana, dia, horario)
         belonging to a different empresa (422).
      5. No existing EXTRA already at (trimestre, semana, dia, horario, empresa)
         (422 with the existing id surfaced).
    """
    # 1. Empresa exists & activa.
    empresa_nombre = await _check_empresa_activa(db, body.empresa_id)

    # 2. Taller exists. Also fetch turno + programa for coherence + insert.
    taller_row = await db.execute(
        text('SELECT id, nombre, programa, turno FROM taller WHERE id = :id'),
        {"id": body.taller_id},
    )
    taller = taller_row.mappings().first()
    if taller is None:
        logger.info(
            "POST extra rechazado — taller id=%s no existe (trimestre=%s)",
            body.taller_id, trimestre,
        )
        raise HTTPException(
            status_code=404, detail=f"Taller id={body.taller_id} no existe"
        )

    # Coherence: programa must match the taller's programa. Frontend should
    # send the taller's programa; mismatches indicate a stale form/UI bug.
    if (taller["programa"] or "").strip().upper() != body.programa.upper():
        raise HTTPException(
            status_code=422,
            detail=(
                f"Programa '{body.programa}' no coincide con el del taller "
                f"'{taller['nombre']}' ({taller['programa']})."
            ),
        )

    # 3. Empresa is EP in trimestre.
    await _check_empresa_es_ep(db, body.empresa_id, empresa_nombre, trimestre)

    # 4. At least one colliding row from a different empresa.
    coll = await db.execute(
        text(
            '''
            SELECT 1 FROM planificacion
             WHERE trimestre = :tri
               AND semana = :sem
               AND dia = :dia
               AND horario = :horario
               AND "empresaId" IS NOT NULL
               AND "empresaId" != :eid
             LIMIT 1
            '''
        ),
        {
            "tri": trimestre,
            "sem": body.semana,
            "dia": body.dia,
            "horario": body.horario,
            "eid": body.empresa_id,
        },
    )
    if coll.scalar() is None:
        logger.info(
            "POST extra rechazado — sin colisión en %s sem=%s dia=%s horario=%s",
            trimestre, body.semana, body.dia, body.horario,
        )
        raise HTTPException(
            status_code=422,
            detail=(
                f"No hay otro slot en {trimestre} sem {body.semana} {body.dia} "
                f"{body.horario} con el que colisionar. Un EXTRA requiere otra "
                f"empresa en el mismo horario."
            ),
        )

    # 5. No exact duplicate EXTRA already.
    dup = await db.execute(
        text(
            '''
            SELECT id FROM planificacion
             WHERE trimestre = :tri
               AND semana = :sem
               AND dia = :dia
               AND horario = :horario
               AND "empresaId" = :eid
               AND "tipoAsignacion" = 'EXTRA'
             LIMIT 1
            '''
        ),
        {
            "tri": trimestre,
            "sem": body.semana,
            "dia": body.dia,
            "horario": body.horario,
            "eid": body.empresa_id,
        },
    )
    dup_id = dup.scalar()
    if dup_id is not None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Ya existe un slot EXTRA de {empresa_nombre} en {trimestre} "
                f"sem {body.semana} {body.dia} {body.horario} (id={dup_id})."
            ),
        )

    # Insert.
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
                'EXTRA', false, 'PLANIFICADO', false,
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
        "POST extra OK — id=%s empresa=%s trimestre=%s sem=%s %s %s",
        new_id, empresa_nombre, trimestre, body.semana, body.dia, body.horario,
    )

    return await _fetch_extra_response(db, new_id)


# ── PATCH EXTRA (V21) ───────────────────────────────────────────


@router.patch("/{slot_id}/extra", response_model=SlotExtraResponse)
async def editar_extra(
    slot_id: int,
    body: EditarSlotExtraInput,
    db: AsyncSession = Depends(get_db),
):
    """V21: edit an EXTRA slot's empresa and/or notas only.

    - Body must have at least one of empresa_id / notas (422 otherwise).
    - Slot must exist (404) and be EXTRA (400).
    - If empresa_id provided, the new empresa must exist+activa and be EP in
      the slot's trimestre (404 / 422).
    - notas accepted as-is, including empty string (= clear).
    - Never touches empresaIdOriginal, tipoAsignacion, estado, confirmado,
      motivoCambio, dia, horario, taller, semana, trimestre, turno, ciudad.
    """
    # 1. Body shape: at least one field.
    if body.empresa_id is None and body.notas is None:
        raise HTTPException(
            status_code=422,
            detail="Debe especificar al menos empresa_id o notas para editar.",
        )

    # 2. Slot exists.
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

    # 3. Slot is EXTRA.
    tipo = rec["tipoAsignacion"]
    if tipo != "EXTRA":
        raise HTTPException(
            status_code=400,
            detail=(
                f"El slot {slot_id} no es EXTRA (es {tipo}). "
                "Solo se pueden editar slots EXTRA con este endpoint."
            ),
        )

    trimestre = rec["trimestre"]

    # 4. Validate new empresa if provided.
    if body.empresa_id is not None:
        nueva_nombre = await _check_empresa_activa(db, body.empresa_id)
        await _check_empresa_es_ep(
            db, body.empresa_id, nueva_nombre, trimestre
        )

    # Build dynamic UPDATE.
    sets: list[str] = ['"updatedAt" = NOW()']
    params: dict = {"id": slot_id}
    if body.empresa_id is not None:
        sets.append('"empresaId" = :eid')
        params["eid"] = body.empresa_id
    if body.notas is not None:
        sets.append('notas = :notas')
        params["notas"] = body.notas  # empty string allowed

    sql = f"UPDATE planificacion SET {', '.join(sets)} WHERE id = :id"
    await db.execute(text(sql), params)
    await db.commit()
    logger.info(
        "PATCH extra OK — id=%s trimestre=%s empresa_id=%s notas_set=%s",
        slot_id, trimestre, body.empresa_id, body.notas is not None,
    )

    return await _fetch_extra_response(db, slot_id)
