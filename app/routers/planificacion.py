"""V20: planificacion-level endpoints (separate from calendario).

Currently hosts only the EXTRA delete endpoint, which protects against
accidental deletion of BASE rows by checking tipoAsignacion before deleting.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db

router = APIRouter()


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
