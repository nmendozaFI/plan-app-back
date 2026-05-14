"""V22 (Cambio A): shared empresa-level guards reused by EXTRA and DOBLE routers.

These were originally inlined in `app/routers/planificacion.py`. Extracted so the
new `doble.py` router can reuse them without duplicating the SQL or the 404/422
contract. Future `permiteExtras`-based gate (Fase 5) will live here too.
"""

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def check_empresa_activa(db: AsyncSession, empresa_id: int) -> str:
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


async def check_empresa_es_ep(
    db: AsyncSession, empresa_id: int, empresa_nombre: str, trimestre: str
) -> None:
    """Raise 422 if empresa is not escuelaPropia=true in this trimestre.

    EP (Escuela Propia) is the gate for DOBLE creation (semana intensiva,
    decision 3 of Cambio A). Also used by PATCH /extra when changing the empresa
    of an existing EXTRA — that endpoint keeps EP semantics (plan §4: "Sin
    cambios en PATCH /extra"), even though POST /extra now uses permiteExtras.
    """
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
                f"en {trimestre}."
            ),
        )


async def check_empresa_permite_extras(
    db: AsyncSession, empresa_id: int, empresa_nombre: str, trimestre: str
) -> None:
    """Raise 422 if empresa doesn't have permiteExtras=true in this trimestre.

    V22 (Cambio A, Fase 5): gate for POST /api/planificacion/{trimestre}/extra.
    Decoupled from escuelaPropia — `permiteExtras` is the explicit per-trimestre
    flag for "this empresa can receive EXTRA slots on top of its regular
    frequency". A missing configTrimestral row is treated as permiteExtras=false.
    """
    row = await db.execute(
        text(
            'SELECT "permiteExtras" FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": empresa_id, "tri": trimestre},
    )
    rec = row.mappings().first()
    if rec is None or not rec["permiteExtras"]:
        raise HTTPException(
            status_code=422,
            detail=(
                f"La empresa {empresa_nombre} no tiene permite extras activado "
                f"en {trimestre}. Solo empresas con permiteExtras=true pueden "
                f"recibir slots EXTRA."
            ),
        )
