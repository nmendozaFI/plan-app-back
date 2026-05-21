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


async def check_empresa_puede_ser_ep(
    db: AsyncSession, empresa_id: int, empresa_nombre: str
) -> None:
    """Raise 422 if empresa is not flagged as puedeSerEP=true in its ficha.

    V25 Cambio C (Capa 3): gate de elegibilidad EP migrado de CT.escuelaPropia
    al flag estructural empresa.puedeSerEP. La elegibilidad es ahora persistente
    por empresa, no por trimestre. CT.escuelaPropia sigue existiendo pero solo
    se usa dentro del solver (Capa 5) para decidir concentración total en una
    semana — el gate de operación pasa por puedeSerEP.

    El parámetro `trimestre` que aceptaba la versión vieja (`check_empresa_es_ep`)
    se eliminó porque ya no participa en el filtro.
    """
    row = await db.execute(
        text('SELECT "puedeSerEP" FROM empresa WHERE id = :eid'),
        {"eid": empresa_id},
    )
    rec = row.mappings().first()
    if rec is None or not rec["puedeSerEP"]:
        raise HTTPException(
            status_code=422,
            detail=(
                f"La empresa {empresa_nombre} no tiene puedeSerEP activado "
                f"según ficha de empresa."
            ),
        )


async def check_empresa_puede_ser_doble(
    db: AsyncSession, empresa_id: int, empresa_nombre: str
) -> None:
    """Raise 422 if empresa is not flagged as puedeSerDoble=true in its ficha.

    V25 Cambio C (Capa 3): gate de elegibilidad DOBLE.
    Decisión §11.3: Doble se gatea por flag estructural en empresa, sin
    contraparte en CT. El solver NO genera DOBLE automáticamente; siempre es
    manual desde /planificacion/doble (POST/PATCH gating aquí).
    """
    row = await db.execute(
        text('SELECT "puedeSerDoble" FROM empresa WHERE id = :eid'),
        {"eid": empresa_id},
    )
    rec = row.mappings().first()
    if rec is None or not rec["puedeSerDoble"]:
        raise HTTPException(
            status_code=422,
            detail=(
                f"La empresa {empresa_nombre} no tiene puedeSerDoble activado "
                f"según ficha de empresa."
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
