"""
Talleres — CRUD completo para los slots fijos (14 EF + 6 IT por semana).
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class TallerOut(BaseModel):
    id: int
    nombre: str
    programa: str        # "EF" | "IT"
    dia_semana: str | None
    horario: str | None
    turno: str | None
    es_contratante: bool
    descripcion: str | None
    activo: bool


class TallerCreate(BaseModel):
    nombre: str
    programa: str        # "EF" | "IT"
    dia_semana: str | None = None
    horario: str | None = None
    turno: str | None = None
    es_contratante: bool = False
    descripcion: str | None = None
    activo: bool = True


class TallerUpdate(BaseModel):
    nombre: str | None = None
    programa: str | None = None
    dia_semana: str | None = None
    horario: str | None = None
    turno: str | None = None
    es_contratante: bool | None = None
    descripcion: str | None = None
    activo: bool | None = None


# ── GET /api/talleres ────────────────────────────────────────

@router.get("", response_model=list[TallerOut])
async def listar_talleres(
    programa: str | None = None,
    solo_activos: bool = True,
    db: AsyncSession = Depends(get_db),
):
    """Lista todos los talleres, con filtros opcionales por programa y estado."""
    query = """
        SELECT id, nombre, programa,
               "diaSemana" AS dia_semana,
               horario, turno,
               "esContratante" AS es_contratante,
               descripcion, activo
        FROM taller
        WHERE 1=1
    """
    params: dict = {}

    if solo_activos:
        query += " AND activo = true"

    if programa:
        query += " AND programa = :programa"
        params["programa"] = programa.upper()

    query += " ORDER BY programa, id"

    result = await db.execute(text(query), params)
    rows = [dict(r) for r in result.mappings().all()]
    return rows


# ── GET /api/talleres/{id} ───────────────────────────────────

@router.get("/{taller_id}", response_model=TallerOut)
async def obtener_taller(
    taller_id: int,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("""
            SELECT id, nombre, programa,
                   "diaSemana" AS dia_semana,
                   horario, turno,
                   "esContratante" AS es_contratante,
                   descripcion, activo
            FROM taller WHERE id = :id
        """),
        {"id": taller_id},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"Taller {taller_id} no encontrado")
    return dict(row)


# ── POST /api/talleres ───────────────────────────────────────

@router.post("", response_model=TallerOut, status_code=201)
async def crear_taller(
    data: TallerCreate,
    db: AsyncSession = Depends(get_db),
):
    """Crea un nuevo taller."""
    if data.programa not in ("EF", "IT"):
        raise HTTPException(status_code=400, detail="Programa debe ser 'EF' o 'IT'")
    if data.dia_semana and data.dia_semana not in ("L", "M", "X", "J", "V"):
        raise HTTPException(status_code=400, detail="Día debe ser L, M, X, J o V")

    result = await db.execute(
        text("""
            INSERT INTO taller (nombre, programa, "diaSemana", horario, turno,
                                "esContratante", descripcion, activo, "updatedAt")
            VALUES (:nombre, :programa, :dia, :horario, :turno,
                    :contratante, :descripcion, :activo, NOW())
            RETURNING id, nombre, programa,
                      "diaSemana" AS dia_semana,
                      horario, turno,
                      "esContratante" AS es_contratante,
                      descripcion, activo
        """),
        {
            "nombre": data.nombre,
            "programa": data.programa.upper(),
            "dia": data.dia_semana,
            "horario": data.horario,
            "turno": data.turno,
            "contratante": data.es_contratante,
            "descripcion": data.descripcion,
            "activo": data.activo,
        },
    )
    await db.commit()
    row = result.mappings().first()
    return dict(row)


# ── PUT /api/talleres/{id} ───────────────────────────────────

@router.put("/{taller_id}", response_model=TallerOut)
async def editar_taller(
    taller_id: int,
    data: TallerUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Edita un taller existente (solo campos enviados)."""
    # Verificar que existe
    check = await db.execute(text("SELECT id FROM taller WHERE id = :id"), {"id": taller_id})
    if not check.first():
        raise HTTPException(status_code=404, detail=f"Taller {taller_id} no encontrado")

    # Construir SET dinámico solo con campos no-None
    field_map = {
        "nombre": "nombre",
        "programa": "programa",
        "dia_semana": '"diaSemana"',
        "horario": "horario",
        "turno": "turno",
        "es_contratante": '"esContratante"',
        "descripcion": "descripcion",
        "activo": "activo",
    }

    updates = []
    params = {"id": taller_id}

    for py_field, db_col in field_map.items():
        val = getattr(data, py_field)
        if val is not None:
            # Validaciones
            if py_field == "programa" and val not in ("EF", "IT"):
                raise HTTPException(status_code=400, detail="Programa debe ser 'EF' o 'IT'")
            if py_field == "dia_semana" and val not in ("L", "M", "X", "J", "V"):
                raise HTTPException(status_code=400, detail="Día debe ser L, M, X, J o V")
            updates.append(f"{db_col} = :{py_field}")
            params[py_field] = val.upper() if py_field == "programa" else val

    if not updates:
        raise HTTPException(status_code=400, detail="No se enviaron campos para actualizar")

    updates.append('"updatedAt" = NOW()')
    set_clause = ", ".join(updates)

    result = await db.execute(
        text(f"""
            UPDATE taller SET {set_clause}
            WHERE id = :id
            RETURNING id, nombre, programa,
                      "diaSemana" AS dia_semana,
                      horario, turno,
                      "esContratante" AS es_contratante,
                      descripcion, activo
        """),
        params,
    )
    await db.commit()
    row = result.mappings().first()
    return dict(row)


# ── DELETE /api/talleres/{id} ────────────────────────────────

@router.delete("/{taller_id}")
async def eliminar_taller(
    taller_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Elimina un taller. Falla si tiene planificaciones asociadas."""
    # Verificar dependencias
    deps = await db.execute(
        text('SELECT COUNT(*) AS cnt FROM planificacion WHERE "tallerId" = :id'),
        {"id": taller_id},
    )
    cnt = deps.scalar()
    if cnt and cnt > 0:
        raise HTTPException(
            status_code=409,
            detail=f"No se puede eliminar: tiene {cnt} asignaciones en planificación. "
                   "Desactívalo en su lugar.",
        )

    result = await db.execute(
        text("DELETE FROM taller WHERE id = :id RETURNING id"),
        {"id": taller_id},
    )
    await db.commit()
    if not result.first():
        raise HTTPException(status_code=404, detail=f"Taller {taller_id} no encontrado")
    return {"ok": True, "id": taller_id}