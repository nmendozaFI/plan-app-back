"""
Empresas — CRUD completo de datos maestros.

Endpoints:
  GET    /api/empresas              → lista (con filtro activa, tipo, semaforo)
  GET    /api/empresas/{id}         → detalle + restricciones + ciudades
  POST   /api/empresas              → crear empresa
  PUT    /api/empresas/{id}         → editar empresa
  PATCH  /api/empresas/{id}/toggle  → activar/desactivar
  GET    /api/empresas/{id}/historico-resumen → stats por trimestre

CONVENCIÓN: Prisma genera columnas en camelCase con @@map().
  - empresa, taller, ciudad → minúsculas
  - "empresaCiudad", "configTrimestral", "historicoTaller" → camelCase entre comillas
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class EmpresaCreate(BaseModel):
    nombre: str
    tipo: str = "AMBAS"          # EF | IT | AMBAS
    semaforo: str = "AMBAR"      # VERDE | AMBAR | ROJO
    scoreV3: float = 0
    fiabilidadReciente: float = 0
    esComodin: bool = False
    aceptaExtras: bool = False
    maxExtrasTrimestre: int = 0
    prioridadReduccion: str = "MEDIA"  # ALTA | MEDIA | BAJA
    tieneBolsa: bool = False
    turnoPreferido: str | None = None
    notas: str | None = None


class EmpresaUpdate(BaseModel):
    nombre: str | None = None
    tipo: str | None = None
    semaforo: str | None = None
    scoreV3: float | None = None
    fiabilidadReciente: float | None = None
    esComodin: bool | None = None
    aceptaExtras: bool | None = None
    maxExtrasTrimestre: int | None = None
    prioridadReduccion: str | None = None
    tieneBolsa: bool | None = None
    turnoPreferido: str | None = None
    notas: str | None = None


# ── Validación ───────────────────────────────────────────────

TIPOS_VALIDOS = {"EF", "IT", "AMBAS"}
SEMAFOROS_VALIDOS = {"VERDE", "AMBAR", "ROJO"}
PRIORIDADES_VALIDAS = {"ALTA", "MEDIA", "BAJA"}


def _validar_empresa(data: EmpresaCreate | EmpresaUpdate, is_update: bool = False):
    if data.tipo is not None and data.tipo not in TIPOS_VALIDOS:
        raise HTTPException(400, f"tipo debe ser EF/IT/AMBAS, recibido: {data.tipo}")
    if data.semaforo is not None and data.semaforo not in SEMAFOROS_VALIDOS:
        raise HTTPException(400, f"semáforo debe ser VERDE/AMBAR/ROJO, recibido: {data.semaforo}")
    if data.prioridadReduccion is not None and data.prioridadReduccion not in PRIORIDADES_VALIDAS:
        raise HTTPException(400, f"prioridadReduccion debe ser ALTA/MEDIA/BAJA, recibido: {data.prioridadReduccion}")
    if not is_update and hasattr(data, "nombre") and (not data.nombre or not data.nombre.strip()):
        raise HTTPException(400, "El nombre de la empresa es obligatorio")


# ── Endpoints ────────────────────────────────────────────────

@router.get("/")
async def listar_empresas(
    activa: bool | None = None,
    tipo: str | None = None,
    semaforo: str | None = None,
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Lista empresas con filtros opcionales."""
    q = """
        SELECT id, nombre, tipo, semaforo,
               "scoreV3", "fiabilidadReciente",
               "esComodin", "aceptaExtras",
               "maxExtrasTrimestre",
               "prioridadReduccion", "tieneBolsa",
               "turnoPreferido", activa, notas
        FROM empresa
        WHERE 1=1
    """
    params: dict = {}

    if activa is not None:
        q += " AND activa = :activa"
        params["activa"] = activa

    if tipo is not None:
        q += " AND tipo = :tipo"
        params["tipo"] = tipo

    if semaforo is not None:
        q += " AND semaforo = :semaforo"
        params["semaforo"] = semaforo

    if search:
        q += " AND LOWER(nombre) LIKE :search"
        params["search"] = f"%{search.lower()}%"

    q += ' ORDER BY "scoreV3" DESC'

    result = await db.execute(text(q), params)
    rows = result.mappings().all()
    return {"empresas": [dict(r) for r in rows]}


@router.get("/{empresa_id}")
async def detalle_empresa(
    empresa_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Detalle de una empresa con restricciones, ciudades y resumen."""
    emp = await db.execute(
        text("""
            SELECT id, nombre, tipo, semaforo,
                   "scoreV3", "fiabilidadReciente",
                   "esComodin", "aceptaExtras",
                   "maxExtrasTrimestre",
                   "prioridadReduccion", "tieneBolsa",
                   "turnoPreferido", activa, notas,
                   "createdAt", "updatedAt"
            FROM empresa WHERE id = :id
        """),
        {"id": empresa_id},
    )
    empresa = emp.mappings().first()
    if not empresa:
        raise HTTPException(404, f"Empresa {empresa_id} no encontrada")

    rest = await db.execute(
        text("""
            SELECT id, tipo, clave, valor, descripcion
            FROM restriccion
            WHERE "empresaId" = :id
            ORDER BY tipo, clave
        """),
        {"id": empresa_id},
    )

    ciudades = await db.execute(
        text("""
            SELECT c.id, c.nombre, ec."activaReciente"
            FROM "empresaCiudad" ec
            JOIN ciudad c ON c.id = ec."ciudadId"
            WHERE ec."empresaId" = :id
        """),
        {"id": empresa_id},
    )

    # Resumen rápido de talleres por trimestre
    historico = await db.execute(
        text("""
            SELECT trimestre,
                   COUNT(*) AS total,
                   SUM(CASE WHEN estado = 'OK' THEN 1 ELSE 0 END) AS ok,
                   SUM(CASE WHEN estado = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados
            FROM "historicoTaller"
            WHERE "empresaId" = :id
            GROUP BY trimestre
            ORDER BY trimestre DESC
            LIMIT 4
        """),
        {"id": empresa_id},
    )

    return {
        "empresa": dict(empresa),
        "restricciones": [dict(r) for r in rest.mappings().all()],
        "ciudades": [dict(c) for c in ciudades.mappings().all()],
        "historico_reciente": [dict(h) for h in historico.mappings().all()],
    }


@router.post("/", status_code=201)
async def crear_empresa(
    data: EmpresaCreate,
    db: AsyncSession = Depends(get_db),
):
    """Crea una nueva empresa."""
    _validar_empresa(data)

    # Check duplicado por nombre
    dup = await db.execute(
        text("SELECT id FROM empresa WHERE LOWER(nombre) = LOWER(:nombre)"),
        {"nombre": data.nombre.strip()},
    )
    if dup.mappings().first():
        raise HTTPException(409, f"Ya existe una empresa con nombre '{data.nombre}'")

    result = await db.execute(
        text("""
            INSERT INTO empresa (
                nombre, tipo, semaforo, "scoreV3", "fiabilidadReciente",
                "esComodin", "aceptaExtras", "maxExtrasTrimestre",
                "prioridadReduccion", "tieneBolsa", "turnoPreferido",
                activa, notas, "createdAt", "updatedAt"
            ) VALUES (
                :nombre, :tipo, :semaforo, :scoreV3, :fiabilidadReciente,
                :esComodin, :aceptaExtras, :maxExtrasTrimestre,
                :prioridadReduccion, :tieneBolsa, :turnoPreferido,
                true, :notas, NOW(), NOW()
            )
            RETURNING id, nombre, tipo, semaforo, "scoreV3",
                      "fiabilidadReciente", "esComodin", "aceptaExtras",
                      "maxExtrasTrimestre", "prioridadReduccion",
                      "tieneBolsa", "turnoPreferido", activa, notas
        """),
        {
            "nombre": data.nombre.strip(),
            "tipo": data.tipo,
            "semaforo": data.semaforo,
            "scoreV3": data.scoreV3,
            "fiabilidadReciente": data.fiabilidadReciente,
            "esComodin": data.esComodin,
            "aceptaExtras": data.aceptaExtras,
            "maxExtrasTrimestre": data.maxExtrasTrimestre,
            "prioridadReduccion": data.prioridadReduccion,
            "tieneBolsa": data.tieneBolsa,
            "turnoPreferido": data.turnoPreferido,
            "notas": data.notas,
        },
    )
    row = result.mappings().first()
    await db.commit()
    return {"empresa": dict(row)}


@router.put("/{empresa_id}")
async def editar_empresa(
    empresa_id: int,
    data: EmpresaUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Edita una empresa existente (solo campos enviados)."""
    _validar_empresa(data, is_update=True)

    # Verificar existencia
    existing = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE id = :id"),
        {"id": empresa_id},
    )
    if not existing.mappings().first():
        raise HTTPException(404, f"Empresa {empresa_id} no encontrada")

    # Check duplicado de nombre (si cambia)
    if data.nombre is not None:
        dup = await db.execute(
            text("SELECT id FROM empresa WHERE LOWER(nombre) = LOWER(:nombre) AND id != :id"),
            {"nombre": data.nombre.strip(), "id": empresa_id},
        )
        if dup.mappings().first():
            raise HTTPException(409, f"Ya existe otra empresa con nombre '{data.nombre}'")

    # Construir SET dinámico — solo campos no-None
    field_map = {
        "nombre": "nombre",
        "tipo": "tipo",
        "semaforo": "semaforo",
        "scoreV3": '"scoreV3"',
        "fiabilidadReciente": '"fiabilidadReciente"',
        "esComodin": '"esComodin"',
        "aceptaExtras": '"aceptaExtras"',
        "maxExtrasTrimestre": '"maxExtrasTrimestre"',
        "prioridadReduccion": '"prioridadReduccion"',
        "tieneBolsa": '"tieneBolsa"',
        "turnoPreferido": '"turnoPreferido"',
        "notas": "notas",
    }

    sets = []
    params = {"id": empresa_id}
    for py_field, sql_col in field_map.items():
        val = getattr(data, py_field, None)
        if val is not None:
            if py_field == "nombre":
                val = val.strip()
            sets.append(f"{sql_col} = :{py_field}")
            params[py_field] = val

    if not sets:
        raise HTTPException(400, "No se envió ningún campo para actualizar")

    sets.append('"updatedAt" = NOW()')
    set_clause = ", ".join(sets)

    result = await db.execute(
        text(f"""
            UPDATE empresa SET {set_clause}
            WHERE id = :id
            RETURNING id, nombre, tipo, semaforo, "scoreV3",
                      "fiabilidadReciente", "esComodin", "aceptaExtras",
                      "maxExtrasTrimestre", "prioridadReduccion",
                      "tieneBolsa", "turnoPreferido", activa, notas
        """),
        params,
    )
    row = result.mappings().first()
    await db.commit()
    return {"empresa": dict(row)}


@router.patch("/{empresa_id}/toggle")
async def toggle_activa(
    empresa_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Activa/desactiva una empresa."""
    existing = await db.execute(
        text("SELECT id, activa FROM empresa WHERE id = :id"),
        {"id": empresa_id},
    )
    row = existing.mappings().first()
    if not row:
        raise HTTPException(404, f"Empresa {empresa_id} no encontrada")

    new_activa = not row["activa"]
    await db.execute(
        text('UPDATE empresa SET activa = :activa, "updatedAt" = NOW() WHERE id = :id'),
        {"activa": new_activa, "id": empresa_id},
    )
    await db.commit()
    return {"id": empresa_id, "activa": new_activa}


@router.get("/{empresa_id}/historico-resumen")
async def resumen_historico(
    empresa_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Resumen de talleres asignados vs impartidos por trimestre."""
    query = text("""
        SELECT trimestre,
               COUNT(*) AS total_asignados,
               SUM(CASE WHEN estado = 'OK' THEN 1 ELSE 0 END) AS impartidos,
               SUM(CASE WHEN estado = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados
        FROM "historicoTaller"
        WHERE "empresaId" = :id
        GROUP BY trimestre
        ORDER BY trimestre DESC
    """)
    result = await db.execute(query, {"id": empresa_id})
    return {"resumen": [dict(r) for r in result.mappings().all()]}