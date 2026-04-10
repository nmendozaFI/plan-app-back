"""
Histórico — Importar y consultar datos históricos de talleres.
Columnas en camelCase (Prisma). Tabla: "historicoTaller".
"""

from fastapi import APIRouter, Depends, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db import get_db

router = APIRouter()


@router.get("/")
async def listar_historico(
    trimestre: str | None = None,
    empresa_id: int | None = None,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    """Consulta el histórico con filtros opcionales."""
    conditions = []
    params: dict = {"limit": limit}

    if trimestre:
        conditions.append('h.trimestre = :trimestre')
        params["trimestre"] = trimestre
    if empresa_id:
        conditions.append('h."empresaId" = :empresa_id')
        params["empresa_id"] = empresa_id

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = text(f"""
        SELECT h.id, h."empresaId", e.nombre AS empresa_nombre,
               h."tallerId", t.nombre AS taller_nombre,
               h.fecha, h.estado, h.ciudad, h.trimestre
        FROM "historicoTaller" h
        JOIN empresa e ON e.id = h."empresaId"
        JOIN taller t ON t.id = h."tallerId"
        {where}
        ORDER BY h.fecha DESC
        LIMIT :limit
    """)
    result = await db.execute(query, params)
    return {"historico": [dict(r) for r in result.mappings().all()]}


@router.post("/importar")
async def importar_historico(
    archivo: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Importa histórico desde CSV/Excel.
    Columnas esperadas: Id_Empresa, Id_Taller, Fecha_Taller, Estado, Ciudad
    """
    import pandas as pd
    from io import BytesIO

    content = await archivo.read()
    filename = archivo.filename or ""

    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        df = pd.read_excel(BytesIO(content))
    else:
        df = pd.read_csv(BytesIO(content))

    column_map = {
        "Id_Empresa": "empresaId",
        "Id_Taller": "tallerId",
        "Fecha_Taller": "fecha",
        "Estado": "estado",
        "Ciudad": "ciudad",
    }
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    inserted = 0
    errors = []

    for idx, row in df.iterrows():
        try:
            await db.execute(
                text("""
                    INSERT INTO "historicoTaller"
                        ("empresaId", "tallerId", fecha, estado, ciudad)
                    VALUES (:empresaId, :tallerId, :fecha, :estado, :ciudad)
                """),
                {
                    "empresaId": int(row["empresaId"]),
                    "tallerId": int(row["tallerId"]),
                    "fecha": pd.to_datetime(row["fecha"]).date(),
                    "estado": row.get("estado", "OK"),
                    "ciudad": row.get("ciudad"),
                },
            )
            inserted += 1
        except Exception as e:
            errors.append(f"Fila {idx}: {str(e)}")

    await db.commit()
    return {
        "insertados": inserted,
        "errores": len(errors),
        "detalle_errores": errors[:10],
    }


@router.get("/stats/{trimestre}")
async def stats_trimestre(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """Estadísticas agregadas de un trimestre para Fase 1."""
    query = text("""
        SELECT e.id AS empresa_id, e.nombre,
               COUNT(*) AS total,
               SUM(CASE WHEN h.estado = 'OK' THEN 1 ELSE 0 END) AS ok,
               SUM(CASE WHEN h.estado = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados,
               ROUND(
                   SUM(CASE WHEN h.estado = 'OK' THEN 1 ELSE 0 END)::numeric
                   / NULLIF(COUNT(*), 0), 2
               ) AS tasa_cumplimiento
        FROM "historicoTaller" h
        JOIN empresa e ON e.id = h."empresaId"
        WHERE h.trimestre = :trimestre
        GROUP BY e.id, e.nombre
        ORDER BY tasa_cumplimiento ASC
    """)
    result = await db.execute(query, {"trimestre": trimestre})
    return {"stats": [dict(r) for r in result.mappings().all()]}