"""
CONFIG TRIMESTRAL — CRUD para configuraciones trimestrales por empresa

Endpoints:
  GET  /{trimestre}             → Lista todas las configs del trimestre
  GET  /{trimestre}/resumen     → Resumen rápido
  PUT  /{trimestre}/batch       → Actualiza múltiples configs
  POST /{trimestre}/inicializar → Inicializa configs (clonar o crear default)
  PUT  /{trimestre}/{empresaId} → Actualiza config de una empresa

IMPORTANT: Route order matters! More specific routes (/batch, /resumen, /inicializar)
must be defined BEFORE the catch-all /{empresaId} route.

Tablas: configTrimestral, empresa
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class ConfigTrimestralOut(BaseModel):
    id: int
    empresa_id: int
    empresa_nombre: str
    tipo_participacion: str  # EF, IT, AMBAS
    escuela_propia: bool
    frecuencia_solicitada: Optional[int]
    disponibilidad_dias: str  # "L,M,X,J,V"
    turno_preferido: Optional[str]  # "M", "T", null
    voluntarios_disponibles: int
    preferencias_taller: Optional[str]
    notas: Optional[str]


class ConfigTrimestralUpdate(BaseModel):
    tipo_participacion: Optional[str] = None
    escuela_propia: Optional[bool] = None
    frecuencia_solicitada: Optional[int] = None
    disponibilidad_dias: Optional[str] = None
    turno_preferido: Optional[str] = None
    voluntarios_disponibles: Optional[int] = None
    preferencias_taller: Optional[str] = None
    notas: Optional[str] = None


class ConfigBatchUpdateItem(BaseModel):
    empresa_id: int
    tipo_participacion: Optional[str] = None
    escuela_propia: Optional[bool] = None
    frecuencia_solicitada: Optional[int] = None
    disponibilidad_dias: Optional[str] = None
    turno_preferido: Optional[str] = None
    voluntarios_disponibles: Optional[int] = None
    preferencias_taller: Optional[str] = None
    notas: Optional[str] = None


class ConfigBatchUpdateInput(BaseModel):
    updates: list[ConfigBatchUpdateItem]


class InicializarInput(BaseModel):
    origen_trimestre: Optional[str] = None  # Si se proporciona, clona desde ese trimestre


class InicializarResult(BaseModel):
    trimestre: str
    total_configs: int
    clonadas: int
    nuevas: int
    warnings: list[str]


class ConfigResumen(BaseModel):
    trimestre: str
    total_configs: int
    por_tipo: dict[str, int]  # EF, IT, AMBAS
    con_frecuencia: int
    sin_frecuencia: int
    escuela_propia: int


# ── Endpoints ────────────────────────────────────────────────
# NOTE: Order matters! Specific routes must come before parameterized routes.


@router.get("/{trimestre}")
async def obtener_configs_trimestre(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Devuelve todas las configuraciones trimestrales para el trimestre dado,
    unidas con el nombre de la empresa.
    """
    result = await db.execute(
        text("""
            SELECT
                ct.id,
                ct."empresaId" AS empresa_id,
                e.nombre AS empresa_nombre,
                ct."tipoParticipacion" AS tipo_participacion,
                ct."escuelaPropia" AS escuela_propia,
                ct."frecuenciaSolicitada" AS frecuencia_solicitada,
                ct."disponibilidadDias" AS disponibilidad_dias,
                ct."turnoPreferido" AS turno_preferido,
                ct."voluntariosDisponibles" AS voluntarios_disponibles,
                ct."preferenciasTaller" AS preferencias_taller,
                ct.notas
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct.trimestre = :trimestre
            ORDER BY e.nombre
        """),
        {"trimestre": trimestre},
    )
    configs = [dict(r) for r in result.mappings().all()]

    return {
        "trimestre": trimestre,
        "total": len(configs),
        "configs": configs,
    }


@router.get("/{trimestre}/resumen")
async def resumen_configs(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Devuelve un resumen rápido de las configuraciones del trimestre.
    """
    # Total y por tipo
    result = await db.execute(
        text("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN "tipoParticipacion" = 'EF' THEN 1 ELSE 0 END) AS ef,
                SUM(CASE WHEN "tipoParticipacion" = 'IT' THEN 1 ELSE 0 END) AS it,
                SUM(CASE WHEN "tipoParticipacion" = 'AMBAS' THEN 1 ELSE 0 END) AS ambas,
                SUM(CASE WHEN "frecuenciaSolicitada" IS NOT NULL AND "frecuenciaSolicitada" > 0 THEN 1 ELSE 0 END) AS con_freq,
                SUM(CASE WHEN "frecuenciaSolicitada" IS NULL OR "frecuenciaSolicitada" = 0 THEN 1 ELSE 0 END) AS sin_freq,
                SUM(CASE WHEN "escuelaPropia" = true THEN 1 ELSE 0 END) AS escuela_propia
            FROM "configTrimestral"
            WHERE trimestre = :tri
        """),
        {"tri": trimestre},
    )
    row = result.mappings().first()

    if not row or row["total"] == 0:
        return {
            "trimestre": trimestre,
            "total_configs": 0,
            "por_tipo": {"EF": 0, "IT": 0, "AMBAS": 0},
            "con_frecuencia": 0,
            "sin_frecuencia": 0,
            "escuela_propia": 0,
        }

    return {
        "trimestre": trimestre,
        "total_configs": row["total"] or 0,
        "por_tipo": {
            "EF": row["ef"] or 0,
            "IT": row["it"] or 0,
            "AMBAS": row["ambas"] or 0,
        },
        "con_frecuencia": row["con_freq"] or 0,
        "sin_frecuencia": row["sin_freq"] or 0,
        "escuela_propia": row["escuela_propia"] or 0,
    }


@router.put("/{trimestre}/batch")
async def actualizar_configs_batch(
    trimestre: str,
    body: ConfigBatchUpdateInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Actualiza múltiples configuraciones en una sola llamada.
    Ideal para guardar cambios de la tabla editable.
    """
    updated = 0
    errors = []

    for item in body.updates:
        try:
            # Verificar que existe la config
            cfg_check = await db.execute(
                text("""
                    SELECT id FROM "configTrimestral"
                    WHERE "empresaId" = :eid AND trimestre = :tri
                """),
                {"eid": item.empresa_id, "tri": trimestre},
            )
            if not cfg_check.first():
                errors.append(f"Config para empresa {item.empresa_id} no encontrada")
                continue

            updates = []
            params = {"eid": item.empresa_id, "tri": trimestre}

            if item.tipo_participacion is not None:
                updates.append('"tipoParticipacion" = :tipo')
                params["tipo"] = item.tipo_participacion

            if item.escuela_propia is not None:
                updates.append('"escuelaPropia" = :escuela')
                params["escuela"] = item.escuela_propia

            if item.frecuencia_solicitada is not None:
                updates.append('"frecuenciaSolicitada" = :freq')
                params["freq"] = item.frecuencia_solicitada

            if item.disponibilidad_dias is not None:
                updates.append('"disponibilidadDias" = :dias')
                params["dias"] = item.disponibilidad_dias

            if item.turno_preferido is not None:
                updates.append('"turnoPreferido" = :turno')
                params["turno"] = item.turno_preferido if item.turno_preferido != "" else None

            if item.voluntarios_disponibles is not None:
                updates.append('"voluntariosDisponibles" = :vol')
                params["vol"] = item.voluntarios_disponibles

            if item.preferencias_taller is not None:
                updates.append('"preferenciasTaller" = :pref')
                params["pref"] = item.preferencias_taller

            if item.notas is not None:
                updates.append("notas = :notas")
                params["notas"] = item.notas

            if updates:
                updates.append('"updatedAt" = NOW()')
                query = f"""
                    UPDATE "configTrimestral"
                    SET {', '.join(updates)}
                    WHERE "empresaId" = :eid AND trimestre = :tri
                """
                await db.execute(text(query), params)
                updated += 1

        except Exception as e:
            errors.append(f"Empresa {item.empresa_id}: {str(e)}")

    await db.commit()
    return {"updated": updated, "errors": errors}


@router.post("/{trimestre}/inicializar")
async def inicializar_configs(
    trimestre: str,
    body: InicializarInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Inicializa configuraciones para un trimestre nuevo.

    Si origen_trimestre se proporciona:
      - Clona las configs de ese trimestre (como el endpoint clonar-trimestre)

    Si no se proporciona:
      - Crea configs por defecto para todas las empresas activas
    """
    warnings: list[str] = []
    clonadas = 0
    nuevas = 0

    if body.origen_trimestre:
        # ── Modo clonar ──────────────────────────────────────
        rows = await db.execute(
            text("""
                SELECT
                    ct."empresaId",
                    e.nombre,
                    e.activa,
                    ct."tipoParticipacion",
                    ct."escuelaPropia",
                    ct."turnoPreferido",
                    ct."frecuenciaSolicitada",
                    ct."disponibilidadDias",
                    ct."voluntariosDisponibles",
                    ct."preferenciasTaller",
                    ct.notas
                FROM "configTrimestral" ct
                JOIN empresa e ON e.id = ct."empresaId"
                WHERE ct.trimestre = :origen
                ORDER BY e.nombre
            """),
            {"origen": body.origen_trimestre},
        )
        configs = [dict(r) for r in rows.mappings().all()]

        if not configs:
            raise HTTPException(
                status_code=404,
                detail=f"No hay configuraciones para el trimestre {body.origen_trimestre}",
            )

        saltadas = []
        for cfg in configs:
            if not cfg["activa"]:
                saltadas.append(cfg["nombre"])
                continue

            result = await db.execute(
                text("""
                    INSERT INTO "configTrimestral" (
                        "empresaId", trimestre, "tipoParticipacion",
                        "escuelaPropia", "disponibilidadDias",
                        "turnoPreferido", "frecuenciaSolicitada",
                        "voluntariosDisponibles", "preferenciasTaller",
                        notas, "updatedAt"
                    )
                    VALUES (
                        :eid, :destino, :tipo,
                        :escuela, :dias,
                        :turno, :freq,
                        :vol, :pref,
                        :notas, NOW()
                    )
                    ON CONFLICT ("empresaId", trimestre) DO NOTHING
                """),
                {
                    "eid": cfg["empresaId"],
                    "destino": trimestre,
                    "tipo": cfg["tipoParticipacion"],
                    "escuela": cfg["escuelaPropia"] or False,
                    "dias": cfg["disponibilidadDias"] or "L,M,X,J,V",
                    "turno": cfg["turnoPreferido"],
                    "freq": cfg["frecuenciaSolicitada"],
                    "vol": cfg["voluntariosDisponibles"] or 0,
                    "pref": cfg["preferenciasTaller"],
                    "notas": cfg["notas"],
                },
            )
            if result.rowcount > 0:
                clonadas += 1

        if saltadas:
            warnings.append(
                f"{len(saltadas)} empresa(s) inactiva(s) no clonadas: {', '.join(saltadas[:5])}"
                + ("..." if len(saltadas) > 5 else "")
            )

    else:
        # ── Modo crear por defecto ───────────────────────────
        # Obtener empresas activas que no tienen config para este trimestre
        rows = await db.execute(
            text("""
                SELECT e.id, e.nombre, e.tipo, e."turnoPreferido"
                FROM empresa e
                WHERE e.activa = true
                AND NOT EXISTS (
                    SELECT 1 FROM "configTrimestral" ct
                    WHERE ct."empresaId" = e.id AND ct.trimestre = :tri
                )
                ORDER BY e.nombre
            """),
            {"tri": trimestre},
        )
        empresas = [dict(r) for r in rows.mappings().all()]

        for emp in empresas:
            await db.execute(
                text("""
                    INSERT INTO "configTrimestral" (
                        "empresaId", trimestre, "tipoParticipacion",
                        "escuelaPropia", "disponibilidadDias",
                        "turnoPreferido", "voluntariosDisponibles",
                        "updatedAt"
                    )
                    VALUES (
                        :eid, :tri, :tipo,
                        false, 'L,M,X,J,V',
                        :turno, 0,
                        NOW()
                    )
                """),
                {
                    "eid": emp["id"],
                    "tri": trimestre,
                    "tipo": emp["tipo"] or "AMBAS",
                    "turno": emp["turnoPreferido"],
                },
            )
            nuevas += 1

    await db.commit()

    # Contar total de configs para el trimestre
    count_result = await db.execute(
        text('SELECT COUNT(*) FROM "configTrimestral" WHERE trimestre = :tri'),
        {"tri": trimestre},
    )
    total = count_result.scalar() or 0

    return {
        "trimestre": trimestre,
        "total_configs": total,
        "clonadas": clonadas,
        "nuevas": nuevas,
        "warnings": warnings,
    }


# NOTE: This route MUST come AFTER specific routes like /resumen, /batch, /inicializar
# because it catches all remaining paths with {empresa_id}
@router.put("/{trimestre}/{empresa_id}")
async def actualizar_config(
    trimestre: str,
    empresa_id: int,
    body: ConfigTrimestralUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Actualiza la configuración trimestral de una empresa.
    Si no existe, la crea con valores por defecto.
    """
    # Verificar que la empresa existe
    emp_check = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE id = :eid"),
        {"eid": empresa_id},
    )
    empresa = emp_check.mappings().first()
    if not empresa:
        raise HTTPException(status_code=404, detail=f"Empresa {empresa_id} no encontrada")

    # Verificar si existe la config
    cfg_check = await db.execute(
        text("""
            SELECT id FROM "configTrimestral"
            WHERE "empresaId" = :eid AND trimestre = :tri
        """),
        {"eid": empresa_id, "tri": trimestre},
    )
    existing = cfg_check.first()

    if existing:
        # Update existente
        updates = []
        params = {"eid": empresa_id, "tri": trimestre}

        if body.tipo_participacion is not None:
            updates.append('"tipoParticipacion" = :tipo')
            params["tipo"] = body.tipo_participacion

        if body.escuela_propia is not None:
            updates.append('"escuelaPropia" = :escuela')
            params["escuela"] = body.escuela_propia

        if body.frecuencia_solicitada is not None:
            updates.append('"frecuenciaSolicitada" = :freq')
            params["freq"] = body.frecuencia_solicitada

        if body.disponibilidad_dias is not None:
            updates.append('"disponibilidadDias" = :dias')
            params["dias"] = body.disponibilidad_dias

        if body.turno_preferido is not None:
            updates.append('"turnoPreferido" = :turno')
            params["turno"] = body.turno_preferido if body.turno_preferido != "" else None

        if body.voluntarios_disponibles is not None:
            updates.append('"voluntariosDisponibles" = :vol')
            params["vol"] = body.voluntarios_disponibles

        if body.preferencias_taller is not None:
            updates.append('"preferenciasTaller" = :pref')
            params["pref"] = body.preferencias_taller

        if body.notas is not None:
            updates.append("notas = :notas")
            params["notas"] = body.notas

        if updates:
            updates.append('"updatedAt" = NOW()')
            query = f"""
                UPDATE "configTrimestral"
                SET {', '.join(updates)}
                WHERE "empresaId" = :eid AND trimestre = :tri
            """
            await db.execute(text(query), params)
    else:
        # Crear nueva config
        await db.execute(
            text("""
                INSERT INTO "configTrimestral" (
                    "empresaId", trimestre, "tipoParticipacion",
                    "escuelaPropia", "frecuenciaSolicitada", "disponibilidadDias",
                    "turnoPreferido", "voluntariosDisponibles", "preferenciasTaller",
                    notas, "updatedAt"
                )
                VALUES (
                    :eid, :tri, :tipo, :escuela, :freq, :dias,
                    :turno, :vol, :pref, :notas, NOW()
                )
            """),
            {
                "eid": empresa_id,
                "tri": trimestre,
                "tipo": body.tipo_participacion or "AMBAS",
                "escuela": body.escuela_propia or False,
                "freq": body.frecuencia_solicitada,
                "dias": body.disponibilidad_dias or "L,M,X,J,V",
                "turno": body.turno_preferido if body.turno_preferido != "" else None,
                "vol": body.voluntarios_disponibles or 0,
                "pref": body.preferencias_taller,
                "notas": body.notas,
            },
        )

    await db.commit()

    # Devolver config actualizada
    result = await db.execute(
        text("""
            SELECT
                ct.id,
                ct."empresaId" AS empresa_id,
                e.nombre AS empresa_nombre,
                ct."tipoParticipacion" AS tipo_participacion,
                ct."escuelaPropia" AS escuela_propia,
                ct."frecuenciaSolicitada" AS frecuencia_solicitada,
                ct."disponibilidadDias" AS disponibilidad_dias,
                ct."turnoPreferido" AS turno_preferido,
                ct."voluntariosDisponibles" AS voluntarios_disponibles,
                ct."preferenciasTaller" AS preferencias_taller,
                ct.notas
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct."empresaId" = :eid AND ct.trimestre = :tri
        """),
        {"eid": empresa_id, "tri": trimestre},
    )
    config = result.mappings().first()

    return {"config": dict(config) if config else None}
