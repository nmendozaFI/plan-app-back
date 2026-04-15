"""
SETTINGS — Global application settings (singleton)

Endpoints:
  GET  /                  → Returns current settings
  PUT  /                  → Update settings
  POST /promover          → Promote: siguiente → activo, clear siguiente

Table: appSettings (singleton with id=1)
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class AppSettingsOut(BaseModel):
    trimestre_activo: str
    trimestre_siguiente: Optional[str]


class AppSettingsUpdate(BaseModel):
    trimestre_activo: Optional[str] = None
    trimestre_siguiente: Optional[str] = None


class PromoverResult(BaseModel):
    success: bool
    message: str
    settings: AppSettingsOut


class PlanningStatusOut(BaseModel):
    """Full planning status for UI decision-making."""
    trimestre_activo: str
    trimestre_siguiente: Optional[str]
    activo_tiene_frecuencias: bool
    activo_tiene_calendario: bool
    siguiente_tiene_frecuencias: bool
    siguiente_tiene_calendario: bool
    # Derived: which trimestre should be planned next
    trimestre_a_planificar: Optional[str]
    activo_necesita_planificacion: bool


# ── Endpoints ────────────────────────────────────────────────


@router.get("/", response_model=AppSettingsOut)
async def obtener_settings(db: AsyncSession = Depends(get_db)):
    """
    Returns the current application settings.
    Creates default settings if they don't exist.
    """
    result = await db.execute(
        text("""
            SELECT "trimestreActivo", "trimestreSiguiente"
            FROM "appSettings"
            WHERE id = 1
        """)
    )
    row = result.mappings().first()

    if not row:
        # Create default settings
        await db.execute(
            text("""
                INSERT INTO "appSettings" (id, "trimestreActivo", "trimestreSiguiente", "updatedAt")
                VALUES (1, '2026-Q2', NULL, NOW())
            """)
        )
        await db.commit()
        return AppSettingsOut(
            trimestre_activo="2026-Q2",
            trimestre_siguiente=None,
        )

    return AppSettingsOut(
        trimestre_activo=row["trimestreActivo"],
        trimestre_siguiente=row["trimestreSiguiente"],
    )


@router.put("/", response_model=AppSettingsOut)
async def actualizar_settings(
    body: AppSettingsUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Update application settings.
    Only updates fields that are provided (not None).
    """
    updates = []
    params = {}

    if body.trimestre_activo is not None:
        # Validate format YYYY-Q[1-4]
        if not _validar_trimestre(body.trimestre_activo):
            raise HTTPException(status_code=400, detail="Formato de trimestre inválido")
        updates.append('"trimestreActivo" = :activo')
        params["activo"] = body.trimestre_activo

    if body.trimestre_siguiente is not None:
        if body.trimestre_siguiente == "":
            # Allow clearing siguiente
            updates.append('"trimestreSiguiente" = NULL')
        else:
            if not _validar_trimestre(body.trimestre_siguiente):
                raise HTTPException(status_code=400, detail="Formato de trimestre inválido")
            updates.append('"trimestreSiguiente" = :siguiente')
            params["siguiente"] = body.trimestre_siguiente

    if not updates:
        raise HTTPException(status_code=400, detail="No hay campos para actualizar")

    updates.append('"updatedAt" = NOW()')
    query = f'UPDATE "appSettings" SET {", ".join(updates)} WHERE id = 1'
    await db.execute(text(query), params)
    await db.commit()

    # Return updated settings
    return await obtener_settings(db)


@router.post("/promover", response_model=PromoverResult)
async def promover_trimestre(db: AsyncSession = Depends(get_db)):
    """
    Promote: trimestreSiguiente becomes trimestreActivo, trimestreSiguiente is cleared.

    Used after closing a quarter to move to the next one.
    Validates that trimestreSiguiente exists before promoting.
    """
    # Get current settings
    result = await db.execute(
        text("""
            SELECT "trimestreActivo", "trimestreSiguiente"
            FROM "appSettings"
            WHERE id = 1
        """)
    )
    row = result.mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Settings not found")

    siguiente = row["trimestreSiguiente"]
    if not siguiente:
        raise HTTPException(
            status_code=400,
            detail="No hay trimestre siguiente configurado para promover"
        )

    # Promote: siguiente → activo, clear siguiente
    await db.execute(
        text("""
            UPDATE "appSettings"
            SET "trimestreActivo" = :siguiente,
                "trimestreSiguiente" = NULL,
                "updatedAt" = NOW()
            WHERE id = 1
        """),
        {"siguiente": siguiente},
    )
    await db.commit()

    return PromoverResult(
        success=True,
        message=f"Trimestre {siguiente} es ahora el trimestre activo",
        settings=AppSettingsOut(
            trimestre_activo=siguiente,
            trimestre_siguiente=None,
        ),
    )


@router.get("/status", response_model=PlanningStatusOut)
async def get_planning_status(db: AsyncSession = Depends(get_db)):
    """
    Returns the current planning status for UI decision-making.

    This helps the frontend decide:
    - Which trimestre to show in Frecuencias/Calendario pages
    - Whether to show "go to Fase 1/2" messages in Operacion
    """
    # Get current settings
    result = await db.execute(
        text("""
            SELECT "trimestreActivo", "trimestreSiguiente"
            FROM "appSettings"
            WHERE id = 1
        """)
    )
    row = result.mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Settings not configured")

    activo = row["trimestreActivo"]
    siguiente = row["trimestreSiguiente"]

    # Check frecuencias for activo
    freq_activo_result = await db.execute(
        text("SELECT COUNT(*) FROM frecuencia WHERE trimestre = :tri"),
        {"tri": activo}
    )
    has_freq_activo = (freq_activo_result.scalar() or 0) > 0

    # Check planificacion for activo
    cal_activo_result = await db.execute(
        text("SELECT COUNT(*) FROM planificacion WHERE trimestre = :tri"),
        {"tri": activo}
    )
    has_cal_activo = (cal_activo_result.scalar() or 0) > 0

    # Check frecuencias for siguiente (if exists)
    has_freq_siguiente = False
    has_cal_siguiente = False
    if siguiente:
        freq_sig_result = await db.execute(
            text("SELECT COUNT(*) FROM frecuencia WHERE trimestre = :tri"),
            {"tri": siguiente}
        )
        has_freq_siguiente = (freq_sig_result.scalar() or 0) > 0

        cal_sig_result = await db.execute(
            text("SELECT COUNT(*) FROM planificacion WHERE trimestre = :tri"),
            {"tri": siguiente}
        )
        has_cal_siguiente = (cal_sig_result.scalar() or 0) > 0

    # Decision logic:
    # If activo has no frecuencias or no calendario, it needs planning first
    activo_necesita = not has_freq_activo or not has_cal_activo

    # Which trimestre to plan?
    if activo_necesita:
        trimestre_a_planificar = activo
    else:
        trimestre_a_planificar = siguiente  # May be None

    return PlanningStatusOut(
        trimestre_activo=activo,
        trimestre_siguiente=siguiente,
        activo_tiene_frecuencias=has_freq_activo,
        activo_tiene_calendario=has_cal_activo,
        siguiente_tiene_frecuencias=has_freq_siguiente,
        siguiente_tiene_calendario=has_cal_siguiente,
        trimestre_a_planificar=trimestre_a_planificar,
        activo_necesita_planificacion=activo_necesita,
    )


# ── Helpers ──────────────────────────────────────────────────

def _validar_trimestre(trimestre: str) -> bool:
    """Validate trimestre format: YYYY-Q[1-4]"""
    import re
    return bool(re.match(r"^\d{4}-Q[1-4]$", trimestre))
