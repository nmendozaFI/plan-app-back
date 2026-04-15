"""
Auto-calculation of company scores based on historical performance.

Score V3 formula (0-100):
  40% — Tasa de cumplimiento (did the company fulfill its assigned slots?)
  30% — Cumplimiento de frecuencia (actual talleres done vs assigned)
  20% — Estabilidad de voluntarios (fixed at 50 for now — future: manual input)
  10% — Antigüedad (number of quarters in the system, capped at 8 = max score)

Semáforo:
  >= 70 → VERDE
  >= 40 → AMBAR
  < 40  → ROJO

fiabilidadReciente:
  Weighted average of last 2 quarters (most recent = 70%, previous = 30%).
  Based on tasa_cumplimiento only.
"""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


async def calcular_scores_trimestre(
    db: AsyncSession,
    trimestre: str,
    warnings: list[str],
) -> dict:
    """
    Calculates and updates scoreV3, semaforo, and fiabilidadReciente
    for all companies based on historicoTaller data.

    Call this AFTER copying planificacion → historicoTaller during quarter close.

    Returns: { empresas_actualizadas: int, detalle: [...] }
    """

    # ── 1. Get all closed quarters ordered chronologically ──────
    trim_result = await db.execute(
        text("""
            SELECT DISTINCT trimestre FROM "historicoTaller"
            WHERE trimestre IS NOT NULL
            ORDER BY trimestre DESC
        """)
    )
    trimestres_cerrados = [r[0] for r in trim_result.fetchall()]

    if not trimestres_cerrados:
        warnings.append("No hay trimestres cerrados en historicoTaller — scores no calculados")
        return {"empresas_actualizadas": 0, "detalle": []}

    # Most recent and previous quarter for fiabilidadReciente
    trimestre_actual = trimestres_cerrados[0] if trimestres_cerrados else None
    trimestre_anterior = trimestres_cerrados[1] if len(trimestres_cerrados) > 1 else None

    # ── 2. Calculate per-company metrics across ALL quarters ────
    # Main query: per company, per quarter stats
    stats_result = await db.execute(
        text("""
            SELECT
                h."empresaIdOriginal" AS empresa_id_original,
                h."empresaId" AS empresa_id_final,
                h.trimestre,
                h.estado,
                h."motivoCambio" AS motivo_cambio
            FROM "historicoTaller" h
            WHERE h."empresaIdOriginal" IS NOT NULL
               OR h."empresaId" IS NOT NULL
            ORDER BY h.trimestre, h."empresaIdOriginal"
        """)
    )
    all_rows = [dict(r) for r in stats_result.mappings().all()]

    # ── 3. Aggregate per company ────────────────────────────────
    # Structure: empresa_id → { trimestres_activos, assigned, fulfilled, cancelled_by_empresa, ... }
    empresa_stats: dict[int, dict] = {}

    for row in all_rows:
        eid_orig = row["empresa_id_original"]
        eid_final = row["empresa_id_final"]
        estado = row["estado"]
        motivo = row["motivo_cambio"]
        tri = row["trimestre"]

        # Track original assignments (what solver planned)
        if eid_orig is not None:
            if eid_orig not in empresa_stats:
                empresa_stats[eid_orig] = {
                    "trimestres": set(),
                    "total_asignado": 0,       # Slots assigned by solver
                    "cumplidos": 0,             # Slots fulfilled (same company, OK)
                    "cancelados_empresa": 0,    # Company cancelled (hurts score)
                    "cambios_planificador": 0,  # Planner moved (doesn't hurt)
                    "cancelados_sin_motivo": 0, # Legacy: no motivoCambio
                    # Per-quarter breakdown for fiabilidadReciente
                    "por_trimestre": {},
                }

            stats = empresa_stats[eid_orig]
            stats["trimestres"].add(tri)
            stats["total_asignado"] += 1

            # Initialize per-quarter
            if tri not in stats["por_trimestre"]:
                stats["por_trimestre"][tri] = {"asignado": 0, "cumplido": 0}
            stats["por_trimestre"][tri]["asignado"] += 1

            # Classify outcome
            if eid_final == eid_orig and estado == "OK":
                stats["cumplidos"] += 1
                stats["por_trimestre"][tri]["cumplido"] += 1
            elif eid_final == eid_orig and estado == "CANCELADO":
                # Company was assigned and the slot was cancelled (company's fault)
                stats["cancelados_empresa"] += 1
            elif eid_final != eid_orig:
                # Company was substituted
                if motivo == "DECISION_PLANIFICADOR":
                    stats["cambios_planificador"] += 1
                    # Don't penalize — planner chose to move them
                    # Still count as "not fulfilled" for frequency, but not for reliability
                elif motivo == "EMPRESA_CANCELO":
                    stats["cancelados_empresa"] += 1
                else:
                    # Legacy data without motivoCambio — assume empresa cancelled
                    stats["cancelados_sin_motivo"] += 1
                    stats["cancelados_empresa"] += 1

        # Track extras (company stepped in as substitute)
        if eid_final is not None and eid_final != eid_orig:
            if eid_final not in empresa_stats:
                empresa_stats[eid_final] = {
                    "trimestres": set(),
                    "total_asignado": 0,
                    "cumplidos": 0,
                    "cancelados_empresa": 0,
                    "cambios_planificador": 0,
                    "cancelados_sin_motivo": 0,
                    "por_trimestre": {},
                }
            empresa_stats[eid_final]["trimestres"].add(tri)
            # Extras count positively — company was reliable enough to sub in

    # ── 4. Calculate scores ─────────────────────────────────────

    detalle = []
    empresas_actualizadas = 0

    # Get all active empresas
    emp_result = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE activa = true")
    )
    empresas_activas = {r["id"]: r["nombre"] for r in emp_result.mappings().all()}

    for eid, nombre in empresas_activas.items():
        stats = empresa_stats.get(eid)

        if not stats or stats["total_asignado"] == 0:
            # No history — set neutral scores
            score_v3 = 50.0  # Neutral
            semaforo = "AMBAR"
            fiabilidad = 50.0
        else:
            # ── Component 1: Tasa de cumplimiento (40%) ──
            # How many of their assigned slots did they actually do?
            # "cancelados_empresa" penalizes, "cambios_planificador" does NOT
            penalized = stats["cancelados_empresa"]
            total = stats["total_asignado"]
            # Effective fulfillment = (total - penalized) / total
            # Note: cambios_planificador are NOT penalized
            tasa_cumplimiento = max(0, (total - penalized) / total) * 100

            # ── Component 2: Cumplimiento de frecuencia (30%) ──
            # How many slots actually ended OK with this company vs total assigned
            if total > 0:
                cumplimiento_freq = (stats["cumplidos"] / total) * 100
            else:
                cumplimiento_freq = 50.0

            # ── Component 3: Estabilidad voluntarios (20%) ──
            # Fixed at 50 for now (future: manual input or volunteer tracking)
            estabilidad = 50.0

            # ── Component 4: Antigüedad (10%) ──
            # Number of quarters active, capped at 8 (= 100 score)
            num_trimestres = len(stats["trimestres"])
            antiguedad = min(num_trimestres / 8.0, 1.0) * 100

            # ── Final score ──
            score_v3 = (
                0.40 * tasa_cumplimiento +
                0.30 * cumplimiento_freq +
                0.20 * estabilidad +
                0.10 * antiguedad
            )
            score_v3 = round(min(100, max(0, score_v3)), 1)

            # ── Semáforo ──
            if score_v3 >= 70:
                semaforo = "VERDE"
            elif score_v3 >= 40:
                semaforo = "AMBAR"
            else:
                semaforo = "ROJO"

            # ── Fiabilidad reciente (last 2 quarters) ──
            tri_data = stats["por_trimestre"]

            if trimestre_actual and trimestre_actual in tri_data:
                q_actual = tri_data[trimestre_actual]
                tasa_actual = (q_actual["cumplido"] / q_actual["asignado"] * 100) if q_actual["asignado"] > 0 else 50
            else:
                tasa_actual = 50

            if trimestre_anterior and trimestre_anterior in tri_data:
                q_anterior = tri_data[trimestre_anterior]
                tasa_anterior = (q_anterior["cumplido"] / q_anterior["asignado"] * 100) if q_anterior["asignado"] > 0 else 50
            else:
                tasa_anterior = tasa_actual  # No previous quarter — use current

            # Weighted: 70% most recent, 30% previous
            fiabilidad = round(0.7 * tasa_actual + 0.3 * tasa_anterior, 1)

        # ── 5. Update empresa in DB ─────────────────────────
        # Using text() with explicit cast for the enum
        await db.execute(
            text("""
                UPDATE empresa
                SET "scoreV3" = :score,
                    semaforo = CAST(:semaforo AS "Semaforo"),
                    "fiabilidadReciente" = :fiabilidad
                WHERE id = :eid
            """),
            {
                "score": score_v3,
                "semaforo": semaforo,
                "fiabilidad": fiabilidad,
                "eid": eid,
            },
        )
        empresas_actualizadas += 1

        detalle.append({
            "empresa_id": eid,
            "empresa_nombre": nombre,
            "score_v3": score_v3,
            "semaforo": semaforo,
            "fiabilidad_reciente": fiabilidad,
            "total_asignado": stats["total_asignado"] if stats else 0,
            "cumplidos": stats["cumplidos"] if stats else 0,
            "cancelados_empresa": stats["cancelados_empresa"] if stats else 0,
        })

    return {
        "empresas_actualizadas": empresas_actualizadas,
        "detalle": detalle,
    }
