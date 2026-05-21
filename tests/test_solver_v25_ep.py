"""V25 Cambio C (Capa 5): regresión del comportamiento EP del solver.

Verifica que `CT.escuelaPropia=true` concentra los talleres de la empresa en
UNA sola semana (caso canónico INDRA cuando es EP, V25 sec. 4 y 7.5 Caso 1).

Antes de V25 el solver usaba la heurística `totalAsignado >= 6` para decidir
H6 (`max_per_week=20`). El bug era doble: empresas EP con freq < 6 no
concentraban; y empresas no-EP con freq >= 6 podían acumular varios en una
semana sin pretenderlo. Ahora el flag CT.escuelaPropia es la única fuente.

Cleanup manual (no se apoya en `_cleanup_test_data` del conftest, que limpia
solo `TEST_TRIMESTRE='2099-Q1'`). Trimestre artificial `2099-Q1` para
aislamiento total.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from .conftest import TEST_EMPRESA_PREFIX


SOLVER_TEST_TRIMESTRE = "2099-Q1"


async def _cleanup_solver_test(db, trimestre: str = SOLVER_TEST_TRIMESTRE) -> None:
    """Borra todo el rastro del test (planificacion + frecuencia + CT)
    en el trimestre artificial. Idempotente."""
    for sql in (
        'DELETE FROM planificacion WHERE trimestre = :tri',
        'DELETE FROM frecuencia    WHERE trimestre = :tri',
        'DELETE FROM "configTrimestral" WHERE trimestre = :tri',
        'DELETE FROM "solverLog"   WHERE trimestre = :tri',
    ):
        try:
            await db.execute(text(sql), {"tri": trimestre})
        except Exception:
            pass
    await db.commit()


async def _setup_empresa_freq10(
    db,
    nombre: str,
    escuela_propia: bool,
) -> int:
    """Crea empresa + CT + frecuencia con EF=10, IT=0 en SOLVER_TEST_TRIMESTRE.

    Empresa: TEST_EMPRESA_PREFIX + nombre, activa, esNueva=false (evita H7 que
    excluye nuevas de semanas 1-4 — distorsiona el assert de concentración).
    CT: escuelaPropia según parámetro, frecuenciaEF=10, frecuenciaIT=0,
    disponibilidadDias L,M,X,J,V.
    Frecuencia: talleresEF=10, talleresIT=0, totalAsignado=10.
    """
    full_name = f"{TEST_EMPRESA_PREFIX}{nombre}"

    # 1. empresa
    res = await db.execute(
        text(
            'INSERT INTO empresa '
            "(nombre, tipo, semaforo, activa, \"esNueva\", \"updatedAt\") "
            "VALUES (:n, 'AMBAS', 'VERDE', true, false, NOW()) "
            'ON CONFLICT (nombre) DO UPDATE SET activa = true, "esNueva" = false '
            "RETURNING id"
        ),
        {"n": full_name},
    )
    eid = res.scalar()

    # 2. configTrimestral
    await db.execute(
        text(
            'INSERT INTO "configTrimestral" '
            '("empresaId", trimestre, "tipoParticipacion", "escuelaPropia", '
            '"frecuenciaEF", "frecuenciaIT", "disponibilidadDias", "updatedAt") '
            "VALUES (:eid, :tri, 'AMBAS', :ep, 10, 0, 'L,M,X,J,V', NOW()) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE SET '
            '"escuelaPropia" = EXCLUDED."escuelaPropia", '
            '"frecuenciaEF" = 10, "frecuenciaIT" = 0'
        ),
        {"eid": eid, "tri": SOLVER_TEST_TRIMESTRE, "ep": escuela_propia},
    )

    # 3. frecuencia (input directo al solver — bypass del cálculo)
    cfg = await db.execute(
        text(
            'SELECT id FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": eid, "tri": SOLVER_TEST_TRIMESTRE},
    )
    config_id = cfg.scalar()

    await db.execute(
        text(
            "INSERT INTO frecuencia "
            '("configId", "empresaId", trimestre, "talleresEF", "talleresIT", '
            '"totalAsignado", "semaforoCalculado", "scoreCalculado", "esNueva") '
            "VALUES (:cfg, :eid, :tri, 10, 0, 10, 'VERDE', 80.0, false) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE SET '
            '"talleresEF" = 10, "talleresIT" = 0, "totalAsignado" = 10'
        ),
        {"cfg": config_id, "eid": eid, "tri": SOLVER_TEST_TRIMESTRE},
    )

    await db.commit()
    return eid


@pytest.mark.asyncio
async def test_solver_v25_ep_concentra_10_en_una_semana(client, db_session):
    """V25 Cambio C (Capa 5, fix bug INDRA): empresa con CT.escuelaPropia=true
    y frecuencia=10 (EF) concentra los 10 talleres en UNA sola semana.

    Capa 5 implementa:
      - H6: max_per_week=20 si CT.escuelaPropia=true (reemplaza heurística >=6).
      - H6b: constraint hard de concentración EP — exactamente una semana
        usada por empresa EP. Sin este constraint las penalties S1/S2
        dispersaban aunque H6 lo permitiera (decisión C5 V25)."""
    await _cleanup_solver_test(db_session)
    try:
        eid = await _setup_empresa_freq10(
            db_session, "V25_EP_CONCENTRA", escuela_propia=True,
        )

        resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": SOLVER_TEST_TRIMESTRE, "timeout_seconds": 60},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] in ("OPTIMAL", "FEASIBLE"), (
            f"solver no produjo solución: status={data['status']}, "
            f"warnings={data.get('warnings')}"
        )

        # Cuenta asignaciones de la empresa por semana.
        per_week: dict[int, int] = {}
        for slot in data["slots"]:
            if slot.get("empresa_id") == eid:
                per_week[slot["semana"]] = per_week.get(slot["semana"], 0) + 1

        total = sum(per_week.values())
        assert total == 10, (
            f"Esperaba 10 asignaciones de la empresa EP, hay {total}: {per_week}"
        )

        # CRÍTICO: una sola semana acumula los 10.
        semanas_con_talleres = [s for s, n in per_week.items() if n > 0]
        assert len(semanas_con_talleres) == 1, (
            f"EP NO concentra: {len(semanas_con_talleres)} semanas con "
            f"talleres: {per_week}"
        )
        semana_unica = semanas_con_talleres[0]
        assert per_week[semana_unica] == 10, (
            f"Semana {semana_unica} debería tener los 10, tiene "
            f"{per_week[semana_unica]}"
        )
    finally:
        await _cleanup_solver_test(db_session)
