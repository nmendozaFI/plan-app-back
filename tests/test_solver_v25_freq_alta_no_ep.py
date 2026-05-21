"""V25 Cambio C (Capa 5): regresión del Caso 2 — empresa con freq alta NO-EP.

Caso documentado en V25 sec. 7.5 Caso 2:
  - Empresa con `CT.escuelaPropia=false` y frecuencia alta (ej. 10).
  - Comportamiento esperado: distribuir aprox. 1 taller por semana.
  - Bug antes de Capa 5: la heurística `total>=6` activaba H6 max_per_week=20
    aunque la empresa no fuera EP — el solver podía meter varios en la misma
    semana sin pretenderlo (origen del bug INDRA).
  - Fix Capa 5: H6 lee `escuela_propia_map` (CT.escuelaPropia), no `total`.

Trimestre artificial `2099-Q1` para aislamiento. Cleanup manual.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from .conftest import TEST_EMPRESA_PREFIX
from .test_solver_v25_ep import (
    SOLVER_TEST_TRIMESTRE,
    _cleanup_solver_test,
    _setup_empresa_freq10,
)


@pytest.mark.asyncio
async def test_solver_v25_freq_alta_no_ep_distribuye(client, db_session):
    """Empresa freq=10 con CT.escuelaPropia=false → max 1 taller/semana.

    Verifica el fix del bug INDRA: el solver respeta H6 (1 por semana) cuando
    la empresa NO es EP, sin importar que la frecuencia sea alta.

    NO se afirma "exactamente 10 semanas con 1 cada una" porque el solver
    podría dejar slots vacantes (capacidad limitada del calendario, restricciones
    de días, etc.). La afirmación es la regla dura: ninguna semana > 1.
    """
    await _cleanup_solver_test(db_session)
    try:
        eid = await _setup_empresa_freq10(
            db_session, "V25_NOEP_DISPERSA", escuela_propia=False,
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
            f"Esperaba 10 asignaciones de la empresa no-EP, hay {total}: "
            f"{per_week}"
        )

        # CRÍTICO (regresión bug INDRA): ninguna semana acumula >1.
        violaciones = {s: n for s, n in per_week.items() if n > 1}
        assert violaciones == {}, (
            "Bug INDRA: empresa freq=10 no-EP tiene semanas con >1 taller: "
            f"{violaciones}"
        )

        # Sanity: las 10 asignaciones caen en 10 semanas distintas (1 por
        # semana). Aceptamos cualquier subset de las 13 semanas.
        assert len(per_week) == 10, (
            f"Esperaba 10 semanas distintas con 1 taller cada una, hubo "
            f"{len(per_week)}: {per_week}"
        )
    finally:
        await _cleanup_solver_test(db_session)
