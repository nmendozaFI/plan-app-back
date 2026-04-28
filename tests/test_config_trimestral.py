"""V21 / F3a: tests for GET /api/config-trimestral/{trimestre}/empresas-ep.

Endpoint behavior under test:
  - Returns only empresas with configTrimestral.escuelaPropia=true AND
    empresa.activa=true for the given trimestre.
  - Sorted alphabetically by empresa.nombre ASC.
  - Trimestre with no rows → 200 with total=0 and empty list (not 404).
  - Smoke check on real 2026-Q2 data when the seeded EP IDs are present
    (skipped otherwise so the suite stays portable).
"""

import pytest
from sqlalchemy import text

from .conftest import TEST_TRIMESTRE, TEST_EMPRESA_PREFIX


# ── Helpers ────────────────────────────────────────────────────


async def _create_empresa(db, nombre: str, activa: bool = True) -> int:
    """Insert (or upsert) a test empresa. Returns its id."""
    res = await db.execute(
        text(
            'INSERT INTO empresa (nombre, tipo, activa, "updatedAt") '
            "VALUES (:n, 'AMBAS', :activa, NOW()) "
            'ON CONFLICT (nombre) DO UPDATE SET activa = EXCLUDED.activa '
            "RETURNING id"
        ),
        {"n": nombre, "activa": activa},
    )
    eid = res.scalar()
    await db.commit()
    return eid


async def _set_config_trimestral(db, empresa_id: int, escuela_propia: bool):
    """Create or update configTrimestral for (empresa, TEST_TRIMESTRE)."""
    await db.execute(
        text(
            'INSERT INTO "configTrimestral" '
            '("empresaId", trimestre, "tipoParticipacion", "escuelaPropia", '
            '"disponibilidadDias", "updatedAt") '
            "VALUES (:eid, :tri, 'AMBAS', :ep, 'L,M,X,J,V', NOW()) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE '
            'SET "escuelaPropia" = EXCLUDED."escuelaPropia"'
        ),
        {"eid": empresa_id, "tri": TEST_TRIMESTRE, "ep": escuela_propia},
    )
    await db.commit()


# ── Tests ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_listar_empresas_ep_devuelve_solo_ep_activas(client, db_session):
    """3 empresas: EP+activa, EP+inactiva, noEP+activa → solo la primera."""
    ep_activa_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}EP_F3A_OK", activa=True
    )
    ep_inactiva_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}EP_F3A_INACTIVA", activa=False
    )
    no_ep_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}EP_F3A_NOEP", activa=True
    )
    await _set_config_trimestral(db_session, ep_activa_id, escuela_propia=True)
    await _set_config_trimestral(db_session, ep_inactiva_id, escuela_propia=True)
    await _set_config_trimestral(db_session, no_ep_id, escuela_propia=False)

    resp = await client.get(f"/api/config-trimestral/{TEST_TRIMESTRE}/empresas-ep")
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["trimestre"] == TEST_TRIMESTRE
    returned_ids = [e["id"] for e in data["empresas"]]

    # Only the EP+activa empresa from this test should appear among the test ones.
    assert ep_activa_id in returned_ids
    assert ep_inactiva_id not in returned_ids
    assert no_ep_id not in returned_ids

    # Sanity on shape of the row we know is there.
    row = next(e for e in data["empresas"] if e["id"] == ep_activa_id)
    assert row["nombre"] == f"{TEST_EMPRESA_PREFIX}EP_F3A_OK"
    assert row["tipo"] == "AMBAS"
    assert row["activa"] is True

    # `total` must match the array length.
    assert data["total"] == len(data["empresas"])


@pytest.mark.asyncio
async def test_listar_empresas_ep_orden_alfabetico(client, db_session):
    """Three EP empresas inserted out of order → response sorted A, B, C."""
    # Names crafted so they sort A < B < C among themselves.
    name_b = f"{TEST_EMPRESA_PREFIX}ORDEN_B_F3A"
    name_a = f"{TEST_EMPRESA_PREFIX}ORDEN_A_F3A"
    name_c = f"{TEST_EMPRESA_PREFIX}ORDEN_C_F3A"

    # Insert in non-alphabetical order (B, A, C) to ensure ordering comes from
    # the SQL, not from insertion order.
    b_id = await _create_empresa(db_session, name_b)
    a_id = await _create_empresa(db_session, name_a)
    c_id = await _create_empresa(db_session, name_c)

    for eid in (b_id, a_id, c_id):
        await _set_config_trimestral(db_session, eid, escuela_propia=True)

    resp = await client.get(f"/api/config-trimestral/{TEST_TRIMESTRE}/empresas-ep")
    assert resp.status_code == 200, resp.text
    data = resp.json()

    test_rows = [e for e in data["empresas"] if e["nombre"].startswith(
        f"{TEST_EMPRESA_PREFIX}ORDEN_"
    )]
    test_names = [e["nombre"] for e in test_rows]
    assert test_names == [name_a, name_b, name_c]


@pytest.mark.asyncio
async def test_listar_empresas_ep_trimestre_inexistente(client):
    """Bogus trimestre → 200 with empty list (NOT 404)."""
    resp = await client.get(
        "/api/config-trimestral/TEST-INEXISTENTE-9999/empresas-ep"
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["trimestre"] == "TEST-INEXISTENTE-9999"
    assert data["total"] == 0
    assert data["empresas"] == []


@pytest.mark.asyncio
async def test_listar_empresas_ep_q2_real(client, db_session):
    """Smoke check on 2026-Q2 real data: should include the seeded 6 EP IDs.

    Skips when those IDs aren't present in this DB (e.g. local dev with
    different seed data) so the test stays portable.
    """
    expected_ids = {42, 49, 55, 60, 78, 82}

    # Confirm the seeded fixtures are actually present before asserting.
    pre = await db_session.execute(
        text(
            'SELECT e.id FROM "configTrimestral" ct '
            "JOIN empresa e ON e.id = ct.\"empresaId\" "
            "WHERE ct.trimestre = :tri "
            'AND ct."escuelaPropia" = true '
            "AND e.activa = true"
        ),
        {"tri": "2026-Q2"},
    )
    real_ids = {row["id"] for row in pre.mappings().all()}
    if not expected_ids.issubset(real_ids):
        pytest.skip(
            f"2026-Q2 EP fixtures not present in this DB "
            f"(found {sorted(real_ids)}); skipping real-data check."
        )

    resp = await client.get("/api/config-trimestral/2026-Q2/empresas-ep")
    assert resp.status_code == 200, resp.text
    data = resp.json()

    returned_ids = {e["id"] for e in data["empresas"]}
    assert expected_ids.issubset(returned_ids), (
        f"Expected {expected_ids} ⊆ returned, got {returned_ids}"
    )

    # Alphabetical order invariant on the real payload.
    nombres = [e["nombre"] for e in data["empresas"]]
    assert nombres == sorted(nombres)
