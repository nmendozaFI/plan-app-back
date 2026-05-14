"""V21 / F3a + V22 (Cambio A): tests for the config-trimestral router.

Endpoints under test:
  - GET /{trimestre}/empresas-ep              (V21): EP+activa filter.
  - GET /{trimestre}/empresas-permite-extras  (V22): permiteExtras+activa filter.
  - GET /{trimestre}                          (V22): row exposes permite_extras.
  - GET /{trimestre}/resumen                  (V22): counter `permite_extras`.
  - PUT /{trimestre}/batch                    (V22): updates permite_extras.
  - PUT /{trimestre}/{empresa_id}             (V22): updates permite_extras.
  - POST /{trimestre}/inicializar             (V22): heredera de aceptaExtras y
                                                     clona permiteExtras.
"""

import pytest
from sqlalchemy import text

from .conftest import TEST_TRIMESTRE, TEST_EMPRESA_PREFIX


# ── Helpers ────────────────────────────────────────────────────


async def _create_empresa(
    db,
    nombre: str,
    activa: bool = True,
    *,
    acepta_extras: bool = False,
) -> int:
    """Insert (or upsert) a test empresa. Returns its id.

    V22 (Cambio A): `acepta_extras` toggles the empresa-level baseline that
    `inicializar_configs` mode-defaults inherits into CT.permiteExtras.
    """
    res = await db.execute(
        text(
            'INSERT INTO empresa (nombre, tipo, activa, "aceptaExtras", "updatedAt") '
            "VALUES (:n, 'AMBAS', :activa, :ae, NOW()) "
            'ON CONFLICT (nombre) DO UPDATE '
            'SET activa = EXCLUDED.activa, "aceptaExtras" = EXCLUDED."aceptaExtras" '
            "RETURNING id"
        ),
        {"n": nombre, "activa": activa, "ae": acepta_extras},
    )
    eid = res.scalar()
    await db.commit()
    return eid


async def _set_config_trimestral(
    db,
    empresa_id: int,
    escuela_propia: bool,
    *,
    permite_extras: bool = False,
    trimestre: str = TEST_TRIMESTRE,
):
    """Create or update configTrimestral for (empresa, trimestre).

    V22 (Cambio A): added `permite_extras` kwarg (independent of escuela_propia).
    """
    await db.execute(
        text(
            'INSERT INTO "configTrimestral" '
            '("empresaId", trimestre, "tipoParticipacion", "escuelaPropia", '
            '"permiteExtras", "disponibilidadDias", "updatedAt") '
            "VALUES (:eid, :tri, 'AMBAS', :ep, :pe, 'L,M,X,J,V', NOW()) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE '
            'SET "escuelaPropia" = EXCLUDED."escuelaPropia", '
            '    "permiteExtras" = EXCLUDED."permiteExtras"'
        ),
        {
            "eid": empresa_id,
            "tri": trimestre,
            "ep": escuela_propia,
            "pe": permite_extras,
        },
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


# ── V22 (Cambio A): permiteExtras ──────────────────────────────


@pytest.mark.asyncio
async def test_listar_empresas_permite_extras_filtra_correctamente(client, db_session):
    """GET /empresas-permite-extras returns only permiteExtras=true + activa=true.

    Three empresas: PE+activa, PE+inactiva, noPE+activa → only the first appears.
    """
    pe_activa_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_OK", activa=True,
    )
    pe_inactiva_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_INACTIVA", activa=False,
    )
    no_pe_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_NOPE", activa=True,
    )
    await _set_config_trimestral(
        db_session, pe_activa_id, escuela_propia=False, permite_extras=True,
    )
    await _set_config_trimestral(
        db_session, pe_inactiva_id, escuela_propia=False, permite_extras=True,
    )
    await _set_config_trimestral(
        db_session, no_pe_id, escuela_propia=False, permite_extras=False,
    )

    resp = await client.get(
        f"/api/config-trimestral/{TEST_TRIMESTRE}/empresas-permite-extras",
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    returned_ids = [e["id"] for e in data["empresas"]]
    assert pe_activa_id in returned_ids
    assert pe_inactiva_id not in returned_ids
    assert no_pe_id not in returned_ids

    # The new endpoint is independent of escuelaPropia (we set ep=False above).
    row = next(e for e in data["empresas"] if e["id"] == pe_activa_id)
    assert row["nombre"] == f"{TEST_EMPRESA_PREFIX}PE_V22_OK"
    assert row["activa"] is True
    assert data["total"] == len(data["empresas"])


@pytest.mark.asyncio
async def test_listar_empresas_permite_extras_trimestre_inexistente(client):
    """Bogus trimestre → 200 with empty list."""
    resp = await client.get(
        "/api/config-trimestral/TEST-INEXISTENTE-V22-PE/empresas-permite-extras",
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total"] == 0
    assert data["empresas"] == []


@pytest.mark.asyncio
async def test_get_trimestre_expone_permite_extras(client, db_session):
    """GET /{trimestre} surfaces the new permite_extras field per config row."""
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_GET")
    await _set_config_trimestral(
        db_session, eid, escuela_propia=False, permite_extras=True,
    )

    resp = await client.get(f"/api/config-trimestral/{TEST_TRIMESTRE}")
    assert resp.status_code == 200, resp.text
    data = resp.json()

    row = next(c for c in data["configs"] if c["empresa_id"] == eid)
    assert row["permite_extras"] is True
    assert row["escuela_propia"] is False  # ortogonal


@pytest.mark.asyncio
async def test_resumen_incluye_contador_permite_extras(client, db_session):
    """GET /{trimestre}/resumen exposes permite_extras count."""
    a = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_R_A")
    b = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_R_B")
    c = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_R_C")
    await _set_config_trimestral(db_session, a, escuela_propia=False, permite_extras=True)
    await _set_config_trimestral(db_session, b, escuela_propia=False, permite_extras=True)
    await _set_config_trimestral(db_session, c, escuela_propia=False, permite_extras=False)

    resp = await client.get(f"/api/config-trimestral/{TEST_TRIMESTRE}/resumen")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "permite_extras" in data
    # At least our 2 fixtures with permite_extras=true should be in the count
    # (other tests may have added more, so use >=).
    assert data["permite_extras"] >= 2


@pytest.mark.asyncio
async def test_batch_update_permite_extras(client, db_session):
    """PUT /batch toggles permiteExtras."""
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_BATCH")
    await _set_config_trimestral(
        db_session, eid, escuela_propia=False, permite_extras=False,
    )

    resp = await client.put(
        f"/api/config-trimestral/{TEST_TRIMESTRE}/batch",
        json={"updates": [{"empresa_id": eid, "permite_extras": True}]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["updated"] == 1

    # DB cross-check.
    row = await db_session.execute(
        text(
            'SELECT "permiteExtras" FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": eid, "tri": TEST_TRIMESTRE},
    )
    assert row.scalar() is True


@pytest.mark.asyncio
async def test_put_individual_permite_extras(client, db_session):
    """PUT /{trimestre}/{empresa_id} toggles permiteExtras."""
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_PUT")
    await _set_config_trimestral(
        db_session, eid, escuela_propia=False, permite_extras=False,
    )

    resp = await client.put(
        f"/api/config-trimestral/{TEST_TRIMESTRE}/{eid}",
        json={"permite_extras": True},
    )
    assert resp.status_code == 200, resp.text
    config = resp.json()["config"]
    assert config["permite_extras"] is True

    # DB cross-check.
    row = await db_session.execute(
        text(
            'SELECT "permiteExtras" FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": eid, "tri": TEST_TRIMESTRE},
    )
    assert row.scalar() is True


@pytest.mark.asyncio
async def test_inicializar_defaults_hereda_acepta_extras(client, db_session):
    """POST /inicializar (modo defaults) → CT.permiteExtras = empresa.aceptaExtras.

    Uses a per-test trimestre so the test empresas have no pre-existing CT
    (inicializar only acts on empresas without a CT for the target trimestre).
    """
    new_tri = "TEST-V22-INIT-DEF"

    # Empresa A: aceptaExtras=true → expect CT.permiteExtras=true.
    a = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_INIT_A", acepta_extras=True,
    )
    # Empresa B: aceptaExtras=false → expect CT.permiteExtras=false.
    b = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_INIT_B", acepta_extras=False,
    )

    # Pre-clean the trimestre for these two empresas so inicializar acts.
    await db_session.execute(
        text(
            'DELETE FROM "configTrimestral" '
            'WHERE trimestre = :tri AND "empresaId" IN (:a, :b)'
        ),
        {"tri": new_tri, "a": a, "b": b},
    )
    await db_session.commit()

    resp = await client.post(
        f"/api/config-trimestral/{new_tri}/inicializar",
        json={},
    )
    assert resp.status_code == 200, resp.text

    row_a = await db_session.execute(
        text(
            'SELECT "permiteExtras" FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": a, "tri": new_tri},
    )
    row_b = await db_session.execute(
        text(
            'SELECT "permiteExtras" FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": b, "tri": new_tri},
    )
    assert row_a.scalar() is True
    assert row_b.scalar() is False

    # Clean up the spawned CTs so the trimestre stays disposable.
    await db_session.execute(
        text('DELETE FROM "configTrimestral" WHERE trimestre = :tri'),
        {"tri": new_tri},
    )
    await db_session.commit()


@pytest.mark.asyncio
async def test_inicializar_clonar_copia_permite_extras(client, db_session):
    """POST /inicializar with origen_trimestre clones permite_extras 1:1."""
    origen_tri = "TEST-V22-INIT-ORIG"
    destino_tri = "TEST-V22-INIT-DEST"

    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}PE_V22_CLONE")
    # Seed origen with permite_extras=true.
    await _set_config_trimestral(
        db_session, eid, escuela_propia=False, permite_extras=True,
        trimestre=origen_tri,
    )

    # Pre-clean destino just in case.
    await db_session.execute(
        text('DELETE FROM "configTrimestral" WHERE trimestre = :tri'),
        {"tri": destino_tri},
    )
    await db_session.commit()

    resp = await client.post(
        f"/api/config-trimestral/{destino_tri}/inicializar",
        json={"origen_trimestre": origen_tri},
    )
    assert resp.status_code == 200, resp.text

    row = await db_session.execute(
        text(
            'SELECT "permiteExtras" FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": eid, "tri": destino_tri},
    )
    assert row.scalar() is True

    # Cleanup.
    await db_session.execute(
        text(
            'DELETE FROM "configTrimestral" WHERE trimestre IN (:o, :d)'
        ),
        {"o": origen_tri, "d": destino_tri},
    )
    await db_session.commit()
