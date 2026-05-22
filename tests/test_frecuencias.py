"""
Tests for Frecuencias (Phase 1) endpoints.
"""

import pytest
from sqlalchemy import text
from .conftest import TEST_TRIMESTRE, TEST_EMPRESA_PREFIX, setup_test_config_trimestral

# V24 Cambio B: real-format trimestre needed because /calcular parses
# trimestre.split("-")[0] as int. TEST_TRIMESTRE="2099-Q1" fails that parser.
# Q3 2026 is fresh post-Cambio-A and has real talleres in SemanaConfig, so
# max_ef/max_it capacity is real (no spurious recortes on small test setups).
V24_TRIMESTRE_REAL = "2026-Q3"


@pytest.mark.asyncio
async def test_calcular_frecuencias_existing_trimestre(client):
    """POST /api/frecuencias/calcular with existing data returns valid response."""
    # Use an existing trimestre that has configTrimestral data
    response = await client.post(
        "/api/frecuencias/calcular",
        json={"trimestre": "2026-Q2"}
    )

    # Should return 200 even if no config (just with warnings)
    assert response.status_code == 200
    data = response.json()

    # Required fields in response
    assert "trimestre" in data
    assert "total_ef" in data
    assert "total_it" in data
    assert "max_ef" in data
    assert "max_it" in data
    assert "empresas" in data
    assert "status" in data


@pytest.mark.asyncio
async def test_calcular_frecuencias_response_structure(client, db_session):
    """Verify frecuencias response has correct structure."""
    # Setup test data
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

    response = await client.post(
        "/api/frecuencias/calcular",
        json={"trimestre": TEST_TRIMESTRE}
    )

    assert response.status_code == 200
    data = response.json()

    # Check empresa structure
    for empresa in data["empresas"]:
        assert "empresa_id" in empresa
        assert "nombre" in empresa
        assert "talleres_ef" in empresa
        assert "talleres_it" in empresa
        assert "total" in empresa
        assert "semaforo" in empresa
        assert empresa["semaforo"] in ("VERDE", "AMBAR", "ROJO")


@pytest.mark.asyncio
async def test_calcular_frecuencias_capacity_limits(client, db_session):
    """Total EF <= 14 * semanas, total IT <= 6 * semanas."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

    response = await client.post(
        "/api/frecuencias/calcular",
        json={"trimestre": TEST_TRIMESTRE, "max_ef": 14, "max_it": 6}
    )

    assert response.status_code == 200
    data = response.json()

    semanas = data.get("semanas_disponibles", 13)
    max_ef_trimestre = data.get("max_ef_trimestre", 14 * semanas)
    max_it_trimestre = data.get("max_it_trimestre", 6 * semanas)

    assert data["total_ef"] <= max_ef_trimestre, \
        f"EF {data['total_ef']} exceeds capacity {max_ef_trimestre}"
    assert data["total_it"] <= max_it_trimestre, \
        f"IT {data['total_it']} exceeds capacity {max_it_trimestre}"


@pytest.mark.asyncio
async def test_confirmar_frecuencias(client, db_session):
    """POST /api/frecuencias/confirmar persists frecuencias."""
    # First setup config and calculate
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

    calc_response = await client.post(
        "/api/frecuencias/calcular",
        json={"trimestre": TEST_TRIMESTRE}
    )
    calc_data = calc_response.json()

    # Build confirm payload from calculated data
    empresas_confirm = [
        {
            "empresa_id": e["empresa_id"],
            "talleres_ef": e["talleres_ef"],
            "talleres_it": e["talleres_it"]
        }
        for e in calc_data["empresas"]
    ]

    confirm_response = await client.post(
        "/api/frecuencias/confirmar",
        json={
            "trimestre": TEST_TRIMESTRE,
            "empresas": empresas_confirm
        }
    )

    assert confirm_response.status_code == 200
    confirm_data = confirm_response.json()
    assert confirm_data.get("confirmadas", 0) > 0 or "frecuencias_confirmadas" in confirm_data


@pytest.mark.asyncio
async def test_obtener_frecuencias_confirmadas(client, db_session):
    """GET /api/frecuencias/{trimestre} returns confirmed frecuencias."""
    # Setup and confirm frecuencias
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

    calc_response = await client.post(
        "/api/frecuencias/calcular",
        json={"trimestre": TEST_TRIMESTRE}
    )

    empresas_confirm = [
        {
            "empresa_id": e["empresa_id"],
            "talleres_ef": e["talleres_ef"],
            "talleres_it": e["talleres_it"]
        }
        for e in calc_response.json()["empresas"]
    ]

    await client.post(
        "/api/frecuencias/confirmar",
        json={"trimestre": TEST_TRIMESTRE, "empresas": empresas_confirm}
    )

    # Now get the confirmed frecuencias
    response = await client.get(f"/api/frecuencias/{TEST_TRIMESTRE}")

    assert response.status_code == 200
    data = response.json()
    assert "trimestre" in data
    assert "frecuencias" in data or "empresas" in data


@pytest.mark.asyncio
async def test_frecuencias_empty_trimestre(client):
    """GET /api/frecuencias/{trimestre} for trimestre without data."""
    response = await client.get("/api/frecuencias/NONEXISTENT-Q1")

    # Should return 200 with empty data or 404
    assert response.status_code in (200, 404)


# ── V24 Cambio B: matriz semáforo skip-when-NULL + redistribución ──
#
# Estos tests usan V24_TRIMESTRE_REAL (=2026-Q3, real-format) en lugar de
# TEST_TRIMESTRE porque /calcular hace int(trimestre.split("-")[0]). Crean
# TEST_EMPRESA_* dedicados con sede en MADRID y los enlazan a su propia CT en
# Q3. El cleanup de conftest (DELETE FROM empresa WHERE nombre LIKE
# TEST_EMPRESA_%) hace CASCADE a configTrimestral, frecuencia y
# empresaCiudad → no deja residuo en Q3.


async def _create_test_empresa_madrid(db, nombre: str) -> int:
    """Insert TEST_EMPRESA_* + link to MADRID via empresaCiudad."""
    res = await db.execute(
        text(
            'INSERT INTO empresa (nombre, tipo, activa, "updatedAt") '
            "VALUES (:n, 'AMBAS', true, NOW()) "
            'ON CONFLICT (nombre) DO UPDATE SET activa = true '
            "RETURNING id"
        ),
        {"n": nombre},
    )
    eid = res.scalar()
    # Link to MADRID (required by /calcular filter).
    madrid = await db.execute(
        text("SELECT id FROM ciudad WHERE UPPER(nombre) = 'MADRID' LIMIT 1")
    )
    madrid_id = madrid.scalar()
    if madrid_id is not None:
        await db.execute(
            text(
                'INSERT INTO "empresaCiudad" ("empresaId", "ciudadId", "activaReciente") '
                'VALUES (:eid, :cid, true) '
                'ON CONFLICT ("empresaId", "ciudadId") DO UPDATE SET "activaReciente" = true'
            ),
            {"eid": eid, "cid": madrid_id},
        )
    await db.commit()
    return eid


async def _upsert_ct_v24(
    db,
    eid: int,
    *,
    freq_ef,
    freq_it,
    tipo: str = "AMBAS",
    trimestre: str = V24_TRIMESTRE_REAL,
):
    """Upsert CT for the V24 trimestre with explicit freq EF/IT + tipo."""
    await db.execute(
        text("""
            INSERT INTO "configTrimestral" (
                "empresaId", trimestre, "tipoParticipacion",
                "frecuenciaEF", "frecuenciaIT",
                "disponibilidadDias", "updatedAt"
            ) VALUES (:eid, :tri, :tipo, :ef, :it, 'L,M,X,J,V', NOW())
            ON CONFLICT ("empresaId", trimestre) DO UPDATE SET
                "tipoParticipacion" = EXCLUDED."tipoParticipacion",
                "frecuenciaEF" = EXCLUDED."frecuenciaEF",
                "frecuenciaIT" = EXCLUDED."frecuenciaIT"
        """),
        {"eid": eid, "tri": trimestre, "tipo": tipo, "ef": freq_ef, "it": freq_it},
    )
    await db.commit()


@pytest.mark.asyncio
async def test_calcular_incluye_empresas_con_freq_null(client, db_session):
    """V26: la pantalla Fase 1 muestra TODAS las empresas activas con CT,
    incluso si freqEF o freqIT son NULL. Los campos NULL se sugieren como 0
    con el flag `sugerido_*=true` para que el wizard pinte el badge.

    Reemplaza el comportamiento V25 Capa 8 ("alguno NULL → omit del cálculo")
    que dejaba huérfanas a las empresas sin decisión todavía. El solver y
    `confirmar_frecuencias` no cambian: siguen omitiendo solo cuando ambos
    son 0 (D5 V24).
    """
    eid_explicit = await _create_test_empresa_madrid(
        db_session, f"{TEST_EMPRESA_PREFIX}V26_EXPLICIT"
    )
    eid_one_null = await _create_test_empresa_madrid(
        db_session, f"{TEST_EMPRESA_PREFIX}V26_ONE_NULL"
    )
    eid_both_null = await _create_test_empresa_madrid(
        db_session, f"{TEST_EMPRESA_PREFIX}V26_BOTH_NULL"
    )
    await _upsert_ct_v24(db_session, eid_explicit, freq_ef=2, freq_it=1)
    await _upsert_ct_v24(db_session, eid_one_null, freq_ef=None, freq_it=1)
    await _upsert_ct_v24(db_session, eid_both_null, freq_ef=None, freq_it=None)

    resp = await client.post(
        "/api/frecuencias/calcular",
        json={"trimestre": V24_TRIMESTRE_REAL},
    )
    assert resp.status_code == 200, resp.text
    empresas = resp.json()["empresas"]
    by_id = {e["empresa_id"]: e for e in empresas}

    # Las tres empresas aparecen en el preview (lo central de V26).
    assert eid_explicit in by_id, "empresa con freq explícita debe entrar"
    assert eid_one_null in by_id, "empresa con un NULL debe entrar (V26)"
    assert eid_both_null in by_id, "empresa con ambos NULL debe entrar (V26)"

    # Flags `sugerido_*` reflejan el estado del CT (no del recorte). El
    # recorte automático puede bajar `talleres_ef`/`talleres_it` cuando el
    # trimestre real excede capacidad — la promesa V26 es el flag, no el
    # valor exacto.
    e1 = by_id[eid_explicit]
    assert e1["sugerido_ef"] is False
    assert e1["sugerido_it"] is False

    e2 = by_id[eid_one_null]
    assert e2["sugerido_ef"] is True, "EF venía NULL → sugerido"
    assert e2["sugerido_it"] is False, "IT venía explícito (1)"

    e3 = by_id[eid_both_null]
    assert e3["sugerido_ef"] is True
    assert e3["sugerido_it"] is True
    # Con ambos sugeridos no se aplica el invariante "mínimo 1": queda 0/0.
    # El recorte no la toca (total bruto = 0, no contribuye al exceso).
    assert e3["talleres_ef"] == 0
    assert e3["talleres_it"] == 0


@pytest.mark.asyncio
async def test_confirmar_uno_cero_explicito_inserta(client, db_session):
    """V26: confirmar con EF=3, IT=0 explícitos → INSERT con esos valores
    (NO se omite por D5). La omisión D5 V24 dispara SOLO con AMBOS en 0.
    """
    eid = await _create_test_empresa_madrid(
        db_session, f"{TEST_EMPRESA_PREFIX}V26_CONFIRM_ONE_ZERO"
    )
    await _upsert_ct_v24(db_session, eid, freq_ef=None, freq_it=None)

    body = {
        "trimestre": V24_TRIMESTRE_REAL,
        "empresas": [
            {"empresa_id": eid, "talleres_ef": 3, "talleres_it": 0},
        ],
    }
    resp = await client.post("/api/frecuencias/confirmar", json=body)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["empresas_omitidas"] == 0, (
        f"EF=3 IT=0 explícito no debe contar como omitido: {data}"
    )

    # BD cross-check: fila insertada con (3, 0, 3).
    row = await db_session.execute(
        text(
            'SELECT "talleresEF", "talleresIT", "totalAsignado" '
            'FROM frecuencia '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": eid, "tri": V24_TRIMESTRE_REAL},
    )
    rec = row.mappings().first()
    assert rec is not None, "debió insertarse fila en `frecuencia`"
    assert rec["talleresEF"] == 3
    assert rec["talleresIT"] == 0
    assert rec["totalAsignado"] == 3


@pytest.mark.asyncio
async def test_e2e_freq_efit_persistido_pasa_a_frecuencia(client, db_session):
    """V24 Cambio B B6 — validación E2E: CT con freq_ef=3 freq_it=2 → matriz
    semáforo → confirmar → tabla `frecuencia` muestra talleresEF=3 talleresIT=2.

    El último paso del plan ("solver genera plan exacto") queda cubierto por
    los constraints H2 (sum(EF) == talleresEF) y H3 (sum(IT) == talleresIT) en
    solver.py — no se replica aquí para mantener el test rápido.
    """
    eid = await _create_test_empresa_madrid(
        db_session, f"{TEST_EMPRESA_PREFIX}V24_E2E",
    )
    await _upsert_ct_v24(db_session, eid, freq_ef=3, freq_it=2)

    # 1. /calcular debe devolver la empresa con EF=3 e IT=2 (sin transformación
    #    algorítmica — D2/D3).
    resp = await client.post(
        "/api/frecuencias/calcular",
        json={"trimestre": V24_TRIMESTRE_REAL},
    )
    assert resp.status_code == 200, resp.text
    target = next(
        (e for e in resp.json()["empresas"] if e["empresa_id"] == eid),
        None,
    )
    assert target is not None, "la empresa con freq explícita debió entrar al cálculo"
    assert target["talleres_ef"] == 3
    assert target["talleres_it"] == 2

    # 2. /confirmar persiste en tabla `frecuencia` con los mismos valores.
    body = {
        "trimestre": V24_TRIMESTRE_REAL,
        "empresas": [
            {"empresa_id": eid, "talleres_ef": 3, "talleres_it": 2},
        ],
    }
    resp = await client.post("/api/frecuencias/confirmar", json=body)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["empresas_omitidas"] == 0
    assert data["total_ef"] >= 3 and data["total_it"] >= 2

    # 3. BD cross-check directo en la tabla `frecuencia`.
    row = await db_session.execute(
        text(
            'SELECT "talleresEF", "talleresIT", "totalAsignado" '
            'FROM frecuencia '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": eid, "tri": V24_TRIMESTRE_REAL},
    )
    rec = row.mappings().first()
    assert rec is not None, "debió insertarse fila en `frecuencia`"
    assert rec["talleresEF"] == 3
    assert rec["talleresIT"] == 2
    assert rec["totalAsignado"] == 5  # invariante EF+IT


@pytest.mark.asyncio
async def test_confirmar_skips_insert_when_both_zero(client, db_session):
    """V24 Cambio B decisión D5: confirmar_frecuencias NO inserta fila en tabla
    `frecuencia` para empresa con talleres_ef=0 Y talleres_it=0. Devuelve
    contador empresas_omitidas.
    """
    eid_normal = await _create_test_empresa_madrid(
        db_session, f"{TEST_EMPRESA_PREFIX}V24_CONF_NORMAL"
    )
    eid_zero = await _create_test_empresa_madrid(
        db_session, f"{TEST_EMPRESA_PREFIX}V24_CONF_ZERO"
    )
    # CT rows requeridas por la query de empresa lookup en confirmar.
    await _upsert_ct_v24(db_session, eid_normal, freq_ef=2, freq_it=1)
    await _upsert_ct_v24(db_session, eid_zero, freq_ef=None, freq_it=None)

    body = {
        "trimestre": V24_TRIMESTRE_REAL,
        "empresas": [
            {"empresa_id": eid_normal, "talleres_ef": 2, "talleres_it": 1},
            {"empresa_id": eid_zero, "talleres_ef": 0, "talleres_it": 0},
        ],
    }
    resp = await client.post("/api/frecuencias/confirmar", json=body)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["empresas_omitidas"] == 1
    persisted_ids = {e["empresa_id"] for e in data["empresas"]}
    assert eid_normal in persisted_ids
    assert eid_zero not in persisted_ids

    # Cross-check directo en BD: la fila zero NO está; la normal SÍ.
    res = await db_session.execute(
        text(
            'SELECT "empresaId" FROM frecuencia '
            'WHERE trimestre = :tri AND "empresaId" IN (:a, :b)'
        ),
        {"tri": V24_TRIMESTRE_REAL, "a": eid_normal, "b": eid_zero},
    )
    eids_in_db = {r["empresaId"] for r in res.mappings().all()}
    assert eid_normal in eids_in_db
    assert eid_zero not in eids_in_db
