"""
Tests for Frecuencias (Phase 1) endpoints.
"""

import pytest
from sqlalchemy import text
from .conftest import TEST_TRIMESTRE, setup_test_config_trimestral


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
