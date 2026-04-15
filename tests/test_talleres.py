"""
Tests for Taller endpoints.
"""

import pytest


@pytest.mark.asyncio
async def test_listar_talleres(client):
    """GET /api/talleres returns list of talleres."""
    response = await client.get("/api/talleres")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # Should have 20 talleres (14 EF + 6 IT)
    assert len(data) >= 20


@pytest.mark.asyncio
async def test_talleres_have_required_fields(client):
    """All talleres have required fields."""
    response = await client.get("/api/talleres")
    talleres = response.json()

    for taller in talleres:
        assert "id" in taller
        assert "nombre" in taller
        assert "programa" in taller
        assert taller["programa"] in ("EF", "IT")
        # Slot talleres should have día, horario, turno
        assert "dia_semana" in taller
        assert "horario" in taller
        assert "turno" in taller


@pytest.mark.asyncio
async def test_talleres_distribution(client):
    """Verify 14 EF + 6 IT distribution."""
    response = await client.get("/api/talleres")
    talleres = response.json()

    ef_count = sum(1 for t in talleres if t["programa"] == "EF")
    it_count = sum(1 for t in talleres if t["programa"] == "IT")

    assert ef_count == 14, f"Expected 14 EF talleres, got {ef_count}"
    assert it_count == 6, f"Expected 6 IT talleres, got {it_count}"


@pytest.mark.asyncio
async def test_talleres_filter_programa_ef(client):
    """GET /api/talleres?programa=EF returns only EF talleres."""
    response = await client.get("/api/talleres", params={"programa": "EF"})

    assert response.status_code == 200
    talleres = response.json()
    assert all(t["programa"] == "EF" for t in talleres)
    assert len(talleres) == 14


@pytest.mark.asyncio
async def test_talleres_filter_programa_it(client):
    """GET /api/talleres?programa=IT returns only IT talleres."""
    response = await client.get("/api/talleres", params={"programa": "IT"})

    assert response.status_code == 200
    talleres = response.json()
    assert all(t["programa"] == "IT" for t in talleres)
    assert len(talleres) == 6


@pytest.mark.asyncio
async def test_talleres_dias_distribucion(client):
    """Talleres are distributed across weekdays L-V."""
    response = await client.get("/api/talleres")
    talleres = response.json()

    dias = set(t["dia_semana"] for t in talleres if t["dia_semana"])
    expected_dias = {"L", "M", "X", "J", "V"}

    # All weekdays should have at least one taller
    assert dias == expected_dias, f"Expected all weekdays, got {dias}"


@pytest.mark.asyncio
async def test_obtener_taller_by_id(client):
    """GET /api/talleres/{id} returns specific taller."""
    # First get a valid taller id
    list_response = await client.get("/api/talleres")
    talleres = list_response.json()
    taller_id = talleres[0]["id"]

    response = await client.get(f"/api/talleres/{taller_id}")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == taller_id


@pytest.mark.asyncio
async def test_obtener_taller_not_found(client):
    """GET /api/talleres/{id} with invalid id returns 404."""
    response = await client.get("/api/talleres/999999")

    assert response.status_code == 404
