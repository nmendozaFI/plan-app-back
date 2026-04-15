"""
Tests for Empresa CRUD endpoints.
"""

import pytest
from .conftest import TEST_EMPRESA_PREFIX


@pytest.mark.asyncio
async def test_listar_empresas(client):
    """GET /api/empresas returns list of empresas."""
    response = await client.get("/api/empresas/")

    assert response.status_code == 200
    data = response.json()
    assert "empresas" in data
    assert isinstance(data["empresas"], list)
    # Should have some empresas in the database
    assert len(data["empresas"]) > 0


@pytest.mark.asyncio
async def test_listar_empresas_filter_activa(client):
    """GET /api/empresas?activa=true returns only active empresas."""
    response = await client.get("/api/empresas/", params={"activa": True})

    assert response.status_code == 200
    data = response.json()
    for emp in data["empresas"]:
        assert emp["activa"] is True


@pytest.mark.asyncio
async def test_listar_empresas_filter_tipo(client):
    """GET /api/empresas?tipo=EF returns only EF empresas."""
    response = await client.get("/api/empresas/", params={"tipo": "EF"})

    assert response.status_code == 200
    data = response.json()
    for emp in data["empresas"]:
        assert emp["tipo"] == "EF"


@pytest.mark.asyncio
async def test_detalle_empresa(client):
    """GET /api/empresas/{id} returns empresa detail."""
    # First get a valid empresa id
    list_response = await client.get("/api/empresas/", params={"activa": True})
    empresas = list_response.json()["empresas"]
    assert len(empresas) > 0

    empresa_id = empresas[0]["id"]
    response = await client.get(f"/api/empresas/{empresa_id}")

    assert response.status_code == 200
    data = response.json()
    assert "empresa" in data
    assert data["empresa"]["id"] == empresa_id
    # Should include restricciones and ciudades
    assert "restricciones" in data
    assert "ciudades" in data


@pytest.mark.asyncio
async def test_detalle_empresa_not_found(client):
    """GET /api/empresas/{id} with invalid id returns 404."""
    response = await client.get("/api/empresas/999999")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_crear_empresa(client, db_session):
    """POST /api/empresas creates a new empresa."""
    empresa_data = {
        "nombre": f"{TEST_EMPRESA_PREFIX}CREATE_TEST",
        "tipo": "EF",
        "semaforo": "VERDE",
        "scoreV3": 85.0,
        "esComodin": False,
        "prioridadReduccion": "MEDIA"
    }

    response = await client.post("/api/empresas/", json=empresa_data)

    assert response.status_code == 200
    data = response.json()
    assert "empresa" in data
    assert data["empresa"]["nombre"] == empresa_data["nombre"]
    assert data["empresa"]["tipo"] == "EF"
    # Cleanup handled by autouse fixture


@pytest.mark.asyncio
async def test_crear_empresa_nombre_duplicado(client, db_session):
    """POST /api/empresas with duplicate nombre fails."""
    empresa_data = {
        "nombre": f"{TEST_EMPRESA_PREFIX}DUPLICATE_TEST",
        "tipo": "EF"
    }

    # Create first
    response1 = await client.post("/api/empresas/", json=empresa_data)
    assert response1.status_code == 200

    # Try to create duplicate
    response2 = await client.post("/api/empresas/", json=empresa_data)
    # Should fail (either 400 or 500 depending on how it's handled)
    assert response2.status_code in (400, 409, 500)


@pytest.mark.asyncio
async def test_actualizar_empresa(client, db_session):
    """PUT /api/empresas/{id} updates an empresa."""
    # Create test empresa first
    create_data = {"nombre": f"{TEST_EMPRESA_PREFIX}UPDATE_TEST", "tipo": "EF"}
    create_response = await client.post("/api/empresas/", json=create_data)
    empresa_id = create_response.json()["empresa"]["id"]

    # Update it
    update_data = {"tipo": "IT", "semaforo": "ROJO"}
    response = await client.put(f"/api/empresas/{empresa_id}", json=update_data)

    assert response.status_code == 200
    data = response.json()
    assert data["empresa"]["tipo"] == "IT"
    assert data["empresa"]["semaforo"] == "ROJO"


@pytest.mark.asyncio
async def test_toggle_empresa(client, db_session):
    """PATCH /api/empresas/{id}/toggle toggles activa status."""
    # Create test empresa
    create_data = {"nombre": f"{TEST_EMPRESA_PREFIX}TOGGLE_TEST", "tipo": "EF"}
    create_response = await client.post("/api/empresas/", json=create_data)
    empresa = create_response.json()["empresa"]
    empresa_id = empresa["id"]
    original_status = empresa["activa"]

    # Toggle
    response = await client.patch(f"/api/empresas/{empresa_id}/toggle")

    assert response.status_code == 200
    data = response.json()
    assert data["activa"] != original_status

    # Toggle back
    response2 = await client.patch(f"/api/empresas/{empresa_id}/toggle")
    assert response2.json()["activa"] == original_status
