"""
Tests for AppSettings endpoints.
"""

import pytest


@pytest.mark.asyncio
async def test_get_settings(client):
    """GET /api/settings returns current settings."""
    response = await client.get("/api/settings/")

    assert response.status_code == 200
    data = response.json()
    assert "trimestre_activo" in data
    # trimestre_activo should match format YYYY-Q[1-4]
    assert data["trimestre_activo"] is not None
    import re
    assert re.match(r"^\d{4}-Q[1-4]$", data["trimestre_activo"])


@pytest.mark.asyncio
async def test_update_settings_trimestre_siguiente(client):
    """PUT /api/settings updates trimestre_siguiente."""
    # First get current settings to restore later
    get_response = await client.get("/api/settings/")
    original = get_response.json()

    # Update to a test value
    update_response = await client.put(
        "/api/settings/",
        json={"trimestre_siguiente": "2099-Q4"}
    )

    assert update_response.status_code == 200
    data = update_response.json()
    assert data["trimestre_siguiente"] == "2099-Q4"

    # Restore original (clear siguiente if it was null)
    if original.get("trimestre_siguiente"):
        await client.put(
            "/api/settings/",
            json={"trimestre_siguiente": original["trimestre_siguiente"]}
        )
    else:
        await client.put(
            "/api/settings/",
            json={"trimestre_siguiente": ""}
        )


@pytest.mark.asyncio
async def test_update_settings_invalid_trimestre(client):
    """PUT /api/settings with invalid format returns 400."""
    response = await client.put(
        "/api/settings/",
        json={"trimestre_activo": "invalid-format"}
    )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_promover_without_siguiente_fails(client):
    """POST /api/settings/promover fails when no siguiente configured."""
    # First ensure siguiente is null
    await client.put("/api/settings/", json={"trimestre_siguiente": ""})

    response = await client.post("/api/settings/promover")

    assert response.status_code == 400
    data = response.json()
    assert "siguiente" in data["detail"].lower()
