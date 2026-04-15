"""
Tests for health check endpoints.
"""

import pytest


@pytest.mark.asyncio
async def test_health_endpoint(client):
    """GET /health returns 200 with status info."""
    response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "service" in data


@pytest.mark.asyncio
async def test_root_endpoint(client):
    """GET / returns welcome message."""
    response = await client.get("/")

    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "Planificador" in data["message"]
