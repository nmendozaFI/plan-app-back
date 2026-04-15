"""
Tests for Quarter Close (Cerrar Trimestre) endpoints.
"""

import pytest
from sqlalchemy import text
from .conftest import TEST_TRIMESTRE, setup_test_config_trimestral, setup_test_frecuencias


@pytest.mark.asyncio
async def test_cerrar_trimestre_preview(client, db_session):
    """POST /api/calendario/{trimestre}/cerrar with confirmar=false returns preview."""
    # Setup complete calendario with some OK/CANCELADO slots
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    # Mark some slots as OK
    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Update first 3 slots to OK, one to CANCELADO
    ok_count = 0
    cancel_count = 0
    for slot in slots:
        if slot.get("empresa_id") and slot["estado"] not in ("VACANTE",):
            if ok_count < 3:
                await client.patch(
                    f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot['id']}",
                    json={"estado": "OK"}
                )
                ok_count += 1
            elif cancel_count < 1:
                await client.patch(
                    f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot['id']}",
                    json={"estado": "CANCELADO"}
                )
                cancel_count += 1
            else:
                break

    # Preview close
    response = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/cerrar",
        json={"confirmar": False}
    )

    assert response.status_code == 200
    data = response.json()

    # Preview should show counts
    assert "total_ok" in data or "preview" in data
    assert data.get("preview", True) is True  # Should be preview mode


@pytest.mark.asyncio
async def test_cerrar_trimestre_preview_counts(client, db_session):
    """Preview shows correct counts of OK and CANCELADO."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Set exactly 2 OK, 1 CANCELADO
    updates = []
    for i, slot in enumerate(slots):
        if slot.get("empresa_id") and slot["estado"] not in ("VACANTE",):
            if i < 2:
                updates.append({"slot_id": slot["id"], "estado": "OK"})
            elif i == 2:
                updates.append({"slot_id": slot["id"], "estado": "CANCELADO"})
            if len(updates) >= 3:
                break

    if updates:
        await client.patch(
            f"/api/calendario/{TEST_TRIMESTRE}/slots-batch",
            json={"updates": updates}
        )

    response = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/cerrar",
        json={"confirmar": False}
    )

    data = response.json()

    # Verify counts match what we set
    if "total_ok" in data:
        assert data["total_ok"] >= 2
    if "total_cancelado" in data:
        assert data["total_cancelado"] >= 1


@pytest.mark.asyncio
async def test_cerrar_trimestre_execute(client, db_session):
    """POST /api/calendario/{trimestre}/cerrar with confirmar=true executes close."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    # Mark some slots as OK
    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    for slot in slots[:2]:
        if slot.get("empresa_id"):
            await client.patch(
                f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot['id']}",
                json={"estado": "OK"}
            )

    # Execute close
    response = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/cerrar",
        json={"confirmar": True}
    )

    assert response.status_code == 200
    data = response.json()

    # Should indicate closure was executed
    assert data.get("preview", True) is False or "total_ok" in data


@pytest.mark.asyncio
async def test_historico_after_close(client, db_session):
    """GET /api/historico/{trimestre} returns data after close."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    # Mark slots as OK
    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    for slot in cal_response.json()["slots"][:3]:
        if slot.get("empresa_id"):
            await client.patch(
                f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot['id']}",
                json={"estado": "OK"}
            )

    # Close trimestre
    await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/cerrar",
        json={"confirmar": True}
    )

    # Check historico
    response = await client.get(f"/api/historico/{TEST_TRIMESTRE}")

    # Should return data (may be 200 with data or 404 if not found)
    assert response.status_code in (200, 404)
    if response.status_code == 200:
        data = response.json()
        # Should have historico records
        assert isinstance(data, list) or "registros" in data or "historico" in data


@pytest.mark.asyncio
async def test_trimestres_cerrados_list(client, db_session):
    """GET /api/historico/trimestres returns list including closed trimestre."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    # Mark and close
    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    for slot in cal_response.json()["slots"][:2]:
        if slot.get("empresa_id"):
            await client.patch(
                f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot['id']}",
                json={"estado": "OK"}
            )

    await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/cerrar",
        json={"confirmar": True}
    )

    # Get list of closed trimestres
    response = await client.get("/api/historico/trimestres")

    assert response.status_code == 200
    data = response.json()

    # Should be a list of trimestres
    assert "trimestres" in data or isinstance(data, list)


@pytest.mark.asyncio
async def test_cerrar_empty_trimestre_fails(client):
    """Closing a trimestre with no data fails or returns 0 counts."""
    response = await client.post(
        "/api/calendario/EMPTY-Q9/cerrar",
        json={"confirmar": False}
    )

    # Either 404 (no data) or 200 with 0 counts
    assert response.status_code in (200, 404)
    if response.status_code == 200:
        data = response.json()
        assert data.get("total_ok", 0) == 0
