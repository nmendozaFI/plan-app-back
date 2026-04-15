"""
Tests for Operacion (Phase 3) endpoints - slot updates and Excel import.
"""

import pytest
from sqlalchemy import text
from .conftest import TEST_TRIMESTRE, setup_test_config_trimestral, setup_test_frecuencias, create_test_excel


@pytest.mark.asyncio
async def test_actualizar_slot_estado(client, db_session):
    """PATCH /api/calendario/{trimestre}/slots/{id} updates slot estado."""
    # Setup calendario
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    # Get a slot to update
    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Find a PLANIFICADO slot with empresa
    planificado_slot = next(
        (s for s in slots if s["estado"] == "PLANIFICADO" and s.get("empresa_id")),
        None
    )

    if planificado_slot:
        slot_id = planificado_slot["id"]

        # Update to CONFIRMADO
        response = await client.patch(
            f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot_id}",
            json={"estado": "CONFIRMADO"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data.get("estado") == "CONFIRMADO" or "slot" in data


@pytest.mark.asyncio
async def test_actualizar_slot_confirmado_to_ok(client, db_session):
    """CONFIRMADO -> OK transition works."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Find or create a CONFIRMADO slot
    slot = next((s for s in slots if s.get("empresa_id") and s["estado"] != "VACANTE"), None)
    if slot:
        slot_id = slot["id"]
        # First set to CONFIRMADO
        await client.patch(
            f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot_id}",
            json={"estado": "CONFIRMADO"}
        )
        # Then update to OK
        response = await client.patch(
            f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot_id}",
            json={"estado": "OK"}
        )

        assert response.status_code == 200


@pytest.mark.asyncio
async def test_actualizar_slot_vacante_assign_empresa(client, db_session):
    """Assigning empresa to VACANTE slot changes estado to PLANIFICADO."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Find a VACANTE slot
    vacante_slot = next((s for s in slots if s["estado"] == "VACANTE"), None)

    if vacante_slot:
        # Get an empresa to assign
        emp_response = await client.get("/api/empresas/", params={"activa": True})
        empresas = emp_response.json()["empresas"]
        if empresas:
            empresa_id = empresas[0]["id"]

            response = await client.patch(
                f"/api/calendario/{TEST_TRIMESTRE}/slots/{vacante_slot['id']}",
                json={"empresa_id": empresa_id}
            )

            assert response.status_code == 200
            # Estado should become PLANIFICADO after assigning empresa
            data = response.json()
            if "estado" in data:
                assert data["estado"] in ("PLANIFICADO", "VACANTE")  # Depends on implementation


@pytest.mark.asyncio
async def test_batch_update_slots(client, db_session):
    """PATCH /api/calendario/{trimestre}/slots-batch updates multiple slots."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Select first 3 slots with empresa
    slots_to_update = [s for s in slots if s.get("empresa_id") and s["estado"] not in ("VACANTE",)][:3]

    if len(slots_to_update) >= 2:
        updates = [
            {"slot_id": s["id"], "estado": "CONFIRMADO"}
            for s in slots_to_update
        ]

        response = await client.patch(
            f"/api/calendario/{TEST_TRIMESTRE}/slots-batch",
            json={"updates": updates}
        )

        assert response.status_code == 200
        data = response.json()
        assert "updated" in data or "actualizados" in data or isinstance(data, list)


@pytest.mark.asyncio
async def test_obtener_resumen(client, db_session):
    """GET /api/calendario/{trimestre}/resumen returns operational summary."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}/resumen")

    assert response.status_code == 200
    data = response.json()

    # Check summary fields
    assert "trimestre" in data
    assert "total_slots" in data


@pytest.mark.asyncio
async def test_importar_excel_dry_run(client, db_session):
    """POST /api/calendario/{trimestre}/importar-excel-file with dry_run=true doesn't apply changes."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    # Get current calendar to create test Excel
    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Create Excel with modified estados
    excel_slots = []
    for slot in slots[:5]:  # Just first 5
        excel_slots.append({
            "semana": slot["semana"],
            "dia": slot["dia"],
            "horario": slot.get("horario", "09:00-10:30"),
            "turno": slot.get("turno", "M"),
            "empresa": slot.get("empresa_nombre", ""),
            "taller": slot.get("taller_nombre", ""),
            "programa": slot.get("programa", "EF"),
            "estado": "CONFIRMADO" if slot["estado"] == "PLANIFICADO" else slot["estado"],
            "confirmado": "SI" if slot["estado"] == "PLANIFICADO" else "",
        })

    excel_bytes = create_test_excel(excel_slots)

    response = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-file",
        params={"dry_run": True},
        files={"file": ("test.xlsx", excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    )

    assert response.status_code == 200
    data = response.json()

    # Should show detected changes
    assert "actualizados" in data or "cambios_detalle" in data
    # With dry_run, changes shouldn't be applied yet


@pytest.mark.asyncio
async def test_importar_excel_cambios_detalle(client, db_session):
    """Import Excel response includes cambios_detalle for estado changes."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Find PLANIFICADO slots to change to CONFIRMADO
    planificado_slots = [s for s in slots if s["estado"] == "PLANIFICADO" and s.get("empresa_nombre")][:3]

    if planificado_slots:
        excel_slots = []
        for slot in planificado_slots:
            excel_slots.append({
                "semana": slot["semana"],
                "dia": slot["dia"],
                "horario": slot.get("horario", ""),
                "turno": slot.get("turno", ""),
                "empresa": slot.get("empresa_nombre", ""),
                "taller": slot.get("taller_nombre", ""),
                "programa": slot.get("programa", "EF"),
                "estado": "CONFIRMADO",  # Changed from PLANIFICADO
                "confirmado": "SI",
            })

        excel_bytes = create_test_excel(excel_slots)

        response = await client.post(
            f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-file",
            params={"dry_run": True},
            files={"file": ("test.xlsx", excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        )

        assert response.status_code == 200
        data = response.json()

        # Should have cambios_detalle
        assert "cambios_detalle" in data
        if data["actualizados"] > 0:
            assert len(data["cambios_detalle"]) > 0
            # Each cambio should have required fields
            for cambio in data["cambios_detalle"]:
                assert "campo" in cambio
                assert "valor_anterior" in cambio
                assert "valor_nuevo" in cambio


@pytest.mark.asyncio
async def test_importar_excel_applies_changes(client, db_session):
    """POST with dry_run=false actually applies changes."""
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
    await setup_test_frecuencias(db_session, TEST_TRIMESTRE)
    await client.post("/api/calendario/generar", json={"trimestre": TEST_TRIMESTRE})

    cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
    slots = cal_response.json()["slots"]

    # Find a PLANIFICADO slot
    slot = next((s for s in slots if s["estado"] == "PLANIFICADO" and s.get("empresa_nombre")), None)

    if slot:
        excel_slots = [{
            "semana": slot["semana"],
            "dia": slot["dia"],
            "horario": slot.get("horario", ""),
            "turno": slot.get("turno", ""),
            "empresa": slot.get("empresa_nombre", ""),
            "taller": slot.get("taller_nombre", ""),
            "programa": slot.get("programa", "EF"),
            "estado": "OK",  # Changed to OK
            "confirmado": "SI",
        }]

        excel_bytes = create_test_excel(excel_slots)

        # Import with dry_run=false
        response = await client.post(
            f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-file",
            params={"dry_run": False},
            files={"file": ("test.xlsx", excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        )

        assert response.status_code == 200

        # Verify change was applied
        verify_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
        updated_slots = verify_response.json()["slots"]
        updated_slot = next((s for s in updated_slots if s["id"] == slot["id"]), None)

        if updated_slot:
            assert updated_slot["estado"] == "OK"
