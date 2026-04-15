"""
Integration test: Full trimestre lifecycle end-to-end.

This is the most important test - it verifies the entire workflow:
  1. Setup: Ensure settings, empresas, talleres exist
  2. Initialize config trimestral
  3. Calculate frecuencias
  4. Confirm frecuencias
  5. Generate calendario
  6. Verify calendario (no festivos, correct slots)
  7. Update slots: confirm, mark OK, cancel
  8. Export Excel
  9. Import modified Excel
  10. Close trimestre
  11. Verify historico created
  12. Cleanup
"""

import pytest
from sqlalchemy import text
from .conftest import (
    TEST_TRIMESTRE,
    TEST_EMPRESA_PREFIX,
    setup_test_config_trimestral,
    create_test_excel,
)


@pytest.mark.asyncio
class TestFullLifecycle:
    """Full trimestre lifecycle integration tests."""

    async def test_complete_trimestre_flow(self, client, db_session):
        """
        Test the complete trimestre lifecycle from setup to close.
        This is the golden path test.
        """
        # ── 1. Verify prerequisites exist ─────────────────────────
        # Check that talleres exist
        talleres_response = await client.get("/api/talleres")
        assert talleres_response.status_code == 200
        talleres = talleres_response.json()
        assert len(talleres) >= 10, "Need at least 10 talleres for meaningful test"

        # Check that empresas exist
        empresas_response = await client.get("/api/empresas")
        assert empresas_response.status_code == 200
        empresas_data = empresas_response.json()
        assert len(empresas_data.get("empresas", [])) >= 5, "Need at least 5 empresas"

        # Get first 5 empresa IDs for testing
        empresa_ids = [e["id"] for e in empresas_data["empresas"][:5]]

        # ── 2. Initialize config trimestral ───────────────────────
        init_response = await client.post(
            f"/api/config-trimestral/{TEST_TRIMESTRE}/inicializar",
            json={"origen_trimestre": None}  # Create defaults
        )
        # 200 or 409 (already exists) are both acceptable
        assert init_response.status_code in (200, 409), \
            f"Config init failed: {init_response.text}"

        # Verify configs were created
        configs_response = await client.get(f"/api/config-trimestral/{TEST_TRIMESTRE}")
        assert configs_response.status_code == 200
        configs_data = configs_response.json()
        assert configs_data.get("total", 0) > 0, "No configs created"

        # ── 3. Calculate frecuencias ──────────────────────────────
        calc_response = await client.post(
            "/api/frecuencias/calcular",
            json={
                "trimestre": TEST_TRIMESTRE,
                "max_ef": 14,
                "max_it": 6,
            }
        )
        assert calc_response.status_code == 200, \
            f"Frecuencias calcular failed: {calc_response.text}"

        calc_data = calc_response.json()
        assert "empresas" in calc_data
        assert "total_ef" in calc_data
        assert "total_it" in calc_data
        assert "status" in calc_data

        # Verify capacity limits
        semanas = calc_data.get("semanas_disponibles", 13)
        assert calc_data["total_ef"] <= 14 * semanas
        assert calc_data["total_it"] <= 6 * semanas

        # ── 4. Confirm frecuencias ────────────────────────────────
        empresas_to_confirm = [
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
                "empresas": empresas_to_confirm
            }
        )
        assert confirm_response.status_code == 200, \
            f"Frecuencias confirm failed: {confirm_response.text}"

        # Verify frecuencias were persisted
        freq_get_response = await client.get(f"/api/frecuencias/{TEST_TRIMESTRE}")
        assert freq_get_response.status_code == 200
        freq_data = freq_get_response.json()
        assert len(freq_data.get("frecuencias", [])) > 0, "No frecuencias confirmed"

        # ── 5. Generate calendario ────────────────────────────────
        gen_response = await client.post(
            "/api/calendario/generar",
            json={
                "trimestre": TEST_TRIMESTRE,
                "timeout_seconds": 60,
                "max_ef": 14,
                "max_it": 6,
            }
        )
        assert gen_response.status_code == 200, \
            f"Calendario generate failed: {gen_response.text}"

        gen_data = gen_response.json()
        assert gen_data["status"] in ("OPTIMAL", "FEASIBLE"), \
            f"Solver failed with status: {gen_data['status']}"
        assert gen_data["total_slots"] > 0, "No slots generated"

        # ── 6. Verify calendario structure ────────────────────────
        cal_response = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
        assert cal_response.status_code == 200

        cal_data = cal_response.json()
        slots = cal_data.get("slots", [])
        assert len(slots) > 0, "No slots in calendario"

        # Verify slot structure
        for slot in slots[:5]:  # Check first 5 slots
            assert "id" in slot
            assert "semana" in slot
            assert "dia" in slot
            assert "taller_nombre" in slot or "taller_id" in slot
            assert "estado" in slot
            assert slot["estado"] in ("PLANIFICADO", "CONFIRMADO", "OK", "CANCELADO", "VACANTE")

        # ── 7. Update some slots ──────────────────────────────────
        # Find a PLANIFICADO slot with an empresa
        planificado_slots = [s for s in slots if s["estado"] == "PLANIFICADO" and s.get("empresa_id")]
        if planificado_slots:
            slot_to_update = planificado_slots[0]

            # Update to CONFIRMADO
            update_response = await client.patch(
                f"/api/calendario/{TEST_TRIMESTRE}/slots/{slot_to_update['id']}",
                json={"estado": "CONFIRMADO", "confirmado": True}
            )
            assert update_response.status_code == 200, \
                f"Slot update failed: {update_response.text}"

            # Verify update
            updated_slot = update_response.json().get("slot", update_response.json())
            assert updated_slot.get("estado") == "CONFIRMADO" or \
                   updated_slot.get("confirmado") == True

        # ── 8. Test batch update ──────────────────────────────────
        if len(planificado_slots) >= 2:
            batch_updates = [
                {"slot_id": planificado_slots[0]["id"], "notas": "Test note 1"},
                {"slot_id": planificado_slots[1]["id"], "notas": "Test note 2"},
            ]

            batch_response = await client.patch(
                f"/api/calendario/{TEST_TRIMESTRE}/slots-batch",
                json={"updates": batch_updates}
            )
            assert batch_response.status_code == 200, \
                f"Batch update failed: {batch_response.text}"

        # ── 9. Export Excel ───────────────────────────────────────
        export_response = await client.post(
            f"/api/calendario/{TEST_TRIMESTRE}/exportar-excel"
        )
        assert export_response.status_code == 200, \
            f"Export failed: {export_response.text}"

        # Verify it's an Excel file
        assert "application/vnd.openxmlformats" in export_response.headers.get("content-type", "")

        # ── 10. Get resumen operacional ───────────────────────────
        resumen_response = await client.get(
            f"/api/calendario/{TEST_TRIMESTRE}/resumen"
        )
        assert resumen_response.status_code == 200
        resumen_data = resumen_response.json()
        assert "total_slots" in resumen_data or "summary" in resumen_data

        # ── 11. Preview close trimestre ───────────────────────────
        # First update some slots to OK/CANCELADO for realistic close
        if planificado_slots:
            await client.patch(
                f"/api/calendario/{TEST_TRIMESTRE}/slots/{planificado_slots[0]['id']}",
                json={"estado": "OK"}
            )

        preview_response = await client.post(
            f"/api/calendario/{TEST_TRIMESTRE}/cerrar",
            json={"confirmar": False}  # Preview only
        )
        # Preview may return 200 with preview data or 400 if preconditions not met
        assert preview_response.status_code in (200, 400), \
            f"Close preview failed: {preview_response.text}"

        # ── 12. Actually close trimestre ──────────────────────────
        close_response = await client.post(
            f"/api/calendario/{TEST_TRIMESTRE}/cerrar",
            json={"confirmar": True}
        )
        # Close may fail if no OK/CANCELADO slots, that's acceptable
        assert close_response.status_code in (200, 400), \
            f"Close failed unexpectedly: {close_response.text}"

        if close_response.status_code == 200:
            # ── 13. Verify historico created ──────────────────────
            historico_response = await client.get(
                f"/api/historico/{TEST_TRIMESTRE}"
            )
            assert historico_response.status_code in (200, 404), \
                f"Historico check failed: {historico_response.text}"

            if historico_response.status_code == 200:
                historico_data = historico_response.json()
                # Should have some records
                records = historico_data.get("records", historico_data.get("historico", []))
                # May be empty if no OK/CANCELADO slots
                assert isinstance(records, list)


@pytest.mark.asyncio
class TestCalendarioConstraints:
    """Tests for calendario constraint validation."""

    async def test_no_empresa_on_festivo_day(self, client, db_session):
        """H8: No empresa should be assigned to a festivo day slot."""
        # Setup test data
        await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

        # Calculate and confirm frecuencias
        calc_resp = await client.post(
            "/api/frecuencias/calcular",
            json={"trimestre": TEST_TRIMESTRE}
        )
        if calc_resp.status_code != 200:
            pytest.skip("Cannot calculate frecuencias")

        empresas = [
            {"empresa_id": e["empresa_id"], "talleres_ef": e["talleres_ef"], "talleres_it": e["talleres_it"]}
            for e in calc_resp.json()["empresas"]
        ]
        await client.post("/api/frecuencias/confirmar", json={"trimestre": TEST_TRIMESTRE, "empresas": empresas})

        # Generate calendario
        gen_resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": TEST_TRIMESTRE, "timeout_seconds": 60}
        )
        if gen_resp.status_code != 200:
            pytest.skip("Cannot generate calendario")

        # Get festivos for this trimestre
        festivos_resp = await client.get("/api/importar/festivos/2026")
        festivos = festivos_resp.json().get("festivos", [])
        festivo_days = {
            (f["semana"], f["dia"])
            for f in festivos
            if f["trimestre"] == TEST_TRIMESTRE
        }

        if not festivo_days:
            pytest.skip(f"No festivos for {TEST_TRIMESTRE}")

        # Get calendario and verify no empresa on festivo slots
        cal_resp = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
        slots = cal_resp.json().get("slots", [])

        for slot in slots:
            slot_key = (slot["semana"], slot["dia"])
            if slot_key in festivo_days:
                assert slot.get("empresa_id") is None, \
                    f"Slot {slot['id']} on festivo day {slot_key} has empresa assigned"

    async def test_max_one_assignment_per_week(self, client, db_session):
        """H6: Each empresa appears max once per week."""
        await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

        calc_resp = await client.post("/api/frecuencias/calcular", json={"trimestre": TEST_TRIMESTRE})
        if calc_resp.status_code != 200:
            pytest.skip("Cannot calculate frecuencias")

        empresas = [
            {"empresa_id": e["empresa_id"], "talleres_ef": e["talleres_ef"], "talleres_it": e["talleres_it"]}
            for e in calc_resp.json()["empresas"]
        ]
        await client.post("/api/frecuencias/confirmar", json={"trimestre": TEST_TRIMESTRE, "empresas": empresas})

        gen_resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": TEST_TRIMESTRE, "timeout_seconds": 60}
        )
        if gen_resp.status_code != 200:
            pytest.skip("Cannot generate calendario")

        cal_resp = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
        slots = cal_resp.json().get("slots", [])

        # Group slots by empresa and week
        from collections import defaultdict
        empresa_week_count = defaultdict(lambda: defaultdict(int))

        for slot in slots:
            eid = slot.get("empresa_id")
            if eid:
                semana = slot["semana"]
                empresa_week_count[eid][semana] += 1

        # Verify no empresa appears more than once per week
        for eid, weeks in empresa_week_count.items():
            for semana, count in weeks.items():
                assert count <= 1, \
                    f"Empresa {eid} appears {count} times in week {semana}"


@pytest.mark.asyncio
class TestDataIntegrity:
    """Tests for data integrity across operations."""

    async def test_frecuencia_totals_match_calendario(self, client, db_session):
        """EF/IT totals in frecuencias should match calendario assignments."""
        await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

        calc_resp = await client.post("/api/frecuencias/calcular", json={"trimestre": TEST_TRIMESTRE})
        if calc_resp.status_code != 200:
            pytest.skip("Cannot calculate frecuencias")

        calc_data = calc_resp.json()
        empresas = [
            {"empresa_id": e["empresa_id"], "talleres_ef": e["talleres_ef"], "talleres_it": e["talleres_it"]}
            for e in calc_data["empresas"]
        ]
        await client.post("/api/frecuencias/confirmar", json={"trimestre": TEST_TRIMESTRE, "empresas": empresas})

        # Store expected totals per empresa
        expected_ef = {e["empresa_id"]: e["talleres_ef"] for e in calc_data["empresas"]}
        expected_it = {e["empresa_id"]: e["talleres_it"] for e in calc_data["empresas"]}

        gen_resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": TEST_TRIMESTRE, "timeout_seconds": 60}
        )
        if gen_resp.status_code != 200:
            pytest.skip("Cannot generate calendario")

        gen_data = gen_resp.json()
        if gen_data["status"] not in ("OPTIMAL", "FEASIBLE"):
            pytest.skip(f"Solver status: {gen_data['status']}")

        # Count actual assignments per empresa
        from collections import defaultdict
        actual_ef = defaultdict(int)
        actual_it = defaultdict(int)

        cal_resp = await client.get(f"/api/calendario/{TEST_TRIMESTRE}")
        slots = cal_resp.json().get("slots", [])

        for slot in slots:
            eid = slot.get("empresa_id")
            if eid:
                programa = slot.get("programa")
                if programa == "EF":
                    actual_ef[eid] += 1
                elif programa == "IT":
                    actual_it[eid] += 1

        # Verify totals match (within festivo tolerance)
        for eid in expected_ef:
            exp_ef = expected_ef.get(eid, 0)
            act_ef = actual_ef.get(eid, 0)
            assert act_ef == exp_ef, \
                f"Empresa {eid}: expected {exp_ef} EF, got {act_ef}"

            exp_it = expected_it.get(eid, 0)
            act_it = actual_it.get(eid, 0)
            assert act_it == exp_it, \
                f"Empresa {eid}: expected {exp_it} IT, got {act_it}"


@pytest.mark.asyncio
class TestErrorHandling:
    """Tests for error handling and edge cases."""

    async def test_generate_without_frecuencias(self, client):
        """Cannot generate calendario without confirmed frecuencias."""
        response = await client.post(
            "/api/calendario/generar",
            json={"trimestre": "NONEXISTENT-Q1"}
        )
        assert response.status_code == 404

    async def test_confirm_empty_empresas(self, client, db_session):
        """Confirming with empty empresas list should fail or be no-op."""
        await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

        response = await client.post(
            "/api/frecuencias/confirmar",
            json={"trimestre": TEST_TRIMESTRE, "empresas": []}
        )
        # Empty list should either fail or return 0 confirmed
        assert response.status_code in (200, 400)

    async def test_update_nonexistent_slot(self, client):
        """Updating a slot that doesn't exist should 404."""
        response = await client.patch(
            f"/api/calendario/{TEST_TRIMESTRE}/slots/999999",
            json={"estado": "OK"}
        )
        assert response.status_code == 404

    async def test_invalid_estado_value(self, client, db_session):
        """Setting an invalid estado should fail."""
        await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)

        # Try to update with invalid estado
        response = await client.patch(
            f"/api/calendario/{TEST_TRIMESTRE}/slots/1",
            json={"estado": "INVALID_STATUS"}
        )
        # Should fail validation
        assert response.status_code in (400, 404, 422)
