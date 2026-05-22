"""V25 Cambio C (Capa 6): regresión de la dispersión hard derivada.

Verifica que para empresas no-EP con frecuencia >= 2 los talleres asignados
están separados por un gap mínimo de floor(semanas / freq) semanas. Decisión
C4: fórmula derivada del modelo, no configurable.

Tres casos canónicos (V25 §10 C4):
  - freq=2  → gap=6  (13 // 2)
  - freq=3  → gap=4  (13 // 3)
  - freq=10 → gap=1  (13 // 10) — caso INDRA reforzado: la propiedad ya la
              garantizaba H6, este test verifica que el constraint nuevo no
              introduce regresión.

Trimestre artificial `2098-Q1` para evitar colisión con Capa 5
(`SOLVER_TEST_TRIMESTRE = "2099-Q1"`) que ahora coincide con `TEST_TRIMESTRE`
tras el hotfix pre-Capa 6. Cleanup manual por trimestre + TEST_EMPRESA_PREFIX
(patrón de test_planificacion_doble y Capa 5).
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from .conftest import TEST_EMPRESA_PREFIX


DISPERSION_TEST_TRIMESTRE = "2098-Q1"


async def _cleanup_dispersion(
    db, trimestre: str = DISPERSION_TEST_TRIMESTRE
) -> None:
    """Borra rastro del test en el trimestre artificial y empresas con prefijo.
    Idempotente."""
    for sql in (
        "DELETE FROM planificacion WHERE trimestre = :tri",
        "DELETE FROM frecuencia    WHERE trimestre = :tri",
        'DELETE FROM "configTrimestral" WHERE trimestre = :tri',
        'DELETE FROM "solverLog"   WHERE trimestre = :tri',
    ):
        try:
            await db.execute(text(sql), {"tri": trimestre})
        except Exception:
            pass
    try:
        await db.execute(
            text("DELETE FROM empresa WHERE nombre LIKE :pref"),
            {"pref": f"{TEST_EMPRESA_PREFIX}%"},
        )
    except Exception:
        pass
    await db.commit()


async def _setup_empresa_freq(
    db,
    nombre: str,
    ef: int,
    it: int = 0,
    escuela_propia: bool = False,
) -> int:
    """Crea empresa + CT + frecuencia con EF=ef, IT=it en
    DISPERSION_TEST_TRIMESTRE.

    `esNueva=false` para evitar H7 (filtra semanas 1-4 a empresas nuevas) y
    asegurar que las 13 semanas son candidatas válidas.
    """
    full_name = f"{TEST_EMPRESA_PREFIX}{nombre}"
    total = ef + it

    res = await db.execute(
        text(
            "INSERT INTO empresa "
            '(nombre, tipo, semaforo, activa, "esNueva", "updatedAt") '
            "VALUES (:n, 'AMBAS', 'VERDE', true, false, NOW()) "
            'ON CONFLICT (nombre) DO UPDATE SET activa = true, "esNueva" = false '
            "RETURNING id"
        ),
        {"n": full_name},
    )
    eid = res.scalar()

    await db.execute(
        text(
            'INSERT INTO "configTrimestral" '
            '("empresaId", trimestre, "tipoParticipacion", "escuelaPropia", '
            '"frecuenciaEF", "frecuenciaIT", "disponibilidadDias", "updatedAt") '
            "VALUES (:eid, :tri, 'AMBAS', :ep, :ef, :it, 'L,M,X,J,V', NOW()) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE SET '
            '"escuelaPropia" = EXCLUDED."escuelaPropia", '
            '"frecuenciaEF" = EXCLUDED."frecuenciaEF", '
            '"frecuenciaIT" = EXCLUDED."frecuenciaIT"'
        ),
        {
            "eid": eid,
            "tri": DISPERSION_TEST_TRIMESTRE,
            "ep": escuela_propia,
            "ef": ef,
            "it": it,
        },
    )

    cfg = await db.execute(
        text(
            'SELECT id FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": eid, "tri": DISPERSION_TEST_TRIMESTRE},
    )
    config_id = cfg.scalar()

    await db.execute(
        text(
            "INSERT INTO frecuencia "
            '("configId", "empresaId", trimestre, "talleresEF", "talleresIT", '
            '"totalAsignado", "semaforoCalculado", "scoreCalculado", "esNueva") '
            "VALUES (:cfg, :eid, :tri, :ef, :it, :tot, 'VERDE', 80.0, false) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE SET '
            '"talleresEF" = EXCLUDED."talleresEF", '
            '"talleresIT" = EXCLUDED."talleresIT", '
            '"totalAsignado" = EXCLUDED."totalAsignado"'
        ),
        {
            "cfg": config_id,
            "eid": eid,
            "tri": DISPERSION_TEST_TRIMESTRE,
            "ef": ef,
            "it": it,
            "tot": total,
        },
    )

    await db.commit()
    return eid


@pytest.mark.asyncio
async def test_dispersion_freq_2_gap_6(client, db_session):
    """V25 Capa 6: empresa no-EP con freq=2 en 13 semanas → gap_min=6.

    Las 2 semanas asignadas deben estar separadas por al menos 6 semanas
    (floor(13/2) = 6). Ejemplo válido: semana 1 y 7. NO permitido: semana
    1 y 6 (gap=5).
    """
    await _cleanup_dispersion(db_session)
    try:
        eid = await _setup_empresa_freq(
            db_session, "V25_DISP_FREQ2", ef=2, it=0, escuela_propia=False,
        )

        resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": DISPERSION_TEST_TRIMESTRE, "timeout_seconds": 60},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] in ("OPTIMAL", "FEASIBLE"), (
            f"solver no produjo solución: status={data['status']}, "
            f"warnings={data.get('warnings')}"
        )

        semanas = sorted(
            slot["semana"]
            for slot in data["slots"]
            if slot.get("empresa_id") == eid
        )
        assert len(semanas) == 2, (
            f"Esperaba 2 asignaciones, hay {len(semanas)}: {semanas}"
        )
        # Cada par de semanas debe respetar gap >= 6.
        assert len(set(semanas)) == 2, (
            f"Semanas no distintas (H6 violado): {semanas}"
        )
        gap = semanas[1] - semanas[0]
        assert gap >= 6, (
            f"Gap insuficiente: {gap} < 6. Semanas asignadas: {semanas}"
        )
    finally:
        await _cleanup_dispersion(db_session)


@pytest.mark.asyncio
async def test_dispersion_freq_3_gap_4(client, db_session):
    """V25 Capa 6: empresa no-EP con freq=3 en 13 semanas → gap_min=4.

    Las 3 semanas asignadas deben tener gap >= 4 entre cada par consecutivo
    (floor(13/3) = 4).
    """
    await _cleanup_dispersion(db_session)
    try:
        eid = await _setup_empresa_freq(
            db_session, "V25_DISP_FREQ3", ef=3, it=0, escuela_propia=False,
        )

        resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": DISPERSION_TEST_TRIMESTRE, "timeout_seconds": 60},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] in ("OPTIMAL", "FEASIBLE"), (
            f"solver no produjo solución: status={data['status']}, "
            f"warnings={data.get('warnings')}"
        )

        semanas = sorted(
            slot["semana"]
            for slot in data["slots"]
            if slot.get("empresa_id") == eid
        )
        assert len(semanas) == 3, (
            f"Esperaba 3 asignaciones, hay {len(semanas)}: {semanas}"
        )
        assert len(set(semanas)) == 3, (
            f"Semanas no distintas (H6 violado): {semanas}"
        )
        gaps = [semanas[i + 1] - semanas[i] for i in range(len(semanas) - 1)]
        assert all(g >= 4 for g in gaps), (
            f"Gaps insuficientes: {gaps} (debe ser >= 4). Semanas: {semanas}"
        )
    finally:
        await _cleanup_dispersion(db_session)


@pytest.mark.asyncio
async def test_dispersion_freq_10_gap_1(client, db_session):
    """V25 Capa 6: empresa no-EP con freq=10 en 13 semanas → gap_min=1.

    Caso INDRA reforzado (V25 §7.5 Caso 2). Las 10 semanas asignadas deben
    ser todas distintas. La propiedad la garantiza H6 (max 1/semana para
    no-EP); este test verifica que H_dispersion no introduce regresión cuando
    gap_min=1 (el doble loop sobre `0 < s2-s1 < 1` queda vacío — no-op
    natural, CP-SAT lo simplifica solo).
    """
    await _cleanup_dispersion(db_session)
    try:
        eid = await _setup_empresa_freq(
            db_session, "V25_DISP_FREQ10", ef=10, it=0, escuela_propia=False,
        )

        resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": DISPERSION_TEST_TRIMESTRE, "timeout_seconds": 60},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["status"] in ("OPTIMAL", "FEASIBLE"), (
            f"solver no produjo solución: status={data['status']}, "
            f"warnings={data.get('warnings')}"
        )

        slots_empresa = [
            s for s in data["slots"] if s.get("empresa_id") == eid
        ]
        assert len(slots_empresa) == 10, (
            f"Esperaba 10 asignaciones, hay {len(slots_empresa)}"
        )
        semanas = [s["semana"] for s in slots_empresa]
        assert len(set(semanas)) == 10, (
            f"Semanas no únicas (regresión H6): {sorted(semanas)}"
        )
    finally:
        await _cleanup_dispersion(db_session)
