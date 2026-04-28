"""V21 / Deuda 4: tests for the BASE/EXTRA-aware legacy update endpoint
POST /api/calendario/{trimestre}/importar-excel-file.

Disambiguation algorithm under test (per row):
  - 1 candidate                        → that row (with optional Tipo cross-check).
  - 2+ candidates + col G resolved     → match on empresa_id_original.
  - 2+ candidates + col G missing      → match on empresa_id (actual).
  - 2+ candidates, none unique         → warn + skip.

Tests run isolated (deuda 6 cascade-fail with module-scoped engine is known
and out of scope here).
"""

import pytest
from io import BytesIO

import openpyxl
from sqlalchemy import text

from .conftest import TEST_TRIMESTRE, TEST_EMPRESA_PREFIX


# ── Excel builder ──────────────────────────────────────────────

def _build_legacy_update_excel(
    rows: list[dict],
    *,
    include_original_col: bool = True,
    include_motivo_col: bool = True,
) -> bytes:
    """Build an Excel that the legacy update endpoint can ingest.

    Each row dict can carry the same keys as the headers below. Missing keys
    default to "" (or 1 for Semana).

    Flags:
      - include_original_col: when False, omits "Empresa Original" (col G) so
        tests can exercise the legacy-mode fallback path.
      - include_motivo_col: when False, omits "Motivo cambio" (col N) so tests
        can exercise the "old Excel without motivo" compatibility path.
    """
    headers = ["Semana", "Fecha", "Día", "Horario", "Turno", "Empresa"]
    if include_original_col:
        headers.append("Empresa Original")
    headers.extend(["Taller", "Programa", "Ciudad", "Tipo", "Estado", "Confirmado"])
    if include_motivo_col:
        headers.append("Motivo cambio")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Calendario"
    ws.append(headers)
    for row in rows:
        values = []
        for h in headers:
            values.append(row.get(h, "" if h != "Semana" else 1))
        ws.append(values)
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


# ── DB helpers ─────────────────────────────────────────────────


async def _create_empresa(db, nombre: str) -> int:
    res = await db.execute(
        text(
            'INSERT INTO empresa (nombre, tipo, "updatedAt") '
            "VALUES (:n, 'AMBAS', NOW()) "
            'ON CONFLICT (nombre) DO UPDATE SET activa = true '
            "RETURNING id"
        ),
        {"n": nombre},
    )
    eid = res.scalar()
    await db.commit()
    return eid


async def _set_config_trimestral(db, empresa_id: int, escuela_propia: bool):
    await db.execute(
        text(
            'INSERT INTO "configTrimestral" '
            '("empresaId", trimestre, "tipoParticipacion", "escuelaPropia", '
            '"disponibilidadDias", "updatedAt") '
            "VALUES (:eid, :tri, 'AMBAS', :ep, 'L,M,X,J,V', NOW()) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE '
            'SET "escuelaPropia" = EXCLUDED."escuelaPropia"'
        ),
        {"eid": empresa_id, "tri": TEST_TRIMESTRE, "ep": escuela_propia},
    )
    await db.commit()


async def _pick_one_taller(db) -> dict:
    res = await db.execute(
        text(
            'SELECT id, nombre, "diaSemana", horario, programa '
            "FROM taller WHERE activo = true AND \"diaSemana\" IS NOT NULL "
            "AND horario IS NOT NULL ORDER BY id LIMIT 1"
        ),
    )
    row = res.mappings().first()
    if row is None:
        pytest.skip("No active talleres with day/horario in catalog")
    return dict(row)


async def _insert_slot(
    db,
    *,
    semana: int,
    dia: str,
    horario: str,
    empresa_id: int,
    empresa_id_original: int | None,
    taller_id: int,
    tipo_asignacion: str,  # BASE | EXTRA
    estado: str = "PLANIFICADO",
    confirmado: bool = False,
    turno: str = "M",
    motivo_cambio: str | None = None,
) -> int:
    """Insert a planificacion row directly. empresa_id_original lets the test
    simulate post-reassignment state (original != current); motivo_cambio
    lets the follow-up tests pre-seed motivoCambio in BD."""
    res = await db.execute(
        text(
            '''
            INSERT INTO planificacion (
                trimestre, semana, dia, horario, turno,
                "empresaId", "empresaIdOriginal", "tallerId",
                "tipoAsignacion", "esContingencia", estado, confirmado,
                "motivoCambio", "updatedAt"
            ) VALUES (
                :tri, :sem, :dia, :horario, :turno,
                :eid, :eid_orig, :tid,
                :tipo, false, :estado, :confirmado,
                :motivo, NOW()
            ) RETURNING id
            '''
        ),
        {
            "tri": TEST_TRIMESTRE,
            "sem": semana,
            "dia": dia,
            "horario": horario,
            "turno": turno,
            "eid": empresa_id,
            "eid_orig": empresa_id_original if empresa_id_original is not None else empresa_id,
            "tid": taller_id,
            "tipo": tipo_asignacion,
            "estado": estado,
            "confirmado": confirmado,
            "motivo": motivo_cambio,
        },
    )
    sid = res.scalar()
    await db.commit()
    return sid


async def _post_excel(client, excel_bytes: bytes, dry_run: bool = False):
    return await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-file",
        params={"dry_run": dry_run},
        files={
            "file": (
                "c.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )


# ── Tests ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_legacy_update_base_only(client, db_session):
    """1 BASE slot, no shared row. Excel marks confirmado=SI. No warnings."""
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_BASE_A")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_BASE_A"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)

    slot_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": a_name, "Empresa Original": a_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["actualizados"] == 1, data
    assert data["errores"] == 0
    assert data["warnings"] == []

    db_row = await db_session.execute(
        text("SELECT confirmado FROM planificacion WHERE id = :id"),
        {"id": slot_id},
    )
    assert db_row.scalar() is True


@pytest.mark.asyncio
async def test_legacy_update_extra_only(client, db_session):
    """1 EXTRA slot solo (no shared BASE next to it). Excel sets estado=CANCELADO."""
    taller = await _pick_one_taller(db_session)
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_EXTRA_B")
    b_name = f"{TEST_EMPRESA_PREFIX}LEG_EXTRA_B"
    await _set_config_trimestral(db_session, b_id, escuela_propia=True)

    slot_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=b_id, empresa_id_original=b_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": b_name, "Empresa Original": b_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "EXTRA", "Estado": "CANCELADO", "Confirmado": "",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["actualizados"] == 1, data
    assert data["errores"] == 0
    assert data["warnings"] == []

    db_row = await db_session.execute(
        text("SELECT estado FROM planificacion WHERE id = :id"),
        {"id": slot_id},
    )
    assert db_row.scalar() == "CANCELADO"


@pytest.mark.asyncio
async def test_legacy_update_slot_compartido_actualiza_base(client, db_session):
    """Shared BASE+EXTRA. Excel touches BASE only → BASE updated, EXTRA intact."""
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_S_BASE")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_S_EP")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_S_BASE"
    b_name = f"{TEST_EMPRESA_PREFIX}LEG_S_EP"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=True)

    base_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )
    extra_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=b_id, empresa_id_original=b_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": a_name, "Empresa Original": a_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 1, data
    assert data["errores"] == 0
    assert data["warnings"] == []

    rows = await db_session.execute(
        text(
            "SELECT id, confirmado, estado FROM planificacion "
            "WHERE id IN (:b, :e)"
        ),
        {"b": base_id, "e": extra_id},
    )
    by_id = {r["id"]: r for r in rows.mappings().all()}
    assert by_id[base_id]["confirmado"] is True, "BASE must be updated"
    assert by_id[extra_id]["confirmado"] is False, "EXTRA must remain untouched"


@pytest.mark.asyncio
async def test_legacy_update_slot_compartido_actualiza_extra(client, db_session):
    """Shared BASE+EXTRA. Excel touches EXTRA only (col G=B)."""
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_SE_BASE")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_SE_EP")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_SE_BASE"
    b_name = f"{TEST_EMPRESA_PREFIX}LEG_SE_EP"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=True)

    base_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )
    extra_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=b_id, empresa_id_original=b_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": b_name, "Empresa Original": b_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "EXTRA", "Estado": "CANCELADO", "Confirmado": "",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 1
    assert data["warnings"] == []

    rows = await db_session.execute(
        text("SELECT id, estado FROM planificacion WHERE id IN (:b, :e)"),
        {"b": base_id, "e": extra_id},
    )
    by_id = {r["id"]: r for r in rows.mappings().all()}
    assert by_id[extra_id]["estado"] == "CANCELADO", "EXTRA must be updated"
    assert by_id[base_id]["estado"] == "PLANIFICADO", "BASE must remain untouched"


@pytest.mark.asyncio
async def test_legacy_update_slot_compartido_ambas_filas(client, db_session):
    """Shared BASE+EXTRA. Excel updates both rows in one go."""
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_BOTH_BASE")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_BOTH_EP")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_BOTH_BASE"
    b_name = f"{TEST_EMPRESA_PREFIX}LEG_BOTH_EP"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=True)

    base_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )
    extra_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=b_id, empresa_id_original=b_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
    )

    excel = _build_legacy_update_excel([
        {
            "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": a_name, "Empresa Original": a_name,
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
        },
        {
            "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": b_name, "Empresa Original": b_name,
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "EXTRA", "Estado": "CANCELADO", "Confirmado": "",
        },
    ])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 2
    assert data["warnings"] == []

    rows = await db_session.execute(
        text(
            "SELECT id, confirmado, estado FROM planificacion "
            "WHERE id IN (:b, :e)"
        ),
        {"b": base_id, "e": extra_id},
    )
    by_id = {r["id"]: r for r in rows.mappings().all()}
    assert by_id[base_id]["confirmado"] is True
    assert by_id[base_id]["estado"] == "PLANIFICADO"
    assert by_id[extra_id]["estado"] == "CANCELADO"


@pytest.mark.asyncio
async def test_legacy_update_match_por_original_tras_reasignacion(client, db_session):
    """Shared slot where BASE has been reassigned A→C since the export.

    BASE row in DB: empresa_id_original=A, empresa_id=C.
    EXTRA row in DB: original=B, actual=B.
    Excel row references col F=A, col G=A (matches the pre-reassignment state
    OR a manual edit by the planner). Endpoint MUST match by col G → BASE.
    """
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_REA_A")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_REA_B")
    c_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_REA_C")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_REA_A"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=True)
    await _set_config_trimestral(db_session, c_id, escuela_propia=False)

    base_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=c_id, empresa_id_original=a_id,  # reassigned A→C
        taller_id=taller["id"], tipo_asignacion="BASE",
    )
    extra_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=b_id, empresa_id_original=b_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": a_name, "Empresa Original": a_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 1
    assert data["errores"] == 0

    rows = await db_session.execute(
        text("SELECT id, confirmado FROM planificacion WHERE id IN (:b, :e)"),
        {"b": base_id, "e": extra_id},
    )
    by_id = {r["id"]: r for r in rows.mappings().all()}
    assert by_id[base_id]["confirmado"] is True, "BASE must be matched via col G"
    assert by_id[extra_id]["confirmado"] is False


@pytest.mark.asyncio
async def test_legacy_update_fallback_a_actual_col_g_vacia(client, db_session):
    """Old Excel without 'Empresa Original' column. Fallback by col F (actual).

    BASE A reassigned to C in DB; EXTRA B intact. Excel only has col F=C.
    Match by actual must resolve to BASE (the reassigned row).
    """
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_FB_A")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_FB_B")
    c_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_FB_C")
    c_name = f"{TEST_EMPRESA_PREFIX}LEG_FB_C"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=True)
    await _set_config_trimestral(db_session, c_id, escuela_propia=False)

    base_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=c_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )
    extra_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=b_id, empresa_id_original=b_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
    )

    excel = _build_legacy_update_excel(
        [{
            "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": c_name,
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
        }],
        include_original_col=False,
    )
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 1, data
    assert data["errores"] == 0

    rows = await db_session.execute(
        text("SELECT id, confirmado FROM planificacion WHERE id IN (:b, :e)"),
        {"b": base_id, "e": extra_id},
    )
    by_id = {r["id"]: r for r in rows.mappings().all()}
    assert by_id[base_id]["confirmado"] is True, "fallback by actual must hit BASE"
    assert by_id[extra_id]["confirmado"] is False


@pytest.mark.asyncio
async def test_legacy_update_empresa_no_matchea(client, db_session):
    """Shared slot. Excel references an empresa that's neither original nor actual.

    Expected: warning naming both attempted matches + nothing modified.
    """
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_NM_BASE")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_NM_EP")
    z_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_NM_Z")
    z_name = f"{TEST_EMPRESA_PREFIX}LEG_NM_Z"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=True)

    base_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
        confirmado=False,
    )
    extra_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=b_id, empresa_id_original=b_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
        confirmado=False,
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": z_name, "Empresa Original": z_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 0, data
    assert data["errores"] >= 1
    assert any(
        "ninguna fila del slot tiene empresa original" in w for w in data["warnings"]
    ), data["warnings"]

    rows = await db_session.execute(
        text("SELECT confirmado FROM planificacion WHERE id IN (:b, :e)"),
        {"b": base_id, "e": extra_id},
    )
    confirms = [r["confirmado"] for r in rows.mappings().all()]
    assert all(c is False for c in confirms), "neither row may be modified"


@pytest.mark.asyncio
async def test_legacy_update_validacion_cruzada_tipo_inconsistente(client, db_session):
    """Lone BASE row. Excel says Tipo=EXTRA. Update applied + informative warning."""
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_TI_A")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_TI_A"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)

    slot_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": a_name, "Empresa Original": a_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "EXTRA",  # ← inconsistent with BD (BASE)
        "Estado": "PLANIFICADO", "Confirmado": "SI",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["actualizados"] == 1, "row must still update"
    assert data["errores"] == 0
    assert any(
        "tipo del Excel (EXTRA) no coincide con BD (BASE)" in w
        for w in data["warnings"]
    ), data["warnings"]

    db_row = await db_session.execute(
        text("SELECT confirmado FROM planificacion WHERE id = :id"),
        {"id": slot_id},
    )
    assert db_row.scalar() is True


@pytest.mark.asyncio
async def test_legacy_update_warning_count_mixto(client, db_session):
    """5 Excel rows: 4 OK across BASE/EXTRA in 2 shared slots, 1 with bogus
    empresa. Expected: 4 actualizados, 1 warning, total_procesados=5.
    """
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_MX_BASE_S1")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_MX_EP_S1")
    c_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_MX_BASE_S2")
    d_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_MX_EP_S2")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_MX_BASE_S1"
    b_name = f"{TEST_EMPRESA_PREFIX}LEG_MX_EP_S1"
    c_name = f"{TEST_EMPRESA_PREFIX}LEG_MX_BASE_S2"
    d_name = f"{TEST_EMPRESA_PREFIX}LEG_MX_EP_S2"
    for eid in (a_id, c_id):
        await _set_config_trimestral(db_session, eid, escuela_propia=False)
    for eid in (b_id, d_id):
        await _set_config_trimestral(db_session, eid, escuela_propia=True)

    # Shared slot in semana 1
    base1 = await _insert_slot(
        db_session, semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )
    extra1 = await _insert_slot(
        db_session, semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=b_id, empresa_id_original=b_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
    )
    # Shared slot in semana 2
    base2 = await _insert_slot(
        db_session, semana=2, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=c_id, empresa_id_original=c_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )
    extra2 = await _insert_slot(
        db_session, semana=2, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=d_id, empresa_id_original=d_id,
        taller_id=taller["id"], tipo_asignacion="EXTRA",
    )

    excel_rows = [
        # 4 valid rows (mix BASE/EXTRA, S1+S2)
        {
            "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": a_name, "Empresa Original": a_name,
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
        },
        {
            "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": b_name, "Empresa Original": b_name,
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "EXTRA", "Estado": "CANCELADO", "Confirmado": "",
        },
        {
            "Semana": 2, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": c_name, "Empresa Original": c_name,
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
        },
        {
            "Semana": 2, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": d_name, "Empresa Original": d_name,
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "EXTRA", "Estado": "CANCELADO", "Confirmado": "",
        },
        # 1 bogus row: empresa original doesn't exist anywhere → hard skip.
        {
            "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}DOES_NOT_EXIST_XYZ",
            "Empresa Original": f"{TEST_EMPRESA_PREFIX}DOES_NOT_EXIST_XYZ",
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
        },
    ]
    excel = _build_legacy_update_excel(excel_rows)
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["total_procesados"] == 5, data
    assert data["actualizados"] == 4, data
    assert data["errores"] == 1, data
    assert len(data["warnings"]) >= 1
    assert any(
        "Empresa Original" in w and "no encontrada" in w
        for w in data["warnings"]
    ), data["warnings"]

    # Spot-check a couple of rows actually moved.
    rows = await db_session.execute(
        text(
            "SELECT id, confirmado, estado FROM planificacion "
            "WHERE id IN (:a, :b, :c, :d)"
        ),
        {"a": base1, "b": extra1, "c": base2, "d": extra2},
    )
    by_id = {r["id"]: r for r in rows.mappings().all()}
    assert by_id[base1]["confirmado"] is True
    assert by_id[extra1]["estado"] == "CANCELADO"
    assert by_id[base2]["confirmado"] is True
    assert by_id[extra2]["estado"] == "CANCELADO"


# ── V21 / Deuda 4 follow-up: motivo cambio (col N) ─────────────


@pytest.mark.asyncio
async def test_legacy_update_motivo_decision_planificador(client, db_session):
    """Excel reassigns empresa A→B with motivo='Decisión planificador'.

    Expected: empresaId moves to B and motivoCambio = DECISION_PLANIFICADOR.
    """
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_M_DP_A")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_M_DP_B")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_M_DP_A"
    b_name = f"{TEST_EMPRESA_PREFIX}LEG_M_DP_B"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=False)

    slot_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": b_name, "Empresa Original": a_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "",
        "Motivo cambio": "Decisión planificador",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 1
    assert data["errores"] == 0
    assert data["warnings"] == []

    db_row = await db_session.execute(
        text(
            'SELECT "empresaId", "motivoCambio" '
            "FROM planificacion WHERE id = :id"
        ),
        {"id": slot_id},
    )
    rec = db_row.mappings().first()
    assert rec["empresaId"] == b_id
    assert rec["motivoCambio"] == "DECISION_PLANIFICADOR"


@pytest.mark.asyncio
async def test_legacy_update_motivo_empresa_cancelo(client, db_session):
    """Reassign A→B with motivo='Empresa canceló' → motivoCambio=EMPRESA_CANCELO."""
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_M_EC_A")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_M_EC_B")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_M_EC_A"
    b_name = f"{TEST_EMPRESA_PREFIX}LEG_M_EC_B"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=False)

    slot_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": b_name, "Empresa Original": a_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "",
        "Motivo cambio": "Empresa canceló",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 1
    assert data["errores"] == 0
    assert data["warnings"] == []

    db_row = await db_session.execute(
        text(
            'SELECT "empresaId", "motivoCambio" '
            "FROM planificacion WHERE id = :id"
        ),
        {"id": slot_id},
    )
    rec = db_row.mappings().first()
    assert rec["empresaId"] == b_id
    assert rec["motivoCambio"] == "EMPRESA_CANCELO"


@pytest.mark.asyncio
async def test_legacy_update_motivo_vacio_no_borra_existente(client, db_session):
    """Re-importing an Excel with empty col N must NOT clear an existing motivoCambio.

    Slot pre-seeded with motivoCambio=DECISION_PLANIFICADOR. Excel changes
    confirmado but leaves motivo column blank. After update: confirmado moved,
    motivoCambio still DECISION_PLANIFICADOR.
    """
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_M_KEEP_A")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_M_KEEP_A"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)

    slot_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
        motivo_cambio="DECISION_PLANIFICADOR",
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": a_name, "Empresa Original": a_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "SI",
        # "Motivo cambio" key omitted → helper fills with "" → cell empty.
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 1
    assert data["errores"] == 0

    db_row = await db_session.execute(
        text(
            'SELECT confirmado, "motivoCambio" '
            "FROM planificacion WHERE id = :id"
        ),
        {"id": slot_id},
    )
    rec = db_row.mappings().first()
    assert rec["confirmado"] is True
    assert rec["motivoCambio"] == "DECISION_PLANIFICADOR", (
        "empty motivo cell must NOT overwrite existing motivoCambio in BD"
    )


@pytest.mark.asyncio
async def test_legacy_update_motivo_invalido_warning(client, db_session):
    """Free-form motivo string ('razón rara') → warning + motivo untouched.

    Other updates (empresa change) still applied.
    """
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_M_INV_A")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_M_INV_B")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_M_INV_A"
    b_name = f"{TEST_EMPRESA_PREFIX}LEG_M_INV_B"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_id, escuela_propia=False)

    slot_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
        # No motivoCambio pre-seeded → BD has NULL.
    )

    excel = _build_legacy_update_excel([{
        "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
        "Empresa": b_name, "Empresa Original": a_name,
        "Taller": taller["nombre"], "Programa": taller["programa"],
        "Tipo": "BASE", "Estado": "PLANIFICADO", "Confirmado": "",
        "Motivo cambio": "razón rara",
    }])
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["actualizados"] == 1, "empresa change must still apply"
    assert any(
        "motivo 'razón rara' no reconocido" in w for w in data["warnings"]
    ), data["warnings"]

    db_row = await db_session.execute(
        text(
            'SELECT "empresaId", "motivoCambio" '
            "FROM planificacion WHERE id = :id"
        ),
        {"id": slot_id},
    )
    rec = db_row.mappings().first()
    assert rec["empresaId"] == b_id
    assert rec["motivoCambio"] is None, "invalid motivo must NOT touch the field"


@pytest.mark.asyncio
async def test_legacy_update_excel_sin_columna_motivo_compatibilidad(client, db_session):
    """Old Excel built without 'Motivo cambio' column must still process other
    updates (estado/confirmado/empresa) without errors. Motivos in BD untouched.
    """
    taller = await _pick_one_taller(db_session)
    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}LEG_M_NOCOL_A")
    a_name = f"{TEST_EMPRESA_PREFIX}LEG_M_NOCOL_A"
    await _set_config_trimestral(db_session, a_id, escuela_propia=False)

    slot_id = await _insert_slot(
        db_session,
        semana=1, dia=taller["diaSemana"], horario=taller["horario"],
        empresa_id=a_id, empresa_id_original=a_id,
        taller_id=taller["id"], tipo_asignacion="BASE",
        motivo_cambio="EMPRESA_CANCELO",
    )

    excel = _build_legacy_update_excel(
        [{
            "Semana": 1, "Día": taller["diaSemana"], "Horario": taller["horario"],
            "Empresa": a_name, "Empresa Original": a_name,
            "Taller": taller["nombre"], "Programa": taller["programa"],
            "Tipo": "BASE", "Estado": "CONFIRMADO", "Confirmado": "SI",
        }],
        include_motivo_col=False,
    )
    resp = await _post_excel(client, excel)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["actualizados"] == 1, data
    assert data["errores"] == 0
    assert data["warnings"] == []

    db_row = await db_session.execute(
        text(
            'SELECT confirmado, estado, "motivoCambio" '
            "FROM planificacion WHERE id = :id"
        ),
        {"id": slot_id},
    )
    rec = db_row.mappings().first()
    assert rec["confirmado"] is True
    assert rec["estado"] == "CONFIRMADO"
    assert rec["motivoCambio"] == "EMPRESA_CANCELO", (
        "Excel without motivo column must NOT touch existing motivoCambio"
    )
