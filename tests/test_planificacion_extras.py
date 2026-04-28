"""V20: tests for the EXTRA classification path in the bulk calendar importer
plus the listing and delete endpoints.

The bulk endpoint must:
  - Treat rows whose (taller.nombre, diaSemana, horario, programa) does NOT
    match the catalog as candidates for EXTRA classification (soft match by
    nombre+programa).
  - Insert them with tipoAsignacion='EXTRA' iff the empresa has
    configTrimestral.escuelaPropia=true AND the row collides on
    (semana, dia, horario) with another row (in-batch or pre-existing) for
    a different empresa.
  - Otherwise reject the row as taller_no_encontrado.
"""

import pytest
from io import BytesIO

import openpyxl
from sqlalchemy import text

from .conftest import TEST_TRIMESTRE, TEST_EMPRESA_PREFIX


# ── Helpers ────────────────────────────────────────────────────

EXTRAS_HEADERS = [
    "Semana", "Fecha", "Día", "Horario", "Turno", "Empresa",
    "Taller", "Programa", "Ciudad", "Estado",
    "Empresa Original", "Tipo", "Confirmado", "Notas", "Motivo cambio",
]


def _build_extras_excel(rows: list[dict]) -> bytes:
    """Build an Excel using the headers the bulk endpoint expects (with 'Día')."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Calendario"
    ws.append(EXTRAS_HEADERS)
    for row in rows:
        ws.append([
            row.get("Semana", 1),
            row.get("Fecha", ""),
            row.get("Día", ""),
            row.get("Horario", ""),
            row.get("Turno", "M"),
            row.get("Empresa", ""),
            row.get("Taller", ""),
            row.get("Programa", "EF"),
            row.get("Ciudad", "MADRID"),
            row.get("Estado", "PLANIFICADO"),
            row.get("Empresa Original", ""),
            row.get("Tipo", ""),
            row.get("Confirmado", ""),
            row.get("Notas", ""),
            row.get("Motivo cambio", ""),
        ])
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


async def _create_empresa(db, nombre: str) -> int:
    """Insert a test empresa and return its id."""
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
    """Create or update configTrimestral for (empresa, TEST_TRIMESTRE)."""
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
    """Pick any active taller with diaSemana + horario set."""
    res = await db.execute(
        text(
            'SELECT id, nombre, "diaSemana", horario, programa '
            "FROM taller WHERE activo = true AND \"diaSemana\" IS NOT NULL "
            "AND horario IS NOT NULL "
            "ORDER BY id LIMIT 1"
        ),
    )
    row = res.mappings().first()
    if row is None:
        pytest.skip("No active talleres with day/horario in catalog")
    return dict(row)


async def _insert_base_slot(
    db,
    *,
    trimestre: str,
    semana: int,
    dia: str,
    horario: str,
    empresa_id: int,
    taller_id: int,
    turno: str = "M",
) -> int:
    """V21 helper: insert a BASE planificacion row directly via SQL.

    Used by POST/PATCH /extra tests to set up a colliding slot without
    going through the bulk Excel importer.
    """
    res = await db.execute(
        text(
            '''
            INSERT INTO planificacion (
                trimestre, semana, dia, horario, turno,
                "empresaId", "empresaIdOriginal", "tallerId",
                "tipoAsignacion", "esContingencia", estado, confirmado,
                "updatedAt"
            ) VALUES (
                :tri, :sem, :dia, :horario, :turno,
                :eid, :eid, :tid,
                'BASE', false, 'PLANIFICADO', false,
                NOW()
            ) RETURNING id
            '''
        ),
        {
            "tri": trimestre,
            "sem": semana,
            "dia": dia,
            "horario": horario,
            "turno": turno,
            "eid": empresa_id,
            "tid": taller_id,
        },
    )
    sid = res.scalar()
    await db.commit()
    return sid


async def _pick_two_talleres(db) -> tuple[dict, dict]:
    """Pick two real talleres that share programa but differ in (diaSemana, horario).

    Returns (catalog_anchor, base_anchor) where:
      - both have same programa
      - their (diaSemana, horario) pairs are different
    The bulk-test row for EXTRA will reference catalog_anchor.nombre with
    base_anchor's (dia, horario), forcing a strict-match miss + soft match.
    """
    res = await db.execute(
        text(
            'SELECT id, nombre, "diaSemana", horario, programa '
            "FROM taller WHERE activo = true AND \"diaSemana\" IS NOT NULL "
            "AND horario IS NOT NULL"
        ),
    )
    rows = [dict(r) for r in res.mappings().all()]
    by_program: dict[str, list[dict]] = {}
    for r in rows:
        by_program.setdefault(r["programa"], []).append(r)

    for prog, lst in by_program.items():
        for i, a in enumerate(lst):
            for b in lst[i + 1:]:
                if (a["diaSemana"], a["horario"]) != (b["diaSemana"], b["horario"]) \
                        and a["nombre"] != b["nombre"]:
                    return a, b
    pytest.skip("No two talleres with same programa but different (dia, horario) found")


# ── Tests ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_extra_inserted_when_ep_and_collision(client, db_session):
    """Positive: EP empresa + collision on (sem, dia, horario) → EXTRA inserted."""
    catalog_anchor, base_anchor = await _pick_two_talleres(db_session)

    ep_empresa_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}EXTRA_EP")
    base_empresa_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}EXTRA_BASE")
    await _set_config_trimestral(db_session, ep_empresa_id, escuela_propia=True)
    await _set_config_trimestral(db_session, base_empresa_id, escuela_propia=False)

    # Row 1 (BASE): base_empresa at base_anchor's catalog day/horario → matches strictly.
    # Row 2 (EXTRA candidate): ep_empresa using catalog_anchor.nombre but base_anchor's
    #   (dia, horario) → strict miss + soft hit + collision with row 1 → EXTRA.
    rows = [
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}EXTRA_BASE",
            "Taller": base_anchor["nombre"],
            "Programa": base_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}EXTRA_EP",
            "Taller": catalog_anchor["nombre"],
            "Programa": catalog_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
    ]
    excel = _build_extras_excel(rows)

    resp = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-bulk",
        files={"file": ("c.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        params={"dry_run": False, "wipe_first": False},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["extras_insertados"] == 1, data
    assert data["insertados"] == 1, data
    assert data["taller_no_encontrado"] == 0, data
    assert len(data["extras_detalle"]) == 1
    extra = data["extras_detalle"][0]
    assert extra["empresa_nombre"] == f"{TEST_EMPRESA_PREFIX}EXTRA_EP"
    assert extra["taller_nombre"] == catalog_anchor["nombre"]
    assert extra["semana"] == 1
    assert extra["dia"] == base_anchor["diaSemana"]
    assert extra["horario"] == base_anchor["horario"]
    assert extra["planificacion_id"] > 0

    # Verify row was actually persisted with tipoAsignacion='EXTRA'.
    db_row = await db_session.execute(
        text(
            'SELECT "tipoAsignacion", "empresaId" FROM planificacion '
            "WHERE id = :id"
        ),
        {"id": extra["planificacion_id"]},
    )
    rec = db_row.mappings().first()
    assert rec is not None
    assert rec["tipoAsignacion"] == "EXTRA"
    assert rec["empresaId"] == ep_empresa_id

    # Counter math invariant.
    assert data["total_procesados"] == (
        data["insertados"]
        + data["vacantes"]
        + data["extras_insertados"]
        + data["empresa_no_encontrada"]
        + data["taller_no_encontrado"]
        + data["errores"]
    ), data

    # GET /api/calendario/{tri}/extras returns the inserted EXTRA.
    list_resp = await client.get(f"/api/calendario/{TEST_TRIMESTRE}/extras")
    assert list_resp.status_code == 200
    list_data = list_resp.json()
    assert list_data["total"] >= 1
    assert any(e["id"] == extra["planificacion_id"] for e in list_data["extras"])


@pytest.mark.asyncio
async def test_no_ep_no_extra(client, db_session):
    """Negative: empresa is NOT escuelaPropia → row rejected as taller_no_encontrado."""
    catalog_anchor, base_anchor = await _pick_two_talleres(db_session)

    a_empresa_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}EXTRA_NOEP_A")
    b_empresa_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}EXTRA_NOEP_B")
    await _set_config_trimestral(db_session, a_empresa_id, escuela_propia=False)
    await _set_config_trimestral(db_session, b_empresa_id, escuela_propia=False)

    rows = [
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}EXTRA_NOEP_B",
            "Taller": base_anchor["nombre"],
            "Programa": base_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}EXTRA_NOEP_A",
            "Taller": catalog_anchor["nombre"],
            "Programa": catalog_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
    ]
    excel = _build_extras_excel(rows)

    resp = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-bulk",
        files={"file": ("c.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        params={"dry_run": False, "wipe_first": False},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["extras_insertados"] == 0
    assert data["taller_no_encontrado"] == 1
    assert data["errores"] == 0  # V20: taller rejection only increments taller_no_encontrado
    assert data["insertados"] == 1  # only the BASE row went in
    assert data["total_procesados"] == (
        data["insertados"]
        + data["vacantes"]
        + data["extras_insertados"]
        + data["empresa_no_encontrada"]
        + data["taller_no_encontrado"]
        + data["errores"]
    )


@pytest.mark.asyncio
async def test_ep_no_collision_no_extra(client, db_session):
    """Negative: EP empresa but no collision → row rejected as taller_no_encontrado."""
    catalog_anchor, base_anchor = await _pick_two_talleres(db_session)

    ep_empresa_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}EXTRA_NOCOL_EP")
    await _set_config_trimestral(db_session, ep_empresa_id, escuela_propia=True)

    # Single row: EP empresa, soft-match only, no other row at (sem,dia,horario).
    rows = [
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}EXTRA_NOCOL_EP",
            "Taller": catalog_anchor["nombre"],
            "Programa": catalog_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
    ]
    excel = _build_extras_excel(rows)

    resp = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-bulk",
        files={"file": ("c.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        params={"dry_run": False, "wipe_first": False},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["extras_insertados"] == 0
    assert data["taller_no_encontrado"] == 1
    assert data["insertados"] == 0


@pytest.mark.asyncio
async def test_delete_extra_endpoint(client, db_session):
    """DELETE /api/planificacion/{id}/extra removes EXTRA, 400s on BASE."""
    catalog_anchor, base_anchor = await _pick_two_talleres(db_session)

    ep_empresa_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DEL_EP")
    base_empresa_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DEL_BASE")
    await _set_config_trimestral(db_session, ep_empresa_id, escuela_propia=True)
    await _set_config_trimestral(db_session, base_empresa_id, escuela_propia=False)

    rows = [
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}DEL_BASE",
            "Taller": base_anchor["nombre"],
            "Programa": base_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}DEL_EP",
            "Taller": catalog_anchor["nombre"],
            "Programa": catalog_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
    ]
    excel = _build_extras_excel(rows)
    resp = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-bulk",
        files={"file": ("c.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        params={"dry_run": False, "wipe_first": False},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    extra_id = data["extras_detalle"][0]["planificacion_id"]

    # Find the BASE row to verify the 400 guard.
    base_row = await db_session.execute(
        text(
            'SELECT id FROM planificacion '
            "WHERE trimestre = :tri AND \"empresaId\" = :eid AND \"tipoAsignacion\" = 'BASE'"
        ),
        {"tri": TEST_TRIMESTRE, "eid": base_empresa_id},
    )
    base_id = base_row.scalar()
    assert base_id is not None

    # 400 because BASE row is not EXTRA.
    base_del = await client.delete(f"/api/planificacion/{base_id}/extra")
    assert base_del.status_code == 400

    # 404 for unknown id.
    nf_del = await client.delete("/api/planificacion/9999999/extra")
    assert nf_del.status_code == 404

    # 200 for the EXTRA row, then it's gone.
    ok_del = await client.delete(f"/api/planificacion/{extra_id}/extra")
    assert ok_del.status_code == 200, ok_del.text
    assert ok_del.json()["deleted_id"] == extra_id

    gone = await db_session.execute(
        text("SELECT id FROM planificacion WHERE id = :id"),
        {"id": extra_id},
    )
    assert gone.scalar() is None


@pytest.mark.asyncio
async def test_empresa_no_encontrada_is_hard_reject(client, db_session):
    """V20: a row whose Empresa name is unknown is rejected (no insert)."""
    catalog_anchor, base_anchor = await _pick_two_talleres(db_session)

    base_empresa_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}HARDREJ_BASE"
    )
    await _set_config_trimestral(db_session, base_empresa_id, escuela_propia=False)

    rows = [
        # Row 1: valid BASE row, should insert.
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}HARDREJ_BASE",
            "Taller": base_anchor["nombre"],
            "Programa": base_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
        # Row 2: empresa name is bogus → hard reject.
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}DOES_NOT_EXIST_XYZ",
            "Taller": base_anchor["nombre"],
            "Programa": base_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
    ]
    excel = _build_extras_excel(rows)

    resp = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-bulk",
        files={"file": ("c.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        params={"dry_run": False, "wipe_first": False},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    # Empresa-rejected row contributes ONLY to empresa_no_encontrada, not to insertados.
    assert data["empresa_no_encontrada"] == 1
    assert data["insertados"] == 1  # only the BASE row went in
    assert data["extras_insertados"] == 0
    assert data["taller_no_encontrado"] == 0
    # Mutual-exclusivity invariant.
    assert data["total_procesados"] == (
        data["insertados"]
        + data["vacantes"]
        + data["extras_insertados"]
        + data["empresa_no_encontrada"]
        + data["taller_no_encontrado"]
        + data["errores"]
    )

    # The bogus-empresa row must NOT exist in DB (no NULL-empresaId leak).
    nulls = await db_session.execute(
        text(
            'SELECT COUNT(*) FROM planificacion '
            'WHERE trimestre = :tri AND "empresaId" IS NULL '
            "AND estado = 'PLANIFICADO'"
        ),
        {"tri": TEST_TRIMESTRE},
    )
    assert nulls.scalar() == 0

    # And only the legit row landed for the test trimestre.
    total = await db_session.execute(
        text("SELECT COUNT(*) FROM planificacion WHERE trimestre = :tri"),
        {"tri": TEST_TRIMESTRE},
    )
    assert total.scalar() == 1


@pytest.mark.asyncio
async def test_get_extras_with_estado_filter(client, db_session):
    """V20: GET /extras?estado=PLANIFICADO filters EXTRAs by estado."""
    catalog_anchor, base_anchor = await _pick_two_talleres(db_session)

    ep_a = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}FILTER_EP_A")
    ep_b = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}FILTER_EP_B")
    base_a = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}FILTER_BASE_A")
    base_b = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}FILTER_BASE_B")
    await _set_config_trimestral(db_session, ep_a, escuela_propia=True)
    await _set_config_trimestral(db_session, ep_b, escuela_propia=True)
    await _set_config_trimestral(db_session, base_a, escuela_propia=False)
    await _set_config_trimestral(db_session, base_b, escuela_propia=False)

    # Two collisions in two different weeks → two EXTRAs with different estados.
    rows = [
        # Sem 1: BASE_A + EP_A as EXTRA (PLANIFICADO).
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}FILTER_BASE_A",
            "Taller": base_anchor["nombre"],
            "Programa": base_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
        {
            "Semana": 1,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}FILTER_EP_A",
            "Taller": catalog_anchor["nombre"],
            "Programa": catalog_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
        # Sem 2: BASE_B + EP_B as EXTRA (CANCELADO).
        {
            "Semana": 2,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}FILTER_BASE_B",
            "Taller": base_anchor["nombre"],
            "Programa": base_anchor["programa"],
            "Estado": "PLANIFICADO",
        },
        {
            "Semana": 2,
            "Día": base_anchor["diaSemana"],
            "Horario": base_anchor["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}FILTER_EP_B",
            "Taller": catalog_anchor["nombre"],
            "Programa": catalog_anchor["programa"],
            "Estado": "CANCELADO",
        },
    ]
    excel = _build_extras_excel(rows)
    resp = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-bulk",
        files={"file": ("c.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        params={"dry_run": False, "wipe_first": False},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["extras_insertados"] == 2

    # Without filter → both.
    all_resp = await client.get(f"/api/calendario/{TEST_TRIMESTRE}/extras")
    assert all_resp.status_code == 200
    assert all_resp.json()["total"] == 2

    # With ?estado=PLANIFICADO → only one.
    filt_resp = await client.get(
        f"/api/calendario/{TEST_TRIMESTRE}/extras",
        params={"estado": "PLANIFICADO"},
    )
    assert filt_resp.status_code == 200
    filt_data = filt_resp.json()
    assert filt_data["total"] == 1
    assert filt_data["extras"][0]["estado"] == "PLANIFICADO"
    assert filt_data["extras"][0]["empresa_nombre"] == f"{TEST_EMPRESA_PREFIX}FILTER_EP_A"

    # Repeated estado: ?estado=PLANIFICADO&estado=CANCELADO → both.
    multi_resp = await client.get(
        f"/api/calendario/{TEST_TRIMESTRE}/extras",
        params=[("estado", "PLANIFICADO"), ("estado", "CANCELADO")],
    )
    assert multi_resp.status_code == 200
    assert multi_resp.json()["total"] == 2

    # Invalid estado → 400.
    bad_resp = await client.get(
        f"/api/calendario/{TEST_TRIMESTRE}/extras",
        params={"estado": "INVALID"},
    )
    assert bad_resp.status_code == 400


@pytest.mark.asyncio
async def test_extra_classified_when_strict_match_and_ep_and_collision(client, db_session):
    """V20 hotfix: strict-match rows can also be EXTRA when EP+collision.

    Two rows on the SAME exact catalog tuple (sem, día, horario, taller, programa).
    Both match the catalog strictly. Empresa A has EP=true, Empresa B has EP=false.
    Expected: B inserts as BASE, A inserts as EXTRA.
    """
    taller = await _pick_one_taller(db_session)

    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}STRICT_EP")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}STRICT_BASE")
    await _set_config_trimestral(db_session, a_id, escuela_propia=True)
    await _set_config_trimestral(db_session, b_id, escuela_propia=False)

    rows = [
        {
            "Semana": 1,
            "Día": taller["diaSemana"],
            "Horario": taller["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}STRICT_BASE",
            "Taller": taller["nombre"],
            "Programa": taller["programa"],
            "Estado": "PLANIFICADO",
        },
        {
            "Semana": 1,
            "Día": taller["diaSemana"],
            "Horario": taller["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}STRICT_EP",
            "Taller": taller["nombre"],
            "Programa": taller["programa"],
            "Estado": "PLANIFICADO",
        },
    ]
    excel = _build_extras_excel(rows)
    resp = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-bulk",
        files={"file": ("c.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        params={"dry_run": False, "wipe_first": False},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["insertados"] == 1, data
    assert data["extras_insertados"] == 1, data
    assert data["total_procesados"] == 2
    assert data["total_procesados"] == (
        data["insertados"]
        + data["vacantes"]
        + data["extras_insertados"]
        + data["empresa_no_encontrada"]
        + data["taller_no_encontrado"]
        + data["errores"]
    )

    extra_id = data["extras_detalle"][0]["planificacion_id"]
    db_row = await db_session.execute(
        text(
            'SELECT "tipoAsignacion", "empresaId" FROM planificacion '
            "WHERE id = :id"
        ),
        {"id": extra_id},
    )
    rec = db_row.mappings().first()
    assert rec["tipoAsignacion"] == "EXTRA"
    assert rec["empresaId"] == a_id

    # GET /extras returns the strict-match EXTRA.
    list_resp = await client.get(f"/api/calendario/{TEST_TRIMESTRE}/extras")
    assert list_resp.status_code == 200
    list_data = list_resp.json()
    assert any(
        e["id"] == extra_id and e["empresa_nombre"] == f"{TEST_EMPRESA_PREFIX}STRICT_EP"
        for e in list_data["extras"]
    )


@pytest.mark.asyncio
async def test_two_ep_companies_collision(client, db_session):
    """V20 hotfix edge case: two EP companies colliding → BOTH go to EXTRA.

    By the rule (is_ep AND has_collision), each row sees the other as a
    collision and qualifies independently. There is no "first row wins as
    BASE" tiebreaker — that's deliberate; the rule is symmetric.

    Probably never happens in real Q2 data, but documented to lock in the
    semantics: insertados=0, extras_insertados=2.
    """
    taller = await _pick_one_taller(db_session)

    a_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}TWOEP_A")
    b_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}TWOEP_B")
    await _set_config_trimestral(db_session, a_id, escuela_propia=True)
    await _set_config_trimestral(db_session, b_id, escuela_propia=True)

    rows = [
        {
            "Semana": 1,
            "Día": taller["diaSemana"],
            "Horario": taller["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}TWOEP_A",
            "Taller": taller["nombre"],
            "Programa": taller["programa"],
            "Estado": "PLANIFICADO",
        },
        {
            "Semana": 1,
            "Día": taller["diaSemana"],
            "Horario": taller["horario"],
            "Empresa": f"{TEST_EMPRESA_PREFIX}TWOEP_B",
            "Taller": taller["nombre"],
            "Programa": taller["programa"],
            "Estado": "PLANIFICADO",
        },
    ]
    excel = _build_extras_excel(rows)
    resp = await client.post(
        f"/api/calendario/{TEST_TRIMESTRE}/importar-excel-bulk",
        files={"file": ("c.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        params={"dry_run": False, "wipe_first": False},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["insertados"] == 0
    assert data["extras_insertados"] == 2
    assert data["total_procesados"] == 2
    assert data["total_procesados"] == (
        data["insertados"]
        + data["vacantes"]
        + data["extras_insertados"]
        + data["empresa_no_encontrada"]
        + data["taller_no_encontrado"]
        + data["errores"]
    )

    # Both rows in DB tagged EXTRA.
    rows_db = await db_session.execute(
        text(
            'SELECT "tipoAsignacion" FROM planificacion '
            "WHERE trimestre = :tri "
            'AND "empresaId" IN (:a, :b)'
        ),
        {"tri": TEST_TRIMESTRE, "a": a_id, "b": b_id},
    )
    types = [r["tipoAsignacion"] for r in rows_db.mappings().all()]
    assert types == ["EXTRA", "EXTRA"]


# ── V21: POST /api/planificacion/{trimestre}/extra ─────────────


@pytest.mark.asyncio
async def test_crear_extra_ok_con_colision_y_empresa_ep(client, db_session):
    """POST creates an EXTRA when empresa is EP and there's a colliding row."""
    taller = await _pick_one_taller(db_session)

    base_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}POST_OK_BASE")
    ep_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}POST_OK_EP")
    await _set_config_trimestral(db_session, base_id, escuela_propia=False)
    await _set_config_trimestral(db_session, ep_id, escuela_propia=True)

    # Pre-existing BASE slot at (sem=1, día, horario) — the collision target.
    await _insert_base_slot(
        db_session,
        trimestre=TEST_TRIMESTRE,
        semana=1,
        dia=taller["diaSemana"],
        horario=taller["horario"],
        empresa_id=base_id,
        taller_id=taller["id"],
    )

    payload = {
        "empresa_id": ep_id,
        "semana": 1,
        "dia": taller["diaSemana"],
        "horario": taller["horario"],
        "taller_id": taller["id"],
        "programa": taller["programa"],
        "notas": "Slot extra IBERIA semana piloto",
    }
    resp = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra", json=payload
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    # Response shape (SlotExtraResponse).
    assert data["empresa_id"] == ep_id
    assert data["empresa_nombre"] == f"{TEST_EMPRESA_PREFIX}POST_OK_EP"
    assert data["taller_nombre"] == taller["nombre"]
    assert data["semana"] == 1
    assert data["dia"] == taller["diaSemana"]
    assert data["horario"] == taller["horario"]
    assert data["estado"] == "PLANIFICADO"
    assert data["confirmado"] is False
    assert data["notas"] == "Slot extra IBERIA semana piloto"
    assert data["motivo_cambio"] is None

    # DB shape: tipoAsignacion=EXTRA, empresaIdOriginal == empresa_id.
    db_row = await db_session.execute(
        text(
            'SELECT "tipoAsignacion", "empresaId", "empresaIdOriginal", '
            '"esContingencia", confirmado '
            "FROM planificacion WHERE id = :id"
        ),
        {"id": data["id"]},
    )
    rec = db_row.mappings().first()
    assert rec["tipoAsignacion"] == "EXTRA"
    assert rec["empresaId"] == ep_id
    assert rec["empresaIdOriginal"] == ep_id
    assert rec["esContingencia"] is False
    assert rec["confirmado"] is False


@pytest.mark.asyncio
async def test_crear_extra_rechaza_empresa_inexistente(client, db_session):
    taller = await _pick_one_taller(db_session)
    payload = {
        "empresa_id": 99999999,
        "semana": 1,
        "dia": taller["diaSemana"],
        "horario": taller["horario"],
        "taller_id": taller["id"],
        "programa": taller["programa"],
    }
    resp = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra", json=payload
    )
    assert resp.status_code == 404, resp.text
    assert "no existe" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_crear_extra_rechaza_empresa_inactiva(client, db_session):
    taller = await _pick_one_taller(db_session)

    inactiva_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}POST_INACTIVA"
    )
    # Mark inactive after creation.
    await db_session.execute(
        text("UPDATE empresa SET activa = false WHERE id = :id"),
        {"id": inactiva_id},
    )
    await db_session.commit()

    payload = {
        "empresa_id": inactiva_id,
        "semana": 1,
        "dia": taller["diaSemana"],
        "horario": taller["horario"],
        "taller_id": taller["id"],
        "programa": taller["programa"],
    }
    resp = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra", json=payload
    )
    assert resp.status_code == 422, resp.text
    assert "inactiva" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_crear_extra_rechaza_taller_inexistente(client, db_session):
    ep_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}POST_NOTALLER_EP"
    )
    await _set_config_trimestral(db_session, ep_id, escuela_propia=True)

    payload = {
        "empresa_id": ep_id,
        "semana": 1,
        "dia": "L",
        "horario": "09:00-10:30",
        "taller_id": 99999999,
        "programa": "EF",
    }
    resp = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra", json=payload
    )
    assert resp.status_code == 404, resp.text
    assert "taller" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_crear_extra_rechaza_empresa_no_ep(client, db_session):
    """Empresa exists & active but escuelaPropia=false → 422 with EP message."""
    taller = await _pick_one_taller(db_session)

    base_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}POST_NOEP_BASE"
    )
    no_ep_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}POST_NOEP_TARGET"
    )
    await _set_config_trimestral(db_session, base_id, escuela_propia=False)
    await _set_config_trimestral(db_session, no_ep_id, escuela_propia=False)

    # Set up a collision so it would otherwise be valid.
    await _insert_base_slot(
        db_session,
        trimestre=TEST_TRIMESTRE,
        semana=1,
        dia=taller["diaSemana"],
        horario=taller["horario"],
        empresa_id=base_id,
        taller_id=taller["id"],
    )

    payload = {
        "empresa_id": no_ep_id,
        "semana": 1,
        "dia": taller["diaSemana"],
        "horario": taller["horario"],
        "taller_id": taller["id"],
        "programa": taller["programa"],
    }
    resp = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra", json=payload
    )
    assert resp.status_code == 422, resp.text
    assert "escuela propia" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_crear_extra_rechaza_sin_colision(client, db_session):
    """Empresa is EP but nothing else at that slot → 422 with collision message."""
    taller = await _pick_one_taller(db_session)

    ep_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}POST_NOCOL_EP"
    )
    await _set_config_trimestral(db_session, ep_id, escuela_propia=True)

    payload = {
        "empresa_id": ep_id,
        "semana": 1,
        "dia": taller["diaSemana"],
        "horario": taller["horario"],
        "taller_id": taller["id"],
        "programa": taller["programa"],
    }
    resp = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra", json=payload
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"].lower()
    assert "coli" in detail  # "colisionar" / "colisión"
    assert "mismo horario" in detail


@pytest.mark.asyncio
async def test_crear_extra_rechaza_duplicado_exacto(client, db_session):
    """Creating the same EXTRA twice → 422 with duplicate message + existing id."""
    taller = await _pick_one_taller(db_session)

    base_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}POST_DUP_BASE"
    )
    ep_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}POST_DUP_EP")
    await _set_config_trimestral(db_session, base_id, escuela_propia=False)
    await _set_config_trimestral(db_session, ep_id, escuela_propia=True)

    await _insert_base_slot(
        db_session,
        trimestre=TEST_TRIMESTRE,
        semana=1,
        dia=taller["diaSemana"],
        horario=taller["horario"],
        empresa_id=base_id,
        taller_id=taller["id"],
    )

    payload = {
        "empresa_id": ep_id,
        "semana": 1,
        "dia": taller["diaSemana"],
        "horario": taller["horario"],
        "taller_id": taller["id"],
        "programa": taller["programa"],
    }

    # First call: succeeds.
    first = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra", json=payload
    )
    assert first.status_code == 200, first.text
    first_id = first.json()["id"]

    # Second call: same payload → 422 duplicate.
    second = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra", json=payload
    )
    assert second.status_code == 422, second.text
    detail = second.json()["detail"]
    assert "ya existe" in detail.lower()
    assert str(first_id) in detail  # existing id surfaced


# ── V21: PATCH /api/planificacion/{slot_id}/extra ──────────────


async def _create_extra_slot(db_session, client) -> tuple[int, int, dict]:
    """Helper: create one EXTRA via POST and return (extra_id, ep_id, taller).

    Yields back the empresa+taller ids so tests can build follow-up payloads.
    """
    taller = await _pick_one_taller(db_session)

    base_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PATCH_BASE"
    )
    ep_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}PATCH_EP")
    await _set_config_trimestral(db_session, base_id, escuela_propia=False)
    await _set_config_trimestral(db_session, ep_id, escuela_propia=True)

    await _insert_base_slot(
        db_session,
        trimestre=TEST_TRIMESTRE,
        semana=1,
        dia=taller["diaSemana"],
        horario=taller["horario"],
        empresa_id=base_id,
        taller_id=taller["id"],
    )

    resp = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/extra",
        json={
            "empresa_id": ep_id,
            "semana": 1,
            "dia": taller["diaSemana"],
            "horario": taller["horario"],
            "taller_id": taller["id"],
            "programa": taller["programa"],
            "notas": "nota inicial",
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["id"], ep_id, taller


@pytest.mark.asyncio
async def test_editar_extra_cambiar_empresa_a_otra_ep(client, db_session):
    extra_id, ep_id, _ = await _create_extra_slot(db_session, client)

    # New empresa, also EP.
    other_ep_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PATCH_OTHER_EP"
    )
    await _set_config_trimestral(db_session, other_ep_id, escuela_propia=True)

    resp = await client.patch(
        f"/api/planificacion/{extra_id}/extra",
        json={"empresa_id": other_ep_id},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["id"] == extra_id
    assert data["empresa_id"] == other_ep_id
    assert data["empresa_nombre"] == f"{TEST_EMPRESA_PREFIX}PATCH_OTHER_EP"
    assert data["notas"] == "nota inicial"  # untouched

    # empresaIdOriginal must remain the original empresa (write-once).
    db_row = await db_session.execute(
        text(
            'SELECT "empresaId", "empresaIdOriginal", "tipoAsignacion" '
            "FROM planificacion WHERE id = :id"
        ),
        {"id": extra_id},
    )
    rec = db_row.mappings().first()
    assert rec["empresaId"] == other_ep_id
    assert rec["empresaIdOriginal"] == ep_id  # untouched
    assert rec["tipoAsignacion"] == "EXTRA"


@pytest.mark.asyncio
async def test_editar_extra_solo_notas(client, db_session):
    extra_id, ep_id, _ = await _create_extra_slot(db_session, client)

    resp = await client.patch(
        f"/api/planificacion/{extra_id}/extra",
        json={"notas": "nota actualizada"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["notas"] == "nota actualizada"
    assert data["empresa_id"] == ep_id  # untouched


@pytest.mark.asyncio
async def test_editar_extra_notas_vacias_limpia(client, db_session):
    extra_id, _, _ = await _create_extra_slot(db_session, client)

    resp = await client.patch(
        f"/api/planificacion/{extra_id}/extra",
        json={"notas": ""},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["notas"] == ""

    # In DB the value is the empty string (not NULL).
    db_row = await db_session.execute(
        text("SELECT notas FROM planificacion WHERE id = :id"),
        {"id": extra_id},
    )
    assert db_row.scalar() == ""


@pytest.mark.asyncio
async def test_editar_extra_slot_inexistente(client, db_session):
    resp = await client.patch(
        "/api/planificacion/9999999/extra",
        json={"notas": "x"},
    )
    assert resp.status_code == 404, resp.text


@pytest.mark.asyncio
async def test_editar_extra_slot_es_base_no_extra(client, db_session):
    """PATCH on a BASE slot → 400 with 'no es EXTRA'."""
    taller = await _pick_one_taller(db_session)

    base_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PATCH_BASE_ONLY"
    )
    await _set_config_trimestral(db_session, base_id, escuela_propia=False)

    base_slot_id = await _insert_base_slot(
        db_session,
        trimestre=TEST_TRIMESTRE,
        semana=1,
        dia=taller["diaSemana"],
        horario=taller["horario"],
        empresa_id=base_id,
        taller_id=taller["id"],
    )

    resp = await client.patch(
        f"/api/planificacion/{base_slot_id}/extra",
        json={"notas": "x"},
    )
    assert resp.status_code == 400, resp.text
    assert "no es EXTRA" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_editar_extra_nueva_empresa_no_ep(client, db_session):
    extra_id, _, _ = await _create_extra_slot(db_session, client)

    no_ep_id = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}PATCH_NOEP"
    )
    await _set_config_trimestral(db_session, no_ep_id, escuela_propia=False)

    resp = await client.patch(
        f"/api/planificacion/{extra_id}/extra",
        json={"empresa_id": no_ep_id},
    )
    assert resp.status_code == 422, resp.text
    assert "escuela propia" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_editar_extra_body_vacio(client, db_session):
    extra_id, _, _ = await _create_extra_slot(db_session, client)

    resp = await client.patch(
        f"/api/planificacion/{extra_id}/extra",
        json={},
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert "al menos" in detail.lower()
