"""V22 (Cambio A) + V25 Cambio C (Capa 3): tests for the DOBLE CRUD endpoints.

DOBLE is ad-hoc. V25 Capa 3 migró el gate de elegibilidad de
`CT.escuelaPropia=true` al flag estructural `empresa.puedeSerDoble=true`
(ficha persistente). La regla de decisiones 3 & 4 sigue intacta: NO collision
check, NO programa coherence, NO duplicate check. PATCH allows changing
semana/día/horario/empresa/taller. The bulk cleanup
`DELETE /{trimestre}/extras-doble` wipes EXTRA+DOBLE but never touches
BASE/CONTINGENCIA.

Module-scoped engine cascade-fail (deuda 6) means these tests should be run
one at a time when many touch the same DB. Each test is self-contained.
"""

import pytest
from sqlalchemy import text

from .conftest import TEST_TRIMESTRE, TEST_EMPRESA_PREFIX


# ── Helpers (local — duplicates pattern from test_planificacion_extras.py) ──


async def _create_empresa(db, nombre: str, activa: bool = True) -> int:
    res = await db.execute(
        text(
            'INSERT INTO empresa (nombre, tipo, activa, "updatedAt") '
            "VALUES (:n, 'AMBAS', :a, NOW()) "
            'ON CONFLICT (nombre) DO UPDATE SET activa = EXCLUDED.activa '
            "RETURNING id"
        ),
        {"n": nombre, "a": activa},
    )
    eid = res.scalar()
    await db.commit()
    return eid


async def _set_config_trimestral(
    db,
    empresa_id: int,
    escuela_propia: bool,
    *,
    permite_extras: bool = False,
):
    await db.execute(
        text(
            'INSERT INTO "configTrimestral" '
            '("empresaId", trimestre, "tipoParticipacion", "escuelaPropia", '
            '"permiteExtras", "disponibilidadDias", "updatedAt") '
            "VALUES (:eid, :tri, 'AMBAS', :ep, :pe, 'L,M,X,J,V', NOW()) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE '
            'SET "escuelaPropia" = EXCLUDED."escuelaPropia", '
            '    "permiteExtras" = EXCLUDED."permiteExtras"'
        ),
        {
            "eid": empresa_id,
            "tri": TEST_TRIMESTRE,
            "ep": escuela_propia,
            "pe": permite_extras,
        },
    )
    await db.commit()


async def _set_puede_ser_doble(db, empresa_id: int, value: bool = True):
    """V25 Cambio C (Capa 3): setea el flag estructural en la ficha de
    empresa. Reemplaza el viejo patrón de "marcar CT.escuelaPropia=true para
    habilitar Doble" — ahora el gate es por ficha, no por trimestre."""
    await db.execute(
        text('UPDATE empresa SET "puedeSerDoble" = :v WHERE id = :id'),
        {"id": empresa_id, "v": value},
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


async def _pick_two_talleres_any(db) -> tuple[dict, dict]:
    """Pick any two distinct active talleres (programa may differ — DOBLE doesn't care)."""
    res = await db.execute(
        text(
            'SELECT id, nombre, "diaSemana", horario, programa '
            "FROM taller WHERE activo = true AND \"diaSemana\" IS NOT NULL "
            "AND horario IS NOT NULL ORDER BY id LIMIT 2"
        ),
    )
    rows = [dict(r) for r in res.mappings().all()]
    if len(rows) < 2:
        pytest.skip("Need at least 2 active talleres")
    return rows[0], rows[1]


async def _insert_slot(
    db,
    *,
    semana: int,
    dia: str,
    horario: str,
    empresa_id: int,
    taller_id: int,
    tipo: str = "BASE",
) -> int:
    """Insert a planificacion row directly with the given tipoAsignacion."""
    res = await db.execute(
        text(
            '''
            INSERT INTO planificacion (
                trimestre, semana, dia, horario, turno,
                "empresaId", "empresaIdOriginal", "tallerId",
                "tipoAsignacion", "esContingencia", estado, confirmado,
                "updatedAt"
            ) VALUES (
                :tri, :sem, :dia, :horario, 'M',
                :eid, :eid, :tid,
                :tipo, false, 'PLANIFICADO', false,
                NOW()
            ) RETURNING id
            '''
        ),
        {
            "tri": TEST_TRIMESTRE,
            "sem": semana,
            "dia": dia,
            "horario": horario,
            "eid": empresa_id,
            "tid": taller_id,
            "tipo": tipo,
        },
    )
    sid = res.scalar()
    await db.commit()
    return sid


# ── POST /doble ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_crear_doble_ok_con_empresa_ep(client, db_session):
    """Happy path: empresa with escuelaPropia=true + valid taller → 200, DOBLE row."""
    taller = await _pick_one_taller(db_session)
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_OK")
    await _set_puede_ser_doble(db_session, eid)

    body = {
        "empresa_id": eid,
        "semana": 3,
        "dia": "L",
        "horario": "10:00",
        "taller_id": taller["id"],
        "notas": "IBERIA semana intensiva",
    }
    resp = await client.post(f"/api/planificacion/{TEST_TRIMESTRE}/doble", json=body)
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["empresa_id"] == eid
    assert data["semana"] == 3
    assert data["dia"] == "L"
    assert data["horario"] == "10:00"
    assert data["taller_id"] == taller["id"]
    assert data["taller_nombre"] == taller["nombre"]
    assert data["notas"] == "IBERIA semana intensiva"
    assert data["estado"] == "PLANIFICADO"
    assert data["confirmado"] is False
    assert data["id"] > 0

    # DB cross-check.
    row = await db_session.execute(
        text('SELECT "tipoAsignacion" FROM planificacion WHERE id = :id'),
        {"id": data["id"]},
    )
    assert row.scalar() == "DOBLE"


@pytest.mark.asyncio
async def test_crear_doble_rechaza_empresa_inexistente(client, db_session):
    taller = await _pick_one_taller(db_session)
    body = {
        "empresa_id": 99999999,
        "semana": 1,
        "dia": "L",
        "horario": "10:00",
        "taller_id": taller["id"],
    }
    resp = await client.post(f"/api/planificacion/{TEST_TRIMESTRE}/doble", json=body)
    assert resp.status_code == 404
    assert "no existe" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_crear_doble_rechaza_empresa_inactiva(client, db_session):
    taller = await _pick_one_taller(db_session)
    eid = await _create_empresa(
        db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_INACTIVE", activa=False,
    )
    await _set_puede_ser_doble(db_session, eid)

    body = {
        "empresa_id": eid,
        "semana": 1,
        "dia": "L",
        "horario": "10:00",
        "taller_id": taller["id"],
    }
    resp = await client.post(f"/api/planificacion/{TEST_TRIMESTRE}/doble", json=body)
    assert resp.status_code == 422
    assert "inactiva" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_crear_doble_rechaza_empresa_no_puede_ser_doble(client, db_session):
    """V25 Cambio C (Capa 3): el gate del POST /doble es `empresa.puedeSerDoble`.
    Empresa sin ese flag → 422, sin importar el estado de CT."""
    taller = await _pick_one_taller(db_session)
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_NODOBLE")
    # NO se pinta puedeSerDoble en la ficha (default false).
    # Una CT con escuelaPropia=true es deliberadamente irrelevante ahora —
    # el gate vive en empresa, no en CT.
    await _set_config_trimestral(db_session, eid, escuela_propia=True)

    body = {
        "empresa_id": eid,
        "semana": 1,
        "dia": "L",
        "horario": "10:00",
        "taller_id": taller["id"],
    }
    resp = await client.post(f"/api/planificacion/{TEST_TRIMESTRE}/doble", json=body)
    assert resp.status_code == 422
    assert "puedeserdoble" in resp.json()["detail"].lower().replace(" ", "")


@pytest.mark.asyncio
async def test_crear_doble_rechaza_taller_inexistente(client, db_session):
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_NOTALLER")
    await _set_puede_ser_doble(db_session, eid)

    body = {
        "empresa_id": eid,
        "semana": 1,
        "dia": "L",
        "horario": "10:00",
        "taller_id": 99999999,
    }
    resp = await client.post(f"/api/planificacion/{TEST_TRIMESTRE}/doble", json=body)
    assert resp.status_code == 404
    assert "taller" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_crear_doble_no_valida_colision(client, db_session):
    """Decision 4: DOBLE doesn't validate collisions.

    A BASE row already at (sem, dia, horario) for empresa B does NOT prevent
    empresa A from creating a DOBLE there.
    """
    taller = await _pick_one_taller(db_session)
    ep_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_COL_EP")
    other_id = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_COL_OTHER")
    await _set_puede_ser_doble(db_session, ep_id)
    # other_id: no requiere puedeSerDoble (no participa en la creación DOBLE).
    # Mantenemos una CT con escuela_propia=false por simetría con el test
    # original — el solver no se entera (Capa 5).
    await _set_config_trimestral(db_session, other_id, escuela_propia=False)

    # Pre-existing BASE row for the OTHER empresa at the same slot.
    await _insert_slot(
        db_session,
        semana=2, dia="M", horario="11:00",
        empresa_id=other_id, taller_id=taller["id"],
        tipo="BASE",
    )

    body = {
        "empresa_id": ep_id,
        "semana": 2,
        "dia": "M",
        "horario": "11:00",
        "taller_id": taller["id"],
    }
    resp = await client.post(f"/api/planificacion/{TEST_TRIMESTRE}/doble", json=body)
    assert resp.status_code == 200, resp.text  # NO 422 colisión.


@pytest.mark.asyncio
async def test_crear_doble_no_valida_duplicado(client, db_session):
    """Decision 4: two identical DOBLE rows are allowed."""
    taller = await _pick_one_taller(db_session)
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_DUP")
    await _set_puede_ser_doble(db_session, eid)

    body = {
        "empresa_id": eid,
        "semana": 4,
        "dia": "X",
        "horario": "09:00",
        "taller_id": taller["id"],
    }
    r1 = await client.post(f"/api/planificacion/{TEST_TRIMESTRE}/doble", json=body)
    r2 = await client.post(f"/api/planificacion/{TEST_TRIMESTRE}/doble", json=body)
    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    assert r1.json()["id"] != r2.json()["id"]


# ── GET /dobles ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_listar_dobles_filtros(client, db_session):
    """GET ?semana= and ?empresa_id= narrow the list correctly."""
    taller = await _pick_one_taller(db_session)
    a = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_LIST_A")
    b = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_LIST_B")
    await _set_puede_ser_doble(db_session, a)
    await _set_puede_ser_doble(db_session, b)

    # 2 DOBLEs for A (sem 5 + sem 6) and 1 DOBLE for B (sem 5).
    for sem, emp in [(5, a), (6, a), (5, b)]:
        resp = await client.post(
            f"/api/planificacion/{TEST_TRIMESTRE}/doble",
            json={
                "empresa_id": emp, "semana": sem, "dia": "L",
                "horario": "10:00", "taller_id": taller["id"],
            },
        )
        assert resp.status_code == 200, resp.text

    # ?semana=5 → 2 (a + b).
    r_s = await client.get(
        f"/api/planificacion/{TEST_TRIMESTRE}/dobles", params={"semana": 5},
    )
    assert r_s.status_code == 200
    ids_s = {(d["empresa_id"], d["semana"]) for d in r_s.json()["dobles"]}
    assert (a, 5) in ids_s and (b, 5) in ids_s

    # ?empresa_id=a → 2 (sem 5 + sem 6).
    r_e = await client.get(
        f"/api/planificacion/{TEST_TRIMESTRE}/dobles", params={"empresa_id": a},
    )
    assert r_e.status_code == 200
    sems = sorted(d["semana"] for d in r_e.json()["dobles"])
    assert sems == [5, 6]

    # ?semana=5&empresa_id=a → 1.
    r_se = await client.get(
        f"/api/planificacion/{TEST_TRIMESTRE}/dobles",
        params={"semana": 5, "empresa_id": a},
    )
    assert r_se.status_code == 200
    assert r_se.json()["total"] == 1


# ── PATCH /doble ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_editar_doble_libertad_total(client, db_session):
    """Decision 4: PATCH can move empresa, taller, semana, día, horario, notas."""
    t1, t2 = await _pick_two_talleres_any(db_session)
    a = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_EDIT_A")
    b = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_EDIT_B")
    await _set_puede_ser_doble(db_session, a)
    await _set_puede_ser_doble(db_session, b)

    created = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/doble",
        json={
            "empresa_id": a, "semana": 7, "dia": "J",
            "horario": "10:00", "taller_id": t1["id"], "notas": "v1",
        },
    )
    assert created.status_code == 200
    slot_id = created.json()["id"]

    patched = await client.patch(
        f"/api/planificacion/{slot_id}/doble",
        json={
            "empresa_id": b,
            "taller_id": t2["id"],
            "semana": 8,
            "dia": "V",
            "horario": "12:00",
            "notas": "v2",
        },
    )
    assert patched.status_code == 200, patched.text
    d = patched.json()
    assert d["empresa_id"] == b
    assert d["taller_id"] == t2["id"]
    assert d["semana"] == 8
    assert d["dia"] == "V"
    assert d["horario"] == "12:00"
    assert d["notas"] == "v2"


@pytest.mark.asyncio
async def test_editar_doble_body_vacio_es_422(client, db_session):
    t = await _pick_one_taller(db_session)
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_EMPTY")
    await _set_puede_ser_doble(db_session, eid)
    created = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/doble",
        json={
            "empresa_id": eid, "semana": 9, "dia": "L",
            "horario": "10:00", "taller_id": t["id"],
        },
    )
    slot_id = created.json()["id"]
    resp = await client.patch(f"/api/planificacion/{slot_id}/doble", json={})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_editar_doble_slot_inexistente(client, db_session):
    resp = await client.patch(
        "/api/planificacion/99999999/doble", json={"notas": "x"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_editar_doble_slot_no_es_doble(client, db_session):
    """A BASE row cannot be PATCHed via /doble — must be DOBLE."""
    t = await _pick_one_taller(db_session)
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_NOTDOBLE")
    await _set_puede_ser_doble(db_session, eid)
    base_id = await _insert_slot(
        db_session,
        semana=1, dia="L", horario="10:00",
        empresa_id=eid, taller_id=t["id"],
        tipo="BASE",
    )
    resp = await client.patch(
        f"/api/planificacion/{base_id}/doble", json={"notas": "x"},
    )
    assert resp.status_code == 400
    assert "no es DOBLE" in resp.json()["detail"]


# ── DELETE /doble ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_eliminar_doble_ok(client, db_session):
    t = await _pick_one_taller(db_session)
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_DEL")
    await _set_puede_ser_doble(db_session, eid)
    created = await client.post(
        f"/api/planificacion/{TEST_TRIMESTRE}/doble",
        json={
            "empresa_id": eid, "semana": 10, "dia": "L",
            "horario": "10:00", "taller_id": t["id"],
        },
    )
    slot_id = created.json()["id"]

    resp = await client.delete(f"/api/planificacion/{slot_id}/doble")
    assert resp.status_code == 200
    assert resp.json()["deleted_id"] == slot_id
    assert resp.json()["tipo_asignacion"] == "DOBLE"

    gone = await db_session.execute(
        text("SELECT id FROM planificacion WHERE id = :id"), {"id": slot_id},
    )
    assert gone.scalar() is None


@pytest.mark.asyncio
async def test_eliminar_doble_no_borra_extra(client, db_session):
    """The DELETE /doble guard rejects EXTRA rows with 400."""
    t = await _pick_one_taller(db_session)
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_GUARD")
    await _set_puede_ser_doble(db_session, eid)
    extra_id = await _insert_slot(
        db_session,
        semana=11, dia="X", horario="10:00",
        empresa_id=eid, taller_id=t["id"],
        tipo="EXTRA",
    )
    resp = await client.delete(f"/api/planificacion/{extra_id}/doble")
    assert resp.status_code == 400
    assert "no es DOBLE" in resp.json()["detail"]

    # EXTRA row still in DB.
    still = await db_session.execute(
        text("SELECT id FROM planificacion WHERE id = :id"), {"id": extra_id},
    )
    assert still.scalar() == extra_id


# ── DELETE /extras-doble (bulk cleanup) ────────────────────────


@pytest.mark.asyncio
async def test_cleanup_extras_doble_sin_confirmar_es_400(client, db_session):
    resp = await client.delete(f"/api/planificacion/{TEST_TRIMESTRE}/extras-doble")
    assert resp.status_code == 400
    assert "confirmar" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_cleanup_extras_doble_borra_extra_y_doble_pero_no_base(client, db_session):
    """confirmar=true → wipes only EXTRA and DOBLE; BASE survives."""
    t = await _pick_one_taller(db_session)
    eid = await _create_empresa(db_session, f"{TEST_EMPRESA_PREFIX}DOBLE_CLEAN")
    await _set_puede_ser_doble(db_session, eid)

    base_id = await _insert_slot(
        db_session,
        semana=12, dia="L", horario="09:00",
        empresa_id=eid, taller_id=t["id"],
        tipo="BASE",
    )
    extra_id = await _insert_slot(
        db_session,
        semana=12, dia="M", horario="09:00",
        empresa_id=eid, taller_id=t["id"],
        tipo="EXTRA",
    )
    doble_id = await _insert_slot(
        db_session,
        semana=12, dia="X", horario="09:00",
        empresa_id=eid, taller_id=t["id"],
        tipo="DOBLE",
    )

    resp = await client.delete(
        f"/api/planificacion/{TEST_TRIMESTRE}/extras-doble",
        params={"confirmar": "true"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["trimestre"] == TEST_TRIMESTRE
    assert data["confirmar"] is True
    assert data["extras_eliminados"] >= 1
    assert data["dobles_eliminados"] >= 1

    # BASE survives, EXTRA + DOBLE gone.
    surv = await db_session.execute(
        text(
            'SELECT id, "tipoAsignacion" FROM planificacion '
            'WHERE id IN (:b, :e, :d)'
        ),
        {"b": base_id, "e": extra_id, "d": doble_id},
    )
    rows = {r["id"]: r["tipoAsignacion"] for r in surv.mappings().all()}
    assert rows.get(base_id) == "BASE"
    assert extra_id not in rows
    assert doble_id not in rows
