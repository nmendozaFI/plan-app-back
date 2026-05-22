"""V26 (edición unificada de slots) — tests de los endpoints PATCH/DELETE/POST
genéricos sobre `planificacion` desde el router calendario.

Cobertura:
  - PATCH slots/{id} con taller_id → JOIN devuelve programa/nombre nuevos.
  - DELETE slots/{id} sobre BASE y EXTRA (sin restricción por tipoAsignacion).
  - DELETE bloqueado con 409 si el trimestre ya tiene historicoTaller.
  - POST slots con tipo_asignacion='BASE' sin gate permiteExtras.
  - POST slots rechazado con 409 si la franja ya tiene los 2 programas.

Trimestre artificial `2095-Q1` para evitar colisiones con suites anteriores
(`2099-Q1` Capa 5, `2098-Q1` Capa 6, `2097-Q1` Capa 7, `2096-Q1` reservado).
Cleanup manual por trimestre + TEST_EMPRESA_PREFIX.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from .conftest import TEST_EMPRESA_PREFIX


EDIT_TRIMESTRE = "2095-Q1"


async def _cleanup_edit(db) -> None:
    """Borra rastro del test en el trimestre + empresas TEST_EMPRESA_*."""
    for sql in (
        "DELETE FROM planificacion WHERE trimestre = :tri",
        'DELETE FROM "historicoTaller" WHERE trimestre = :tri',
        'DELETE FROM "configTrimestral" WHERE trimestre = :tri',
        "DELETE FROM frecuencia WHERE trimestre = :tri",
    ):
        try:
            await db.execute(text(sql), {"tri": EDIT_TRIMESTRE})
        except Exception:
            pass
    try:
        await db.execute(
            text("DELETE FROM empresa WHERE nombre LIKE :p"),
            {"p": f"{TEST_EMPRESA_PREFIX}%"},
        )
    except Exception:
        pass
    await db.commit()


async def _setup_empresa(
    db,
    nombre: str,
    *,
    permite_extras: bool | None = None,
) -> int:
    """Crea empresa activa. Si `permite_extras` no es None, crea CT con ese flag."""
    res = await db.execute(
        text(
            "INSERT INTO empresa (nombre, tipo, activa, \"updatedAt\") "
            "VALUES (:n, 'AMBAS', true, NOW()) "
            "ON CONFLICT (nombre) DO UPDATE SET activa = true "
            "RETURNING id"
        ),
        {"n": f"{TEST_EMPRESA_PREFIX}{nombre}"},
    )
    eid = res.scalar()
    if permite_extras is not None:
        await db.execute(
            text(
                'INSERT INTO "configTrimestral" '
                '("empresaId", trimestre, "tipoParticipacion", "permiteExtras", '
                ' "disponibilidadDias", "updatedAt") '
                "VALUES (:eid, :tri, 'AMBAS', :pe, 'L,M,X,J,V', NOW()) "
                'ON CONFLICT ("empresaId", trimestre) DO UPDATE SET '
                '"permiteExtras" = EXCLUDED."permiteExtras"'
            ),
            {"eid": eid, "tri": EDIT_TRIMESTRE, "pe": permite_extras},
        )
    await db.commit()
    return eid


async def _pick_taller(db, programa: str) -> dict:
    """Selecciona el primer taller activo del programa pedido."""
    r = await db.execute(
        text(
            'SELECT id, nombre, programa, turno, horario, "diaSemana" AS dia '
            "FROM taller WHERE programa = :p AND activo = true "
            "ORDER BY id LIMIT 1"
        ),
        {"p": programa},
    )
    row = r.mappings().first()
    assert row is not None, f"No hay taller {programa} activo en BD"
    return dict(row)


async def _insert_slot(
    db,
    eid: int,
    taller: dict,
    *,
    tipo: str = "BASE",
    semana: int = 1,
    dia: str | None = None,
    horario: str | None = None,
) -> int:
    r = await db.execute(
        text(
            "INSERT INTO planificacion "
            '("empresaId", "empresaIdOriginal", "tallerId", trimestre, semana, dia, '
            ' horario, turno, "tipoAsignacion", estado, confirmado, "updatedAt") '
            "VALUES (:eid, :eid, :tid, :tri, :sem, :dia, :hora, :turno, "
            ":tipo, 'PLANIFICADO', false, NOW()) "
            "RETURNING id"
        ),
        {
            "eid": eid,
            "tid": taller["id"],
            "tri": EDIT_TRIMESTRE,
            "sem": semana,
            "dia": dia or taller["dia"],
            "hora": horario or taller["horario"],
            "turno": taller["turno"],
            "tipo": tipo,
        },
    )
    sid = r.scalar()
    await db.commit()
    return sid


# ─────────────────────────────────────────────────────────────
# PATCH: cambio de taller
# ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_patch_slot_cambia_taller(client, db_session):
    """V26: PATCH con taller_id distinto al actual → BD actualizada,
    response trae programa/nombre derivados del nuevo taller."""
    await _cleanup_edit(db_session)
    try:
        eid = await _setup_empresa(db_session, "V26_PATCH_TALLER")
        taller_orig = await _pick_taller(db_session, "EF")
        taller_nuevo = await _pick_taller(db_session, "IT")
        sid = await _insert_slot(db_session, eid, taller_orig, tipo="BASE")

        resp = await client.patch(
            f"/api/calendario/{EDIT_TRIMESTRE}/slots/{sid}",
            json={"taller_id": taller_nuevo["id"]},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["slot"]
        assert data["taller_id"] == taller_nuevo["id"]
        assert data["taller_nombre"] == taller_nuevo["nombre"]
        assert data["programa"] == taller_nuevo["programa"]

        # BD cross-check.
        r = await db_session.execute(
            text('SELECT "tallerId" FROM planificacion WHERE id = :id'),
            {"id": sid},
        )
        assert r.scalar() == taller_nuevo["id"]
    finally:
        await _cleanup_edit(db_session)


# ─────────────────────────────────────────────────────────────
# DELETE genérico
# ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_slot_base(client, db_session):
    """V26: DELETE genérico borra slots BASE (antes solo el /extra borraba)."""
    await _cleanup_edit(db_session)
    try:
        eid = await _setup_empresa(db_session, "V26_DEL_BASE")
        taller = await _pick_taller(db_session, "EF")
        sid = await _insert_slot(db_session, eid, taller, tipo="BASE")

        resp = await client.delete(
            f"/api/calendario/{EDIT_TRIMESTRE}/slots/{sid}",
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["deleted_id"] == sid
        assert resp.json()["tipo_asignacion"] == "BASE"

        # BD: la fila desapareció.
        r = await db_session.execute(
            text("SELECT id FROM planificacion WHERE id = :id"),
            {"id": sid},
        )
        assert r.first() is None
    finally:
        await _cleanup_edit(db_session)


@pytest.mark.asyncio
async def test_delete_slot_extra(client, db_session):
    """V26: DELETE genérico también borra EXTRA (cumple decisión 'cualquier
    tipoAsignacion, sin restricción')."""
    await _cleanup_edit(db_session)
    try:
        eid = await _setup_empresa(db_session, "V26_DEL_EXTRA")
        taller = await _pick_taller(db_session, "IT")
        sid = await _insert_slot(db_session, eid, taller, tipo="EXTRA")

        resp = await client.delete(
            f"/api/calendario/{EDIT_TRIMESTRE}/slots/{sid}",
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["tipo_asignacion"] == "EXTRA"
    finally:
        await _cleanup_edit(db_session)


@pytest.mark.asyncio
async def test_delete_slot_en_historico_rechaza(client, db_session):
    """V26: DELETE en trimestre con historicoTaller → 409 (cierre inmutable)."""
    await _cleanup_edit(db_session)
    try:
        eid = await _setup_empresa(db_session, "V26_DEL_HIST")
        taller = await _pick_taller(db_session, "EF")
        sid = await _insert_slot(db_session, eid, taller, tipo="BASE")

        # Simular trimestre cerrado: insertar una fila en historicoTaller
        # con el mismo trimestre. El schema requiere empresaId + tallerId +
        # estado entre los enums válidos.
        await db_session.execute(
            text(
                'INSERT INTO "historicoTaller" '
                '("empresaId", "tallerId", trimestre, fecha, estado, "createdAt") '
                "VALUES (:eid, :tid, :tri, '2095-01-01', 'OK', NOW())"
            ),
            {"eid": eid, "tid": taller["id"], "tri": EDIT_TRIMESTRE},
        )
        await db_session.commit()

        resp = await client.delete(
            f"/api/calendario/{EDIT_TRIMESTRE}/slots/{sid}",
        )
        assert resp.status_code == 409, resp.text
        assert "cerrado" in resp.json()["detail"].lower()

        # BD: el slot sigue ahí.
        r = await db_session.execute(
            text("SELECT id FROM planificacion WHERE id = :id"),
            {"id": sid},
        )
        assert r.first() is not None
    finally:
        await _cleanup_edit(db_session)


# ─────────────────────────────────────────────────────────────
# POST genérico
# ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_post_slot_libre_con_tipoasignacion_base(client, db_session):
    """V26: POST con tipo_asignacion='BASE' no requiere permiteExtras."""
    await _cleanup_edit(db_session)
    try:
        # Empresa SIN permiteExtras configurado.
        eid = await _setup_empresa(db_session, "V26_POST_BASE")
        taller = await _pick_taller(db_session, "EF")

        resp = await client.post(
            f"/api/calendario/{EDIT_TRIMESTRE}/slots",
            json={
                "empresa_id": eid,
                "semana": 1,
                "dia": taller["dia"],
                "horario": taller["horario"],
                "taller_id": taller["id"],
                "programa": taller["programa"],
                "tipo_asignacion": "BASE",
                "notas": "creado desde test V26",
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["tipo_asignacion"] == "BASE"
        assert body["empresa_id"] == eid
        assert body["taller_id"] == taller["id"]

        # BD cross-check.
        r = await db_session.execute(
            text(
                'SELECT "tipoAsignacion" FROM planificacion '
                'WHERE id = :id'
            ),
            {"id": body["id"]},
        )
        assert r.scalar() == "BASE"
    finally:
        await _cleanup_edit(db_session)


@pytest.mark.asyncio
async def test_post_slot_franja_llena_rechaza(client, db_session):
    """V26: la franja (semana, día, horario) tolera 1 EF + 1 IT como mucho.
    Un tercer slot o un mismo programa duplicado → 409."""
    await _cleanup_edit(db_session)
    try:
        eid_a = await _setup_empresa(db_session, "V26_POST_FULL_A")
        eid_b = await _setup_empresa(db_session, "V26_POST_FULL_B")
        eid_c = await _setup_empresa(db_session, "V26_POST_FULL_C")
        taller_ef = await _pick_taller(db_session, "EF")
        taller_it = await _pick_taller(db_session, "IT")

        # Sembrar la franja con EF + IT en (sem=1, día, horario=EF). Para
        # forzar el mismo horario en ambos talleres, usamos el horario de EF.
        comun_dia = taller_ef["dia"]
        comun_horario = taller_ef["horario"]
        await _insert_slot(
            db_session, eid_a, taller_ef,
            tipo="BASE", semana=1, dia=comun_dia, horario=comun_horario,
        )
        await _insert_slot(
            db_session, eid_b, taller_it,
            tipo="BASE", semana=1, dia=comun_dia, horario=comun_horario,
        )

        # Intento 1: mismo programa que el EF ya ocupado → 409.
        resp = await client.post(
            f"/api/calendario/{EDIT_TRIMESTRE}/slots",
            json={
                "empresa_id": eid_c,
                "semana": 1,
                "dia": comun_dia,
                "horario": comun_horario,
                "taller_id": taller_ef["id"],
                "programa": "EF",
                "tipo_asignacion": "BASE",
            },
        )
        assert resp.status_code == 409, resp.text
        assert "franja" in resp.json()["detail"].lower()
    finally:
        await _cleanup_edit(db_session)
