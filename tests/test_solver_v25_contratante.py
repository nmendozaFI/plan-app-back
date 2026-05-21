"""V25 Cambio C (Capa 7): regresión de la priorización contratante hard.

Verifica que empresas con `empresa.esContratante=true` reciben sus talleres
del catálogo (`taller.esContratante=true`) hasta cubrirlo, y el resto del
general. Decisión C7 (V25 §10): orden libre dentro del catálogo, sin
prelación entre los 5 talleres.

Tres casos canónicos (V25 §10 C7):
  - freq=3 (EF=3)            → 3 del catálogo  (target_cat=min(3,5)=3)
  - freq=5 (EF=3, IT=2)      → 5 del catálogo  (target_cat=min(5,5)=5)
  - freq=7 (EF=4, IT=3) + cat=5 (3EF+2IT) → 5 del catálogo + 2 del general

Trimestre artificial `2097-Q1` para evitar colisión con Capa 5 (2099-Q1) y
Capa 6 (2098-Q1). Cleanup manual por trimestre + `TEST_EMPRESA_PREFIX` +
snapshot/restore del flag `taller.esContratante` (la columna es global —
no per-trimestre — y Capa 10 aún no marcó talleres reales).
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from .conftest import TEST_EMPRESA_PREFIX


CONTRATANTE_TEST_TRIMESTRE = "2097-Q1"


async def _snapshot_catalogo(db) -> list[int]:
    """Devuelve los IDs de talleres marcados hoy con `esContratante=true`."""
    res = await db.execute(
        text('SELECT id FROM taller WHERE "esContratante" = true ORDER BY id')
    )
    return [row["id"] for row in res.mappings().all()]


async def _setup_catalogo(db, n_ef: int = 3, n_it: int = 2) -> list[int]:
    """Marca `n_ef` talleres EF + `n_it` talleres IT con esContratante=true.

    Antes resetea TODOS los talleres a `esContratante=false` para asegurar
    estado conocido del catálogo durante el test. Restaurar al estado previo
    es responsabilidad del finally (`_restore_catalogo`).
    """
    await db.execute(text('UPDATE taller SET "esContratante" = false'))

    res_ef = await db.execute(
        text(
            "SELECT id FROM taller "
            "WHERE programa = :p AND activo = true ORDER BY id LIMIT :n"
        ),
        {"p": "EF", "n": n_ef},
    )
    ef_ids = [row["id"] for row in res_ef.mappings().all()]
    res_it = await db.execute(
        text(
            "SELECT id FROM taller "
            "WHERE programa = :p AND activo = true ORDER BY id LIMIT :n"
        ),
        {"p": "IT", "n": n_it},
    )
    it_ids = [row["id"] for row in res_it.mappings().all()]
    cat_ids = ef_ids + it_ids

    if cat_ids:
        await db.execute(
            text('UPDATE taller SET "esContratante" = true WHERE id = ANY(:ids)'),
            {"ids": cat_ids},
        )
    await db.commit()
    return cat_ids


async def _restore_catalogo(db, prev_ids: list[int]) -> None:
    """Restaura el catálogo al estado previo al test. Idempotente."""
    try:
        await db.execute(text('UPDATE taller SET "esContratante" = false'))
        if prev_ids:
            await db.execute(
                text(
                    'UPDATE taller SET "esContratante" = true '
                    "WHERE id = ANY(:ids)"
                ),
                {"ids": prev_ids},
            )
        await db.commit()
    except Exception:
        pass


async def _cleanup_contratante(
    db, trimestre: str = CONTRATANTE_TEST_TRIMESTRE
) -> None:
    """Borra rastro del test en el trimestre + empresas con prefijo."""
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


async def _setup_empresa(
    db,
    nombre: str,
    ef: int,
    it: int = 0,
    es_contratante: bool = True,
) -> int:
    """Crea empresa + CT + frecuencia en CONTRATANTE_TEST_TRIMESTRE.

    `esNueva=false`, `escuelaPropia=false` (la regla contratante aplica a
    no-EP — EP concentra por H6b y el catálogo dejaría de tener sentido).
    """
    full_name = f"{TEST_EMPRESA_PREFIX}{nombre}"
    total = ef + it

    res = await db.execute(
        text(
            "INSERT INTO empresa "
            '(nombre, tipo, semaforo, activa, "esNueva", "esContratante", "updatedAt") '
            "VALUES (:n, 'AMBAS', 'VERDE', true, false, :ec, NOW()) "
            "ON CONFLICT (nombre) DO UPDATE SET "
            'activa = true, "esNueva" = false, "esContratante" = EXCLUDED."esContratante" '
            "RETURNING id"
        ),
        {"n": full_name, "ec": es_contratante},
    )
    eid = res.scalar()

    await db.execute(
        text(
            'INSERT INTO "configTrimestral" '
            '("empresaId", trimestre, "tipoParticipacion", "escuelaPropia", '
            '"frecuenciaEF", "frecuenciaIT", "disponibilidadDias", "updatedAt") '
            "VALUES (:eid, :tri, 'AMBAS', false, :ef, :it, 'L,M,X,J,V', NOW()) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE SET '
            '"escuelaPropia" = false, '
            '"frecuenciaEF" = EXCLUDED."frecuenciaEF", '
            '"frecuenciaIT" = EXCLUDED."frecuenciaIT"'
        ),
        {
            "eid": eid,
            "tri": CONTRATANTE_TEST_TRIMESTRE,
            "ef": ef,
            "it": it,
        },
    )

    cfg = await db.execute(
        text(
            'SELECT id FROM "configTrimestral" '
            'WHERE "empresaId" = :eid AND trimestre = :tri'
        ),
        {"eid": eid, "tri": CONTRATANTE_TEST_TRIMESTRE},
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
            "tri": CONTRATANTE_TEST_TRIMESTRE,
            "ef": ef,
            "it": it,
            "tot": total,
        },
    )

    await db.commit()
    return eid


@pytest.mark.asyncio
async def test_contratante_freq_3_todos_del_catalogo(client, db_session):
    """V25 Capa 7: empresa contratante freq=3 (EF=3) → los 3 del catálogo.

    target_cat = min(3, 5) = 3. La empresa gasta toda su frecuencia dentro
    del catálogo de contratantes.
    """
    prev_cat = await _snapshot_catalogo(db_session)
    await _cleanup_contratante(db_session)
    try:
        cat_ids = await _setup_catalogo(db_session, n_ef=3, n_it=2)
        eid = await _setup_empresa(
            db_session, "V25_CONT_FREQ3", ef=3, it=0, es_contratante=True,
        )

        resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": CONTRATANTE_TEST_TRIMESTRE, "timeout_seconds": 60},
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
        assert len(slots_empresa) == 3, (
            f"Esperaba 3 asignaciones, hay {len(slots_empresa)}"
        )
        for slot in slots_empresa:
            assert slot["taller_id"] in cat_ids, (
                f"Slot fuera del catálogo: taller_id={slot['taller_id']}, "
                f"catálogo={cat_ids}"
            )
    finally:
        await _cleanup_contratante(db_session)
        await _restore_catalogo(db_session, prev_cat)


@pytest.mark.asyncio
async def test_contratante_freq_5_cubre_catalogo_completo(client, db_session):
    """V25 Capa 7: empresa contratante freq=5 (EF=3, IT=2) → los 5 son
    exactamente los 5 del catálogo (3 EF + 2 IT).

    target_cat = min(5, 5) = 5. Sin slack para asignar talleres generales.
    """
    prev_cat = await _snapshot_catalogo(db_session)
    await _cleanup_contratante(db_session)
    try:
        cat_ids = await _setup_catalogo(db_session, n_ef=3, n_it=2)
        eid = await _setup_empresa(
            db_session, "V25_CONT_FREQ5", ef=3, it=2, es_contratante=True,
        )

        resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": CONTRATANTE_TEST_TRIMESTRE, "timeout_seconds": 60},
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
        assert len(slots_empresa) == 5, (
            f"Esperaba 5 asignaciones, hay {len(slots_empresa)}"
        )
        talleres_asignados = sorted(s["taller_id"] for s in slots_empresa)
        assert talleres_asignados == sorted(cat_ids), (
            f"Asignaciones no coinciden con el catálogo completo: "
            f"asignados={talleres_asignados}, catálogo={sorted(cat_ids)}"
        )
    finally:
        await _cleanup_contratante(db_session)
        await _restore_catalogo(db_session, prev_cat)


@pytest.mark.asyncio
async def test_contratante_freq_7_catalogo_mas_general(client, db_session):
    """V25 Capa 7: empresa contratante freq=7 (EF=4, IT=3) con cat=5 (3EF+2IT)
    → 5 del catálogo + 2 del general.

    target_cat = min(7, 5) = 5. Verifica el count, no el orden (Decisión C7:
    orden libre dentro del catálogo).
    """
    prev_cat = await _snapshot_catalogo(db_session)
    await _cleanup_contratante(db_session)
    try:
        cat_ids = await _setup_catalogo(db_session, n_ef=3, n_it=2)
        eid = await _setup_empresa(
            db_session, "V25_CONT_FREQ7", ef=4, it=3, es_contratante=True,
        )

        resp = await client.post(
            "/api/calendario/generar",
            json={"trimestre": CONTRATANTE_TEST_TRIMESTRE, "timeout_seconds": 60},
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
        assert len(slots_empresa) == 7, (
            f"Esperaba 7 asignaciones, hay {len(slots_empresa)}"
        )
        del_catalogo = sum(
            1 for s in slots_empresa if s["taller_id"] in cat_ids
        )
        del_general = sum(
            1 for s in slots_empresa if s["taller_id"] not in cat_ids
        )
        assert del_catalogo == 5, (
            f"Esperaba 5 del catálogo, hay {del_catalogo}. "
            f"Asignados: {[s['taller_id'] for s in slots_empresa]}, "
            f"catálogo: {cat_ids}"
        )
        assert del_general == 2, (
            f"Esperaba 2 del general, hay {del_general}"
        )
    finally:
        await _cleanup_contratante(db_session)
        await _restore_catalogo(db_session, prev_cat)
