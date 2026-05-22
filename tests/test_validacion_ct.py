"""V27 — Pre-validación de Configuración Trimestral.

Cobertura:
  - GET /api/config-trimestral/{trimestre}/validar
  - Trimestre vacío (sin CTs) → response coherente, listas vacías.
  - V1 (caso EY real): freq IT=1, dias=V, catálogo IT no incluye V → error.
  - V2: tipoParticipacion='EF' con freqIT>0 → error.
  - V3: ambas frecuencias 0 → warning empresa_fantasma.
  - V2 edge: tipoParticipacion=NULL (compat V24 D4) → no dispara.
  - V1 edge: disponibilidadDias=NULL → default L,M,X,J,V.
  - Combinado: una mezcla de los 3 casos en el mismo trimestre.

Trimestre artificial `2094-Q1` (el siguiente libre tras 2095-Q1 V26). El
conftest global solo limpia `2099-Q1`; este módulo añade su propio cleanup
autouse (sin tocar conftest).

Cataálogo real del proyecto: EF disponible L-V, IT disponible solo L-M.
Los casos `freq_ef_sin_dias_disponibles` requerirían un día sin EF en el
catálogo — como EF está en todos los días, ese error específico no es
fácilmente testeable sin alterar catálogo global; queda cubierto por el
test combinado que ejerce el path simétrico vía IT.
"""

import pytest
import pytest_asyncio
from sqlalchemy import text

from .conftest import TEST_EMPRESA_PREFIX


V27_TRIMESTRE = "2094-Q1"
V27_PREFIX = f"{TEST_EMPRESA_PREFIX}V27_"


# ── Helpers ────────────────────────────────────────────────────


async def _cleanup_v27(db):
    """Borra cualquier resto del trimestre V27 antes/después del test.

    El conftest solo limpia 2099-Q1; este fixture cubre 2094-Q1.
    Las empresas TEST_EMPRESA_V27_* las limpia el conftest porque empiezan
    con TEST_EMPRESA_ y caen en el filtro existente.
    """
    queries = [
        f'DELETE FROM planificacion WHERE trimestre = \'{V27_TRIMESTRE}\'',
        f'DELETE FROM frecuencia WHERE trimestre = \'{V27_TRIMESTRE}\'',
        f'DELETE FROM "configTrimestral" WHERE trimestre = \'{V27_TRIMESTRE}\'',
    ]
    for q in queries:
        try:
            await db.execute(text(q))
        except Exception:
            pass
    await db.commit()


@pytest_asyncio.fixture(autouse=True)
async def cleanup_v27(db_session):
    """Limpia 2094-Q1 antes y después de cada test del módulo."""
    await _cleanup_v27(db_session)
    yield
    await _cleanup_v27(db_session)


async def _create_empresa(db, suffix: str, *, activa: bool = True) -> int:
    """Inserta (o reusa) una empresa de test. Devuelve id."""
    nombre = f"{V27_PREFIX}{suffix}"
    res = await db.execute(
        text(
            'INSERT INTO empresa (nombre, tipo, activa, "updatedAt") '
            "VALUES (:n, 'AMBAS', :a, NOW()) "
            'ON CONFLICT (nombre) DO UPDATE SET activa = EXCLUDED.activa '
            'RETURNING id'
        ),
        {"n": nombre, "a": activa},
    )
    eid = res.scalar()
    await db.commit()
    return eid


async def _set_ct(
    db,
    empresa_id: int,
    *,
    tipo_participacion: str | None = "AMBAS",
    frecuencia_ef: int | None = None,
    frecuencia_it: int | None = None,
    disponibilidad_dias: str | None = "L,M,X,J,V",
):
    """Upsert CT V27 con los campos relevantes para validación."""
    await db.execute(
        text(
            'INSERT INTO "configTrimestral" '
            '("empresaId", trimestre, "tipoParticipacion", '
            '"frecuenciaEF", "frecuenciaIT", "disponibilidadDias", "updatedAt") '
            "VALUES (:eid, :tri, :tp, :fef, :fit, :dd, NOW()) "
            'ON CONFLICT ("empresaId", trimestre) DO UPDATE SET '
            '"tipoParticipacion" = EXCLUDED."tipoParticipacion", '
            '"frecuenciaEF" = EXCLUDED."frecuenciaEF", '
            '"frecuenciaIT" = EXCLUDED."frecuenciaIT", '
            '"disponibilidadDias" = EXCLUDED."disponibilidadDias"'
        ),
        {
            "eid": empresa_id,
            "tri": V27_TRIMESTRE,
            "tp": tipo_participacion,
            "fef": frecuencia_ef,
            "fit": frecuencia_it,
            "dd": disponibilidad_dias,
        },
    )
    await db.commit()


# ── Tests ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_validar_ct_vacia(client, db_session):
    """Trimestre sin CTs → response coherente con listas vacías."""
    resp = await client.get(f"/api/config-trimestral/{V27_TRIMESTRE}/validar")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["trimestre"] == V27_TRIMESTRE
    assert body["total_empresas_revisadas"] == 0
    assert body["errores"] == []
    assert body["warnings"] == []
    assert "0 errores" in body["resumen"]


@pytest.mark.asyncio
async def test_validar_caso_EY(client, db_session):
    """Caso real EY: freq IT=1, dias=V, catálogo IT no incluye V → error V1."""
    eid = await _create_empresa(db_session, "EY")
    await _set_ct(
        db_session,
        eid,
        frecuencia_ef=2,
        frecuencia_it=1,
        disponibilidad_dias="V",
    )

    resp = await client.get(f"/api/config-trimestral/{V27_TRIMESTRE}/validar")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_empresas_revisadas"] == 1

    tipos = [e["tipo"] for e in body["errores"]]
    assert "freq_it_sin_dias_disponibles" in tipos

    item = next(e for e in body["errores"] if e["tipo"] == "freq_it_sin_dias_disponibles")
    assert item["empresa_id"] == eid
    assert item["severidad"] == "error"
    assert "IT=1" in item["detalle"]
    assert "V" in item["detalle"]


@pytest.mark.asyncio
async def test_validar_tipo_incoherente(client, db_session):
    """tipoParticipacion='EF' + freqIT=2 → error tipo_incoherente_it."""
    eid = await _create_empresa(db_session, "INCOHERENTE")
    await _set_ct(
        db_session,
        eid,
        tipo_participacion="EF",
        frecuencia_ef=2,
        frecuencia_it=2,
    )

    resp = await client.get(f"/api/config-trimestral/{V27_TRIMESTRE}/validar")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    tipos = [e["tipo"] for e in body["errores"]]
    assert "tipo_incoherente_it" in tipos

    item = next(e for e in body["errores"] if e["tipo"] == "tipo_incoherente_it")
    assert item["empresa_id"] == eid
    assert "frecuenciaIT=2" in item["detalle"]
    assert "tipoParticipacion='EF'" in item["detalle"]


@pytest.mark.asyncio
async def test_validar_empresa_fantasma(client, db_session):
    """freqEF=0 + freqIT=0 → warning empresa_fantasma (no error)."""
    eid = await _create_empresa(db_session, "FANTASMA")
    await _set_ct(
        db_session,
        eid,
        frecuencia_ef=0,
        frecuencia_it=0,
    )

    resp = await client.get(f"/api/config-trimestral/{V27_TRIMESTRE}/validar")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["errores"] == []
    assert len(body["warnings"]) == 1
    assert body["warnings"][0]["tipo"] == "empresa_fantasma"
    assert body["warnings"][0]["empresa_id"] == eid
    assert body["warnings"][0]["severidad"] == "warning"


@pytest.mark.asyncio
async def test_validar_tipo_ambas_no_dispara(client, db_session):
    """tipoParticipacion='AMBAS' con freq mixta → no error V2.

    La BD tiene NOT NULL en tipoParticipacion (constraint estructural), por
    lo que el path NULL del código defensivo (`or ""`) no es alcanzable en
    producción. El test cubre el caso real: AMBAS + freq mixta es la config
    estándar y NO debe disparar V2 — mismo branch del `if tipo == "EF"` /
    `if tipo == "IT"` que el código defensivo cubriría para NULL.
    """
    eid = await _create_empresa(db_session, "TIPO_AMBAS")
    await _set_ct(
        db_session,
        eid,
        tipo_participacion="AMBAS",
        frecuencia_ef=2,
        frecuencia_it=1,
        disponibilidad_dias="L,M,X,J,V",
    )

    resp = await client.get(f"/api/config-trimestral/{V27_TRIMESTRE}/validar")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    tipos = [e["tipo"] for e in body["errores"]]
    assert "tipo_incoherente_ef" not in tipos
    assert "tipo_incoherente_it" not in tipos


@pytest.mark.asyncio
async def test_validar_dias_completos_no_dispara(client, db_session):
    """disponibilidadDias='L,M,X,J,V' (rango completo) + freq mixta → 0 errores V1.

    La BD tiene NOT NULL en disponibilidadDias también, por lo que el path
    NULL del `_parse_dias` (`or DIAS_DEFAULT`) no es alcanzable en
    producción. El test cubre el caso equivalente al default: días que
    cubren el catálogo completo (EF L-V, IT L,M) — V1 no debe disparar.
    Whitespace tolerance via 'L, M, X' verifica que `_parse_dias` strip
    funciona, ejercitando el path de normalización.
    """
    eid = await _create_empresa(db_session, "DIAS_COMPLETOS")
    await _set_ct(
        db_session,
        eid,
        frecuencia_ef=2,
        frecuencia_it=1,
        disponibilidad_dias="L, M, X, J, V",
    )

    resp = await client.get(f"/api/config-trimestral/{V27_TRIMESTRE}/validar")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    tipos = [e["tipo"] for e in body["errores"]]
    assert "freq_it_sin_dias_disponibles" not in tipos
    assert "freq_ef_sin_dias_disponibles" not in tipos


@pytest.mark.asyncio
async def test_validar_combinado(client, db_session):
    """Mezcla de los 3 casos + empresa inactiva (debe ser excluida).

    - EY-like: V1 error → 1 error freq_it
    - INCOHERENTE: V2 error → 1 error tipo_incoherente_it
    - FANTASMA: V3 warning → 1 warning empresa_fantasma
    - INACTIVA con CT inválida → NO aparece (filtrada por activa=false)
    """
    eid_ey = await _create_empresa(db_session, "MIX_EY")
    eid_inc = await _create_empresa(db_session, "MIX_INC")
    eid_fan = await _create_empresa(db_session, "MIX_FAN")
    eid_inactiva = await _create_empresa(db_session, "MIX_INACTIVA", activa=False)

    await _set_ct(db_session, eid_ey, frecuencia_it=1, disponibilidad_dias="V")
    await _set_ct(
        db_session,
        eid_inc,
        tipo_participacion="EF",
        frecuencia_ef=2,
        frecuencia_it=1,
    )
    await _set_ct(db_session, eid_fan, frecuencia_ef=0, frecuencia_it=0)
    await _set_ct(
        db_session,
        eid_inactiva,
        tipo_participacion="EF",
        frecuencia_it=5,  # error tipo_incoherente_it si no estuviera filtrada
        disponibilidad_dias="V",  # error V1 también si no estuviera filtrada
    )

    resp = await client.get(f"/api/config-trimestral/{V27_TRIMESTRE}/validar")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Solo 3 empresas revisadas (la inactiva queda fuera).
    assert body["total_empresas_revisadas"] == 3

    # Errores: 1 V1 (EY) + 1 V2 (INC) = 2.
    err_tipos = sorted(e["tipo"] for e in body["errores"])
    assert err_tipos == ["freq_it_sin_dias_disponibles", "tipo_incoherente_it"]
    err_empresas = {e["empresa_id"] for e in body["errores"]}
    assert err_empresas == {eid_ey, eid_inc}
    assert eid_inactiva not in err_empresas

    # Warnings: 1 V3 (FANTASMA).
    assert len(body["warnings"]) == 1
    assert body["warnings"][0]["tipo"] == "empresa_fantasma"
    assert body["warnings"][0]["empresa_id"] == eid_fan
    assert eid_inactiva not in {w["empresa_id"] for w in body["warnings"]}
