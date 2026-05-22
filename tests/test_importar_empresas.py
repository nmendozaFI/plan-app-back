"""V24 Cambio B (T9, decisión D8): tests para que el master importer
(`POST /api/importar/empresas`) escriba `permiteExtras` en CT desde el Excel
maestro, simétrico al manejo de `escuelaPropia`.

Cobertura:
  - Columna "Permite Extras" con valor SI → CT.permiteExtras = true.
  - Columna "Permite Extras" con valor NO → CT.permiteExtras = false.
  - Columna ausente en el Excel → no sobreescribir CT existente en UPDATE;
    default false en INSERT (mismo patrón que escuelaPropia).
"""

import pytest
import openpyxl
from io import BytesIO
from sqlalchemy import text

from .conftest import TEST_TRIMESTRE, TEST_EMPRESA_PREFIX


def _build_master_excel(empresas: list[dict]) -> bytes:
    """Build a minimal master Excel with "Empresas" sheet.

    `empresas` is a list of dicts with keys among:
      nombre (required), tipo, "Permite Extras", "Escuela Propia",
      activa, esNueva, esComodin, frecuencia, etc.
    Only the columns present in the first dict get headers (so the test can
    control "column absent" semantics).
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Empresas"

    if not empresas:
        wb.save(BytesIO())
        raise ValueError("test bug: empresas list vacía")

    headers = list(empresas[0].keys())
    ws.append(headers)
    for emp in empresas:
        ws.append([emp.get(h) for h in headers])

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


@pytest.mark.asyncio
async def test_master_writes_permite_extras_to_ct(client, db_session):
    """SI → permiteExtras=true en CT post-import."""
    nombre = f"{TEST_EMPRESA_PREFIX}V24_MASTER_PE_SI"
    excel_bytes = _build_master_excel([
        {"nombre": nombre, "tipo": "AMBAS", "Permite Extras": "SI"},
    ])

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_test.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text

    # DB cross-check: empresa creada + CT con permiteExtras=true.
    row = await db_session.execute(
        text(
            'SELECT ct."permiteExtras" '
            'FROM "configTrimestral" ct '
            'JOIN empresa e ON e.id = ct."empresaId" '
            'WHERE e.nombre = :n AND ct.trimestre = :tri'
        ),
        {"n": nombre, "tri": TEST_TRIMESTRE},
    )
    rec = row.mappings().first()
    assert rec is not None, "CT debería existir tras el import"
    assert rec["permiteExtras"] is True


@pytest.mark.asyncio
async def test_master_permite_extras_acepta_si_no(client, db_session):
    """Mismo importer con NO → permiteExtras=false."""
    nombre = f"{TEST_EMPRESA_PREFIX}V24_MASTER_PE_NO"
    excel_bytes = _build_master_excel([
        {"nombre": nombre, "tipo": "EF", "Permite Extras": "NO"},
    ])

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_test.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text

    row = await db_session.execute(
        text(
            'SELECT ct."permiteExtras" '
            'FROM "configTrimestral" ct '
            'JOIN empresa e ON e.id = ct."empresaId" '
            'WHERE e.nombre = :n AND ct.trimestre = :tri'
        ),
        {"n": nombre, "tri": TEST_TRIMESTRE},
    )
    rec = row.mappings().first()
    assert rec is not None
    assert rec["permiteExtras"] is False


@pytest.mark.asyncio
async def test_master_permite_extras_columna_ausente_default_false(client, db_session):
    """Sin columna PE en el Excel → INSERT default false (no error)."""
    nombre = f"{TEST_EMPRESA_PREFIX}V24_MASTER_PE_ABSENT"
    excel_bytes = _build_master_excel([
        {"nombre": nombre, "tipo": "IT"},
    ])

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_test.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text

    row = await db_session.execute(
        text(
            'SELECT ct."permiteExtras" '
            'FROM "configTrimestral" ct '
            'JOIN empresa e ON e.id = ct."empresaId" '
            'WHERE e.nombre = :n AND ct.trimestre = :tri'
        ),
        {"n": nombre, "tri": TEST_TRIMESTRE},
    )
    rec = row.mappings().first()
    assert rec is not None
    assert rec["permiteExtras"] is False


# ── V25 Cambio C: 3 flags estructurales en empresa via importer maestro ──


async def _fetch_empresa_flags(db_session, nombre: str) -> dict | None:
    """DB cross-check helper para los 3 flags V25 a nivel empresa."""
    row = await db_session.execute(
        text(
            'SELECT "esContratante", "puedeSerEP", "puedeSerDoble" '
            "FROM empresa WHERE nombre = :n"
        ),
        {"n": nombre},
    )
    return row.mappings().first()


@pytest.mark.asyncio
async def test_master_importer_v25_flags_columnas_presentes(client, db_session):
    """Excel con esContratante/puedeSerEP/puedeSerDoble → empresa con flags correctos."""
    nombre = f"{TEST_EMPRESA_PREFIX}V25_FLAGS_OK"
    excel_bytes = _build_master_excel(
        [
            {
                "nombre": nombre,
                "tipo": "AMBAS",
                "esContratante": "SI",
                "puedeSerEP": "SI",
                "puedeSerDoble": "NO",
            }
        ]
    )

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_test.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text

    rec = await _fetch_empresa_flags(db_session, nombre)
    assert rec is not None, "Empresa no creada"
    assert rec["esContratante"] is True
    assert rec["puedeSerEP"] is True
    assert rec["puedeSerDoble"] is False


@pytest.mark.asyncio
async def test_master_importer_v25_columnas_ausentes_no_toca_bd(
    client, db_session
):
    """Excel sin las 3 columnas V25 (formato V24 viejo) NO debe sobrescribir
    los flags V25 en BD. Esto preserva el backfill de puedeSerEP que aplicó
    la migration 0006 si la planificadora corre el maestro V24 por inercia."""
    nombre = f"{TEST_EMPRESA_PREFIX}V25_FLAGS_ABSENT"

    # Setup: empresa preexistente con flags V25 ya seteados en BD (simula
    # estado post-Capa-1 + algún UPDATE manual previo).
    await db_session.execute(
        text(
            "INSERT INTO empresa (nombre, tipo, semaforo, activa, \"esNueva\", "
            '"esContratante", "puedeSerEP", "puedeSerDoble", "updatedAt") '
            "VALUES (:n, 'EF', 'AMBAR', true, false, true, true, true, NOW())"
        ),
        {"n": nombre},
    )
    await db_session.commit()

    # Excel formato V24: solo nombre + tipo, ninguna columna V25.
    excel_bytes = _build_master_excel([{"nombre": nombre, "tipo": "EF"}])

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_test.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text

    # Los 3 flags deben seguir en true: el importer NO los tocó porque la
    # columna no estaba en el Excel.
    rec = await _fetch_empresa_flags(db_session, nombre)
    assert rec is not None
    assert rec["esContratante"] is True, "esContratante destruido por import V24"
    assert rec["puedeSerEP"] is True, "puedeSerEP destruido (backfill perdido)"
    assert rec["puedeSerDoble"] is True, "puedeSerDoble destruido por import V24"

    # No warning ruidoso sobre los flags V25.
    warnings = resp.json().get("warnings", [])
    for w in warnings:
        wl = w.lower()
        assert "escontratante" not in wl
        assert "puedeserep" not in wl
        assert "puedeserdoble" not in wl


@pytest.mark.asyncio
async def test_master_importer_v25_columna_presente_celda_vacia_setea_false(
    client, db_session
):
    """Excel con la columna presente pero celda vacía → False explícito.
    La planificadora vació la celda a propósito; el importer debe respetar
    esa intención (distinta de "columna ausente")."""
    nombre = f"{TEST_EMPRESA_PREFIX}V25_CELDA_VACIA"

    # Setup: empresa con esContratante=true en BD.
    await db_session.execute(
        text(
            "INSERT INTO empresa (nombre, tipo, semaforo, activa, \"esNueva\", "
            '"esContratante", "puedeSerEP", "puedeSerDoble", "updatedAt") '
            "VALUES (:n, 'EF', 'AMBAR', true, false, true, false, false, NOW())"
        ),
        {"n": nombre},
    )
    await db_session.commit()

    # Excel con columna esContratante presente pero celda vacía (None).
    # _build_master_excel toma `emp.get(h)` → None → openpyxl escribe celda vacía.
    excel_bytes = _build_master_excel(
        [
            {
                "nombre": nombre,
                "tipo": "EF",
                "esContratante": None,
            }
        ]
    )

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_test.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text

    rec = await _fetch_empresa_flags(db_session, nombre)
    assert rec is not None
    assert rec["esContratante"] is False, (
        "Columna presente con celda vacía debe setear False (intención explícita)"
    )


@pytest.mark.asyncio
async def test_master_importer_v25_columna_ausente_insert_nueva_empresa_default_false(
    client, db_session
):
    """Empresa NUEVA (INSERT) sin las 3 columnas V25 → los flags quedan en
    false (default de la columna). No hay valor previo en BD que preservar."""
    nombre = f"{TEST_EMPRESA_PREFIX}V25_INSERT_DEFAULT"

    # Excel formato V24, empresa nueva (no existe en BD).
    excel_bytes = _build_master_excel([{"nombre": nombre, "tipo": "AMBAS"}])

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_test.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text

    rec = await _fetch_empresa_flags(db_session, nombre)
    assert rec is not None, "Empresa no creada"
    assert rec["esContratante"] is False
    assert rec["puedeSerEP"] is False
    assert rec["puedeSerDoble"] is False


@pytest.mark.asyncio
async def test_master_importer_v25_excel_overrides_backfill(client, db_session):
    """Excel con puedeSerEP=false explícito sobre empresa con backfill true → false.
    Diferencia con el test "columna ausente": aquí la columna SÍ está, con
    valor explícito NO, así que el Excel manda."""
    nombre = f"{TEST_EMPRESA_PREFIX}V25_BACKFILL_OVERRIDE"

    # Setup: empresa pre-existente con puedeSerEP=true (simula backfill aplicado).
    await db_session.execute(
        text(
            "INSERT INTO empresa (nombre, tipo, semaforo, activa, \"esNueva\", "
            '"puedeSerEP", "esContratante", "puedeSerDoble", "updatedAt") '
            "VALUES (:n, 'EF', 'AMBAR', true, false, true, false, false, NOW())"
        ),
        {"n": nombre},
    )
    await db_session.commit()

    # Pre-condición.
    pre = await _fetch_empresa_flags(db_session, nombre)
    assert pre is not None
    assert pre["puedeSerEP"] is True, "setup falló: debería empezar con backfill true"

    # Importer corre con NO explícito en puedeSerEP → debe sobreescribir.
    excel_bytes = _build_master_excel(
        [
            {
                "nombre": nombre,
                "tipo": "EF",
                "puedeSerEP": "NO",
            }
        ]
    )
    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_test.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text

    # Excel manda: el flag queda en false aunque la BD lo tenía en true.
    post = await _fetch_empresa_flags(db_session, nombre)
    assert post is not None
    assert post["puedeSerEP"] is False, "Excel debería haber sobreescrito el backfill"


# ─────────────────────────────────────────────────────────────
# V25 Capa 9 — Guard explícito activa/inactiva en master importer
# ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_importer_rechaza_update_empresa_inactiva(client, db_session):
    """V25 Capa 9 (decisión C2): si la empresa ya existe en BD con
    activa=false, el master importer NO la actualiza. Genera warning específico
    y `rechazadas_inactivas` ≥ 1. Las demás filas del Excel se procesan.
    """
    inactiva = f"{TEST_EMPRESA_PREFIX}V25_IMP_INACTIVA_EXIST"
    activa = f"{TEST_EMPRESA_PREFIX}V25_IMP_ACTIVA"

    # Pre-sembrar la inactiva en BD.
    await db_session.execute(
        text(
            'INSERT INTO empresa (nombre, tipo, activa, "updatedAt") '
            "VALUES (:n, 'AMBAS', false, NOW()) "
            'ON CONFLICT (nombre) DO UPDATE SET activa = false'
        ),
        {"n": inactiva},
    )
    await db_session.commit()

    excel_bytes = _build_master_excel([
        # Fila 1: empresa inactiva con intento de cambiar tipo → rechazada.
        {"nombre": inactiva, "tipo": "EF", "activa": "SI"},
        # Fila 2: empresa activa nueva → procesada normalmente.
        {"nombre": activa, "tipo": "AMBAS", "activa": "SI"},
    ])

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_capa9.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["rechazadas_inactivas"] >= 1, (
        f"esperaba rechazadas_inactivas >= 1, hay {data['rechazadas_inactivas']}"
    )

    warnings_inactiva = [
        w for w in data["warnings"] if inactiva in w and "inactiva" in w.lower()
    ]
    assert warnings_inactiva, (
        f"warning específico para '{inactiva}' no encontrado: {data['warnings']}"
    )

    # La inactiva sigue inactiva y con tipo original (NO actualizada).
    row = await db_session.execute(
        text('SELECT tipo, activa FROM empresa WHERE nombre = :n'),
        {"n": inactiva},
    )
    rec = row.mappings().first()
    assert rec["activa"] is False, "la inactiva debería seguir inactiva"
    assert rec["tipo"] == "AMBAS", (
        f"tipo no debería haberse modificado: {rec['tipo']}"
    )

    # La activa sí fue creada.
    row_a = await db_session.execute(
        text('SELECT id, activa FROM empresa WHERE nombre = :n'),
        {"n": activa},
    )
    rec_a = row_a.mappings().first()
    assert rec_a is not None and rec_a["activa"] is True


@pytest.mark.asyncio
async def test_importer_permite_crear_empresa_con_activa_false(
    client, db_session,
):
    """V25 Capa 9: empresa NUEVA (no existe en BD) con activa=NO en Excel
    SÍ se crea con activa=false. El guard solo bloquea UPDATEs de inactivas
    preexistentes, no la creación legítima con flag inactiva.
    """
    nombre = f"{TEST_EMPRESA_PREFIX}V25_IMP_NEW_INACTIVA"

    # Asegurar que no existe.
    await db_session.execute(
        text("DELETE FROM empresa WHERE nombre = :n"), {"n": nombre}
    )
    await db_session.commit()

    excel_bytes = _build_master_excel([
        {"nombre": nombre, "tipo": "IT", "activa": "NO"},
    ])

    resp = await client.post(
        "/api/importar/empresas",
        files={
            "file": (
                "maestro_capa9_new.xlsx",
                excel_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        },
        data={"trimestre": TEST_TRIMESTRE},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["creadas"] >= 1, (
        f"empresa nueva con activa=false debería contar como creada: {data}"
    )
    assert data["rechazadas_inactivas"] == 0, (
        f"creación nueva no debería contarse como rechazada: {data}"
    )

    # BD: empresa creada con activa=false.
    row = await db_session.execute(
        text("SELECT activa, tipo FROM empresa WHERE nombre = :n"),
        {"n": nombre},
    )
    rec = row.mappings().first()
    assert rec is not None, "la empresa debería existir tras el import"
    assert rec["activa"] is False
    assert rec["tipo"] == "IT"
