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
