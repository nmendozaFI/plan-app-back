"""
REPORT-ONLY audit of the `restriccion` table.

Generates a markdown report in `reports/restricciones_audit_{YYYY-MM-DD}.md`
covering:
  A. Current state of all restrictions joined to empresa + taller.
  B. Orphan rows missing `descripcion`.
  C. Detection of restrictions that the planner doc requires but are absent
     in the DB (e.g. Santander FxM `solo_taller`, Telefónica `solo_dia=X`).
  D. Suggested SQL fixes — emitted as commented-out INSERT statements that
     the planner can review and execute manually.

This script DOES NOT mutate any data. Run with:
    python scripts/audit_restricciones.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import date
from pathlib import Path

# Ensure the project root is on sys.path when invoked as `python scripts/...`
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from app.db import AsyncSessionLocal  # noqa: E402


REPORTS_DIR = ROOT / "reports"


async def _load_restrictions(db) -> list[dict]:
    res = await db.execute(
        text(
            """
            SELECT r.id,
                   r."empresaId" AS empresa_id,
                   e.nombre      AS empresa_nombre,
                   r.tipo,
                   r.clave,
                   r.valor,
                   r."tallerId"  AS taller_id,
                   t.nombre      AS taller_nombre,
                   r.descripcion
              FROM restriccion r
              JOIN empresa e ON e.id = r."empresaId"
              LEFT JOIN taller t ON t.id = r."tallerId"
             ORDER BY e.nombre, r.tipo, r.clave, r.id
            """
        )
    )
    return [dict(row) for row in res.mappings().all()]


async def _load_active_empresas(db) -> list[dict]:
    res = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE activa = true ORDER BY nombre")
    )
    return [dict(row) for row in res.mappings().all()]


def _match_companies(empresas: list[dict], needles: list[str]) -> list[dict]:
    """Return empresas whose name (uppercased) contains any of the needles."""
    matches = []
    for emp in empresas:
        upper = (emp["nombre"] or "").upper()
        if any(n in upper for n in needles):
            matches.append(emp)
    return matches


def _section_a(restricciones: list[dict]) -> list[str]:
    lines = ["## A. Current state", ""]
    if not restricciones:
        lines.append("_No hay restricciones en la BD._")
        return lines

    lines.append(
        "| id | empresa | tipo | clave | valor | tallerId | taller (FK) | descripcion |"
    )
    lines.append(
        "|---:|---------|------|-------|-------|---------:|-------------|-------------|"
    )
    for r in restricciones:
        desc = (r.get("descripcion") or "").replace("|", "\\|")
        valor = (r.get("valor") or "").replace("|", "\\|")
        empresa = (r.get("empresa_nombre") or "").replace("|", "\\|")
        taller = (r.get("taller_nombre") or "").replace("|", "\\|")
        taller_id = r.get("taller_id")
        lines.append(
            f"| {r['id']} | {empresa} | {r['tipo']} | {r['clave']} | {valor} | "
            f"{taller_id if taller_id is not None else '—'} | {taller or '—'} | {desc or '—'} |"
        )
    return lines


def _section_b(restricciones: list[dict]) -> list[str]:
    lines = ["", "## B. Orphan rows (missing description) — needs review by planner", ""]
    orphans = [
        r for r in restricciones if not (r.get("descripcion") or "").strip()
    ]
    if not orphans:
        lines.append("_Todas las restricciones tienen descripción. ✅_")
        return lines

    lines.append("| id | empresa | tipo | clave | valor |")
    lines.append("|---:|---------|------|-------|-------|")
    for r in orphans:
        valor = (r.get("valor") or "").replace("|", "\\|")
        empresa = (r.get("empresa_nombre") or "").replace("|", "\\|")
        lines.append(
            f"| {r['id']} | {empresa} | {r['tipo']} | {r['clave']} | {valor} |"
        )
    lines.append("")
    lines.append(
        "_Acción sugerida: el planificador revisa cada fila y añade descripción "
        "explicativa (origen: documento oficial, decisión, etc.)._"
    )
    return lines


def _section_c(
    restricciones: list[dict], empresas: list[dict]
) -> tuple[list[str], dict]:
    lines = [
        "",
        "## C. Missing restrictions per planner doc",
        "",
    ]
    findings = {"santander_fxm_missing": False, "telefonica_missing": False}

    # ── Santander FxM → solo_taller='Gestión de ingresos' ─────
    fxm_companies = _match_companies(empresas, ["SANTANDER FXM", "FXM"])
    fxm_has_solo_taller = any(
        r["clave"] == "solo_taller"
        and any(c["id"] == r["empresa_id"] for c in fxm_companies)
        for r in restricciones
    )
    if fxm_companies and not fxm_has_solo_taller:
        findings["santander_fxm_missing"] = True
        names = ", ".join(c["nombre"] for c in fxm_companies)
        lines.append(
            f"- ❌ **Santander FxM → solo_taller='Gestión de ingresos' MISSING.** "
            f"Empresas detectadas: {names}."
        )
    elif not fxm_companies:
        lines.append(
            "- ⚠ No se encontró ninguna empresa con 'SANTANDER FXM' o 'FXM' en el nombre. "
            "Verificar nomenclatura."
        )
    else:
        lines.append(
            "- ✅ Santander FxM tiene al menos una restricción `solo_taller`. Revisar valor."
        )

    # ── Telefónica → solo_dia='X' ─────────────────────────────
    telef_companies = _match_companies(empresas, ["TELEFONICA", "TELEFÓNICA"])
    telef_has_solo_dia = any(
        r["clave"] == "solo_dia"
        and any(c["id"] == r["empresa_id"] for c in telef_companies)
        for r in restricciones
    )
    if telef_companies and not telef_has_solo_dia:
        findings["telefonica_missing"] = True
        names = ", ".join(c["nombre"] for c in telef_companies)
        lines.append(
            f"- ❌ **Telefónica → solo_dia='X' MISSING (presente en Excel maestro pero no en BD).** "
            f"Empresas detectadas: {names}."
        )
    elif not telef_companies:
        lines.append(
            "- ⚠ No se encontró ninguna empresa con 'TELEFONICA' en el nombre. "
            "Verificar nomenclatura."
        )
    else:
        lines.append("- ✅ Telefónica tiene al menos una restricción `solo_dia`.")

    return lines, findings


def _section_d(findings: dict) -> list[str]:
    lines = [
        "",
        "## D. Suggested SQL fixes (commented out — review before running)",
        "",
        "```sql",
    ]

    if findings["santander_fxm_missing"]:
        lines.extend(
            [
                "-- Suggested fix for Santander FxM",
                "-- (requires manual confirmation of exact empresa name and taller id):",
                "-- INSERT INTO restriccion (\"empresaId\", tipo, clave, valor, \"tallerId\", descripcion)",
                "-- SELECT e.id, 'HARD', 'solo_taller', 'Gestión de ingresos',",
                "--        (SELECT id FROM taller WHERE nombre ILIKE '%gestión de ingresos%' LIMIT 1),",
                "--        'Santander FxM → solo Gestión de ingresos (doc planificador)'",
                "-- FROM empresa e WHERE UPPER(e.nombre) LIKE '%FXM%';",
                "",
            ]
        )

    if findings["telefonica_missing"]:
        lines.extend(
            [
                "-- Suggested fix for Telefónica:",
                "-- INSERT INTO restriccion (\"empresaId\", tipo, clave, valor, descripcion)",
                "-- SELECT id, 'HARD', 'solo_dia', 'X',",
                "--        'Telefónica solo miércoles (del Excel maestro)'",
                "-- FROM empresa WHERE UPPER(nombre) LIKE 'TELEFONICA%';",
                "",
            ]
        )

    if not findings["santander_fxm_missing"] and not findings["telefonica_missing"]:
        lines.append("-- No SQL fixes needed: both expected restrictions are present.")

    lines.append("```")
    return lines


async def _run() -> str:
    REPORTS_DIR.mkdir(exist_ok=True)
    today = date.today().isoformat()
    out_path = REPORTS_DIR / f"restricciones_audit_{today}.md"

    async with AsyncSessionLocal() as db:
        restricciones = await _load_restrictions(db)
        empresas = await _load_active_empresas(db)

    header = [
        f"# Auditoría de restricciones — {today}",
        "",
        f"Total restricciones en BD: **{len(restricciones)}**  ",
        f"Empresas activas: **{len(empresas)}**",
        "",
        "Este reporte es **solo lectura**. No se modifican datos. Las correcciones "
        "sugeridas se emiten como SQL comentado al final para revisión manual.",
        "",
    ]

    section_a = _section_a(restricciones)
    section_b = _section_b(restricciones)
    section_c, findings = _section_c(restricciones, empresas)
    section_d = _section_d(findings)

    body = "\n".join(header + section_a + section_b + section_c + section_d) + "\n"

    out_path.write_text(body, encoding="utf-8")
    return body


def main():
    output = asyncio.run(_run())
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        print(output)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(output.encode("utf-8", errors="replace"))
        sys.stdout.buffer.write(b"\n")


if __name__ == "__main__":
    main()
