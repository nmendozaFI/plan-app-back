"""
V16 — quick smoke tests for the franja restriction validator + uniqueness.

Runs the endpoint handlers directly (no HTTP, no TestClient) to avoid the
async-engine / event-loop issue with httpx TestClient. Picks the smallest
existing empresa id, exercises every rule, then cleans up.

Usage:
    python scripts/test_franja_v16.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi import HTTPException  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.db import AsyncSessionLocal  # noqa: E402
from app.routers.restricciones import (  # noqa: E402
    RestriccionIn,
    crear_restriccion,
    editar_restriccion,
    borrar_restriccion,
)


async def _pick_empresa_id(db) -> int:
    res = await db.execute(text("SELECT id FROM empresa ORDER BY id LIMIT 1"))
    val = res.scalar()
    if not val:
        raise SystemExit("No hay empresas en la BD")
    return int(val)


async def _cleanup(db, empresa_id: int) -> None:
    await db.execute(
        text(
            """
            DELETE FROM restriccion
             WHERE "empresaId" = :eid
               AND clave IN ('franja_horaria', 'franja_por_dia')
            """
        ),
        {"eid": empresa_id},
    )
    await db.commit()


async def _expect_201(label: str, coro):
    try:
        out = await coro
        ok = isinstance(out, dict) and out.get("id") is not None
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
        return ok, out
    except HTTPException as e:
        print(f"  [FAIL] {label} — HTTPException {e.status_code}: {e.detail}")
        return False, None


async def _expect_status(label: str, coro, expected: int):
    try:
        await coro
        print(f"  [FAIL] {label} — expected {expected}, got 201")
        return False
    except HTTPException as e:
        ok = e.status_code == expected
        print(
            f"  [{'PASS' if ok else 'FAIL'}] {label} — got {e.status_code}"
            + (f" ({e.detail})" if not ok else "")
        )
        return ok


async def run() -> int:
    results: list[bool] = []
    created_ids: list[int] = []

    async with AsyncSessionLocal() as db:
        empresa_id = await _pick_empresa_id(db)
        await _cleanup(db, empresa_id)
        print(f"Using empresa_id={empresa_id}\n")

        try:
            # 1. Accept canonical franja_horaria SOFT
            ok, out = await _expect_201(
                "1. POST franja_horaria 09:30-11:30 SOFT",
                crear_restriccion(
                    empresa_id,
                    RestriccionIn(tipo="SOFT", clave="franja_horaria", valor="09:30-11:30"),
                    db,
                ),
            )
            results.append(ok)
            if ok:
                created_ids.append(out["id"])

            # 2. Reject non-canonical franja_horaria → 400
            results.append(
                await _expect_status(
                    "2. POST franja_horaria 10:15-11:45 (non-canonical)",
                    crear_restriccion(
                        empresa_id,
                        RestriccionIn(tipo="SOFT", clave="franja_horaria", valor="10:15-11:45"),
                        db,
                    ),
                    400,
                )
            )

            # 3. Reject second franja_horaria → 409
            results.append(
                await _expect_status(
                    "3. POST second franja_horaria (same empresa)",
                    crear_restriccion(
                        empresa_id,
                        RestriccionIn(tipo="SOFT", clave="franja_horaria", valor="12:00-14:00"),
                        db,
                    ),
                    409,
                )
            )

            # 4. Accept franja_por_dia HARD
            ok, out = await _expect_201(
                "4. POST franja_por_dia L:12:00-14:00 HARD",
                crear_restriccion(
                    empresa_id,
                    RestriccionIn(tipo="HARD", clave="franja_por_dia", valor="L:12:00-14:00"),
                    db,
                ),
            )
            results.append(ok)
            if ok:
                created_ids.append(out["id"])

            # 5. Reject invalid day → 400
            results.append(
                await _expect_status(
                    "5. POST franja_por_dia Z:09:30-11:30 (invalid day)",
                    crear_restriccion(
                        empresa_id,
                        RestriccionIn(tipo="HARD", clave="franja_por_dia", valor="Z:09:30-11:30"),
                        db,
                    ),
                    400,
                )
            )

            # 6. Reject invalid franja → 400
            results.append(
                await _expect_status(
                    "6. POST franja_por_dia L:10:15-11:45 (invalid franja)",
                    crear_restriccion(
                        empresa_id,
                        RestriccionIn(tipo="HARD", clave="franja_por_dia", valor="L:10:15-11:45"),
                        db,
                    ),
                    400,
                )
            )

            # 7a. Reject duplicate franja_por_dia same day → 409
            results.append(
                await _expect_status(
                    "7a. POST franja_por_dia second L (same day)",
                    crear_restriccion(
                        empresa_id,
                        RestriccionIn(tipo="HARD", clave="franja_por_dia", valor="L:09:30-11:30"),
                        db,
                    ),
                    409,
                )
            )

            # 7b. Accept franja_por_dia different day M
            ok, out = await _expect_201(
                "7b. POST franja_por_dia M:09:30-11:30 (different day)",
                crear_restriccion(
                    empresa_id,
                    RestriccionIn(tipo="SOFT", clave="franja_por_dia", valor="M:09:30-11:30"),
                    db,
                ),
            )
            results.append(ok)
            if ok:
                created_ids.append(out["id"])

            # 8. PUT update existing franja_horaria (excludes self in uniqueness)
            if created_ids:
                try:
                    await editar_restriccion(
                        created_ids[0],
                        RestriccionIn(tipo="SOFT", clave="franja_horaria", valor="15:00-17:00"),
                        db,
                    )
                    print("  [PASS] 8. PUT update franja_horaria -> 15:00-17:00")
                    results.append(True)
                except HTTPException as e:
                    print(f"  [FAIL] 8. PUT update — {e.status_code}: {e.detail}")
                    results.append(False)

        finally:
            for rid in created_ids:
                try:
                    await borrar_restriccion(rid, db)
                except HTTPException:
                    pass
            await _cleanup(db, empresa_id)

    passed = sum(results)
    total = len(results)
    print(f"\nResult: {passed}/{total} passed")
    return 0 if passed == total else 1


def main():
    code = asyncio.run(run())
    sys.exit(code)


if __name__ == "__main__":
    main()
