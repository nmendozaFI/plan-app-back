"""Smoke test post-Cambio B Fase B1+B2.

Tres comprobaciones rápidas contra la BD real (Neon) vía ASGITransport:
  1. GET /api/config-trimestral/2026-Q3 → JSON expone frecuencia_ef y frecuencia_it.
  2. POST /api/frecuencias/calcular con trimestre=2026-Q3 → empresas list (Q3 hoy
     tiene 85 CTs con freqEF/IT NULL → debería devolver 0 empresas, decisión D2).
  3. PUT temporal sobre una empresa Q3 con frecuencia_ef/_it, releer, restaurar.
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from httpx import AsyncClient, ASGITransport
from sqlalchemy import text

from main import app
from app.db import AsyncSessionLocal


async def main() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1) GET /api/config-trimestral/2026-Q3 — schema check.
        resp = await client.get("/api/config-trimestral/2026-Q3")
        print(f"[1] GET /api/config-trimestral/2026-Q3 → {resp.status_code}")
        data = resp.json()
        configs = data["configs"]
        sample = configs[0] if configs else {}
        has_ef = "frecuencia_ef" in sample
        has_it = "frecuencia_it" in sample
        print(f"    total CTs: {len(configs)}")
        print(f"    keys: frecuencia_ef={has_ef}, frecuencia_it={has_it}")
        print(f"    primera fila: {sample.get('empresa_nombre')}: "
              f"freq_ef={sample.get('frecuencia_ef')}, "
              f"freq_it={sample.get('frecuencia_it')}, "
              f"freq_total={sample.get('frecuencia_solicitada')}")

        # 2) POST /api/frecuencias/calcular → debería ser empty list (Q3 sin freq EF/IT).
        resp = await client.post(
            "/api/frecuencias/calcular",
            json={"trimestre": "2026-Q3"},
        )
        print(f"\n[2] POST /api/frecuencias/calcular {'2026-Q3'} → {resp.status_code}")
        if resp.status_code == 200:
            d = resp.json()
            empresas = d.get("empresas", [])
            print(f"    empresas devueltas: {len(empresas)} (esperado 0 — D2 omitidas)")
            print(f"    total_ef={d.get('total_ef')}, total_it={d.get('total_it')}")
            print(f"    warnings (primeros 2): {d.get('warnings', [])[:2]}")

        # 3) PUT temporal + GET + RESTORE.
        async with AsyncSessionLocal() as db:
            # Pick first empresa with CT in Q3.
            row = await db.execute(
                text(
                    'SELECT ct."empresaId", e.nombre, '
                    'ct."frecuenciaEF" AS orig_ef, ct."frecuenciaIT" AS orig_it '
                    'FROM "configTrimestral" ct '
                    'JOIN empresa e ON e.id = ct."empresaId" '
                    'WHERE ct.trimestre = \'2026-Q3\' '
                    'ORDER BY e.nombre LIMIT 1'
                )
            )
            rec = row.mappings().first()

        if rec is None:
            print("\n[3] sin CTs en Q3 — skip")
            return

        eid = rec["empresaId"]
        nombre = rec["nombre"]
        orig_ef = rec["orig_ef"]
        orig_it = rec["orig_it"]
        print(f"\n[3] PUT temporal sobre empresa {eid} ({nombre}):")
        print(f"    valores originales: freq_ef={orig_ef}, freq_it={orig_it}")

        resp = await client.put(
            f"/api/config-trimestral/2026-Q3/{eid}",
            json={"frecuencia_ef": 99, "frecuencia_it": 88},
        )
        print(f"    PUT → {resp.status_code}")
        if resp.status_code == 200:
            cfg = resp.json()["config"]
            print(f"    config devuelto: freq_ef={cfg['frecuencia_ef']}, freq_it={cfg['frecuencia_it']}")

        # Releer via GET para confirmar persist.
        resp = await client.get("/api/config-trimestral/2026-Q3")
        target = next(c for c in resp.json()["configs"] if c["empresa_id"] == eid)
        print(f"    GET releer: freq_ef={target['frecuencia_ef']}, freq_it={target['frecuencia_it']}")

        # RESTORE valores originales.
        async with AsyncSessionLocal() as db:
            await db.execute(
                text(
                    'UPDATE "configTrimestral" '
                    'SET "frecuenciaEF" = :ef, "frecuenciaIT" = :it, "updatedAt" = NOW() '
                    'WHERE "empresaId" = :eid AND trimestre = \'2026-Q3\''
                ),
                {"eid": eid, "ef": orig_ef, "it": orig_it},
            )
            await db.commit()
        print(f"    restaurado a freq_ef={orig_ef}, freq_it={orig_it}")


if __name__ == "__main__":
    asyncio.run(main())
