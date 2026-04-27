"""
Fundación Integra — Planificador de Talleres
Backend FastAPI · main.py
"""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import frecuencias, calendario, empresas, historico, health, importar, restricciones, talleres, config_trimestral, settings, calendario_anual, planificacion
# ── App ──────────────────────────────────────────────────────

app = FastAPI(
    title="Planificador Fundación Integra",
    version="1.0.0",
    description="Motor de planificación trimestral de talleres EF/IT",
)

# CORS configuration
# With Next.js proxy, browser requests come from the same origin (no CORS).
# Direct API calls (testing, SSR) still need CORS for these origins.
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "https://plani-app-rose.vercel.app,http://localhost:3000,http://127.0.0.1:3000"
).split(",")

# Regex to match Vercel preview URLs: https://plani-app-*.vercel.app
ALLOWED_ORIGINS_REGEX = os.getenv(
    "ALLOWED_ORIGINS_REGEX",
    r"https://plani-app.*\.vercel\.app"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=ALLOWED_ORIGINS_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ──────────────────────────────────────────────────

app.include_router(health.router,       tags=["Health"])
app.include_router(empresas.router,     prefix="/api/empresas",     tags=["Empresas"])
app.include_router(historico.router,    prefix="/api/historico",    tags=["Histórico"])
app.include_router(frecuencias.router,  prefix="/api/frecuencias",  tags=["Fase 1 — Frecuencias"])
app.include_router(calendario.router,   prefix="/api/calendario",   tags=["Fase 2 — Calendario"])
app.include_router(importar.router, prefix="/api/importar", tags=["importar"])
app.include_router(restricciones.router, prefix="/api/restricciones", tags=["restricciones"])
app.include_router(talleres.router,      prefix="/api/talleres",      tags=["Talleres"])
app.include_router(calendario_anual.router, prefix="/api/talleres/calendario-anual", tags=["Calendario Anual"])
app.include_router(config_trimestral.router, prefix="/api/config-trimestral", tags=["Config Trimestral"])
app.include_router(settings.router, prefix="/api/settings", tags=["Settings"])
app.include_router(planificacion.router, prefix="/api/planificacion", tags=["Planificación"])