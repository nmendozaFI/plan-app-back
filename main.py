"""
Fundación Integra — Planificador de Talleres
Backend FastAPI · main.py
"""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import frecuencias, calendario, empresas, historico, health, importar, restricciones, talleres
# ── App ──────────────────────────────────────────────────────

app = FastAPI(
    title="Planificador Fundación Integra",
    version="1.0.0",
    description="Motor de planificación trimestral de talleres EF/IT",
)

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["POST", "GET", "PUT", "DELETE"],
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