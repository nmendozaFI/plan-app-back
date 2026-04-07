"""
Backend FastAPI — expone el pipeline de transcripción como API REST
Archivo: back/main.py  (el Procfile y Render apuntan a este)
"""

import os
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app = FastAPI(title="Transcriptor API", version="1.0.0")

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


class ResultadoTranscripcion(BaseModel):
    transcripcion: str
    acta: str
    duracion_min: int
    n_speakers: int


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/")
def helloWorld():
    return {"message": "¡Hola Mundo!"}
