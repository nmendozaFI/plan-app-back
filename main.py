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

from transcriptor import (
    validar_entorno,
    transcribir,
    diarizar,
    mergear,
    formatear_transcripcion,
    generar_acta,
)

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


@app.post("/transcribir", response_model=ResultadoTranscripcion)
async def endpoint_transcribir(
    audio: UploadFile = File(...),
    speakers: int = Form(default=2, ge=1, le=10),
):
    extensiones_ok = {".mp3", ".m4a", ".mp4", ".wav", ".ogg", ".flac"}
    ext = Path(audio.filename or "").suffix.lower()
    if ext not in extensiones_ok:
        raise HTTPException(status_code=400, detail=f"Formato no soportado: {ext}")

    # guardar temporalmente en disco (faster-whisper necesita path, no stream)
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        contenido = await audio.read()
        tmp.write(contenido)
        tmp_path = tmp.name

    try:
        validar_entorno()
        segmentos    = transcribir(tmp_path)
        diarization  = diarizar(tmp_path, num_speakers=speakers)
        segments     = mergear(segmentos, diarization)
        transcripcion = formatear_transcripcion(segments)
        acta         = generar_acta(transcripcion, segments)

        duracion_min = 0
        if segments:
            secs = segments[-1]["end"] - segments[0]["start"]
            duracion_min = max(1, int(secs // 60))

        n_speakers = len(set(s["speaker"] for s in segments))

        return ResultadoTranscripcion(
            transcripcion=transcripcion,
            acta=acta,
            duracion_min=duracion_min,
            n_speakers=n_speakers,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        Path(tmp_path).unlink(missing_ok=True)