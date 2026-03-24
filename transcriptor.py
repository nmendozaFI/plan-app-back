"""
Transcriptor con diarización — compatible con Windows, macOS y Linux
Stack: faster-whisper + pyannote.audio + Claude API
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime

try:
    from faster_whisper import WhisperModel
    from pyannote.audio import Pipeline
    import anthropic
    from dotenv import load_dotenv
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.panel import Panel
    from rich.text import Text
    from rich.table import Table
except ImportError as e:
    print(f"\n[ERROR] Falta instalar dependencias: {e}")
    print("Corré: pip install -r requirements.txt\n")
    sys.exit(1)

load_dotenv()
console = Console()

# ── configuración ─────────────────────────────────────────────────────────────
HF_TOKEN       = os.getenv("HF_TOKEN")
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY")
WHISPER_MODEL  = os.getenv("WHISPER_MODEL", "large-v3")
DEVICE         = "cpu"
COMPUTE_TYPE   = "int8"   # más liviano en CPU, soportado en Windows
LANGUAGE       = "es"

# ── helpers de consola ────────────────────────────────────────────────────────
def banner():
    console.print(Panel(
        Text("🎙  Transcriptor con Diarización", justify="center", style="bold white"),
        subtitle="faster-whisper + pyannote + Claude",
        border_style="cyan",
        padding=(0, 2),
    ))

def step(msg: str):  console.print(f"\n[cyan]▶[/cyan] {msg}")
def ok(msg: str):    console.print(f"[green]✓[/green] {msg}")
def error(msg: str): console.print(f"[red]✗[/red] {msg}"); sys.exit(1)

# ── validaciones iniciales ────────────────────────────────────────────────────
def validar_entorno():
    if not HF_TOKEN:
        error("HF_TOKEN no encontrado en .env")
    if not ANTHROPIC_KEY:
        error("ANTHROPIC_API_KEY no encontrado en .env")
    ok("Variables de entorno OK")

# ── 1. transcripción con faster-whisper ──────────────────────────────────────
def transcribir(audio_path: str) -> list[dict]:
    """
    Retorna lista de segmentos con start, end, text.
    faster-whisper devuelve generadores — los materializamos acá.
    """
    step(f"Cargando modelo Whisper ({WHISPER_MODEL}) — puede tardar la primera vez...")

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as p:
        p.add_task("Cargando modelo...", total=None)
        model = WhisperModel(WHISPER_MODEL, device=DEVICE, compute_type=COMPUTE_TYPE)

    step("Transcribiendo audio...")
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as p:
        p.add_task("Procesando...", total=None)
        segments_gen, info = model.transcribe(
            audio_path,
            language=LANGUAGE,
            beam_size=5,
            word_timestamps=True,   # necesario para el merge con diarización
            vad_filter=True,        # filtra silencios automáticamente
        )
        # materializamos el generador
        segments = [
            {
                "start": seg.start,
                "end":   seg.end,
                "text":  seg.text.strip(),
                "words": [
                    {"start": w.start, "end": w.end, "word": w.word}
                    for w in (seg.words or [])
                ],
            }
            for seg in segments_gen
            if seg.text.strip()
        ]

    ok(f"Transcripción: {len(segments)} segmentos · idioma detectado: {info.language}")
    return segments

# ── 2. diarización con pyannote ───────────────────────────────────────────────
def diarizar(audio_path: str, num_speakers: int | None = None):
    """Retorna un objeto Annotation de pyannote con los turnos de habla."""
    step("Ejecutando diarización de speakers (pyannote.audio)...")

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as p:
        p.add_task("Detectando speakers...", total=None)
        pipeline = Pipeline.from_pretrained(
            "pyannote/speaker-diarization-3.1",
            use_auth_token=HF_TOKEN,
        )
        kwargs = {}
        if num_speakers:
            kwargs["num_speakers"] = num_speakers
        diarization = pipeline(audio_path, **kwargs)

    ok("Diarización completada")
    return diarization

# ── 3. merge transcripción + diarización ─────────────────────────────────────
def mergear(segmentos: list[dict], diarization) -> list[dict]:
    """
    Para cada palabra en la transcripción busca qué speaker habla en ese instante
    según la diarización, y lo asigna al segmento.
    Estrategia: el speaker con mayor overlap sobre el segmento gana.
    """
    step("Combinando transcripción con speakers...")

    def speaker_en(t_start: float, t_end: float) -> str:
        """Devuelve el speaker con mayor overlap en [t_start, t_end]."""
        votos: dict[str, float] = {}
        for turn, _, speaker in diarization.itertracks(yield_label=True):
            overlap = min(turn.end, t_end) - max(turn.start, t_start)
            if overlap > 0:
                votos[speaker] = votos.get(speaker, 0) + overlap
        if not votos:
            return "SPEAKER_DESCONOCIDO"
        return max(votos, key=lambda s: votos[s])

    resultado = []
    for seg in segmentos:
        speaker = speaker_en(seg["start"], seg["end"])
        resultado.append({
            "speaker": speaker,
            "start":   round(seg["start"], 2),
            "end":     round(seg["end"], 2),
            "text":    seg["text"],
        })

    ok(f"Merge completo: {len(resultado)} segmentos con speaker asignado")
    return resultado

# ── 4. formatear transcripción cruda ─────────────────────────────────────────
def formatear_transcripcion(segments: list[dict]) -> str:
    def ts(s: float) -> str:
        m, sec = divmod(int(s), 60)
        return f"{m:02d}:{sec:02d}"

    lineas = []
    speaker_actual = None
    bloque: list[str] = []

    for seg in segments:
        sp = seg["speaker"]
        if sp != speaker_actual:
            if bloque:
                lineas.append(f"[{speaker_actual}] {' '.join(bloque)}")
                bloque = []
            speaker_actual = sp
            lineas.append(f"\n⏱ {ts(seg['start'])}")
        bloque.append(seg["text"])

    if bloque:
        lineas.append(f"[{speaker_actual}] {' '.join(bloque)}")

    return "\n".join(lineas)

# ── 5. generar acta con Claude ────────────────────────────────────────────────
SYSTEM_PROMPT = """Sos un asistente especializado en generar actas de reunión claras y estructuradas para organizaciones del tercer sector (ONGs, fundaciones).

Tu tarea es transformar una transcripción en bruto (con timestamps y etiquetas de speaker) en un acta profesional y útil.

Reglas:
- Escribí en español neutro, formal pero accesible
- Reorganizá el contenido temáticamente, no cronológicamente
- Identificá a los speakers por roles si es posible deducirlo del contexto; si no, usá "Participante A", "Participante B", etc.
- Ignorá muletillas, repeticiones y fragmentos sin contenido
- Sé conciso: el acta debe poder leerse en 2 minutos
- Usá Markdown para el formato
"""

ACTA_PROMPT = """A continuación la transcripción de una reunión:

{transcripcion}

---

Generá un acta con esta estructura exacta:

# Acta de reunión
**Fecha:** {fecha}
**Duración estimada:** {duracion}
**Participantes detectados:** {n_speakers}

## Resumen ejecutivo
(2-3 oraciones que capturen la esencia de la reunión)

## Temas tratados
(Lista de temas con un párrafo cada uno)

## Decisiones tomadas
(Lista con viñetas — solo decisiones concretas)

## Próximos pasos
(Lista con viñetas — acciones, responsables si se mencionan, fechas si se mencionan)

## Notas adicionales
(Omitir si no hay nada relevante)
"""

def generar_acta(transcripcion: str, segments: list[dict]) -> str:
    step("Generando acta estructurada con Claude...")

    duracion_min = 0
    if segments:
        secs = segments[-1]["end"] - segments[0]["start"]
        duracion_min = max(1, int(secs // 60))

    n_speakers = len(set(s["speaker"] for s in segments))

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as p:
        p.add_task("Claude generando acta...", total=None)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": ACTA_PROMPT.format(
                    transcripcion=transcripcion,
                    fecha=datetime.now().strftime("%d/%m/%Y"),
                    duracion=f"~{duracion_min} minutos",
                    n_speakers=n_speakers,
                )
            }],
        )

    acta = message.content[0].text
    ok("Acta generada")
    return acta

# ── 6. guardar outputs ────────────────────────────────────────────────────────
def guardar_outputs(audio_path: str, segments: list[dict], transcripcion: str, acta: str) -> dict:
    audio_name = Path(audio_path).stem
    ts_now = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("outputs") / f"{audio_name}_{ts_now}"
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "transcripcion_raw.txt").write_text(transcripcion, encoding="utf-8")
    (out_dir / "segments.json").write_text(json.dumps(segments, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "acta.md").write_text(acta, encoding="utf-8")

    return {
        "directorio":    str(out_dir),
        "transcripcion": str(out_dir / "transcripcion_raw.txt"),
        "segments_json": str(out_dir / "segments.json"),
        "acta":          str(out_dir / "acta.md"),
    }