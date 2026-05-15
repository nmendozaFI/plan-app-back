"""
CONFIG TRIMESTRAL — CRUD para configuraciones trimestrales por empresa

Endpoints:
  GET  /{trimestre}             → Lista todas las configs del trimestre
  GET  /{trimestre}/resumen     → Resumen rápido
  GET  /{trimestre}/exportar-excel → Exporta configs a Excel (formato ideal)
  POST /{trimestre}/importar-excel → Importa Excel (detecta formato ideal/legacy)
  PUT  /{trimestre}/batch       → Actualiza múltiples configs
  POST /{trimestre}/inicializar → Inicializa configs (clonar o crear default)
  PUT  /{trimestre}/{empresaId} → Actualiza config de una empresa

IMPORTANT: Route order matters! More specific routes (/batch, /resumen, /inicializar,
/exportar-excel, /importar-excel) must be defined BEFORE the catch-all /{empresaId} route.

Tablas: configTrimestral, empresa
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional
from io import BytesIO
import re

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class ConfigTrimestralOut(BaseModel):
    id: int
    empresa_id: int
    empresa_nombre: str
    tipo_participacion: str  # EF, IT, AMBAS
    escuela_propia: bool
    permite_extras: bool  # V22 (Cambio A): empresa puede recibir EXTRA en este trimestre.
    frecuencia_solicitada: Optional[int]
    frecuencia_ef: Optional[int]  # V24 (Cambio B): input planificadora para matriz semáforo
    frecuencia_it: Optional[int]  # V24 (Cambio B): input planificadora para matriz semáforo
    disponibilidad_dias: str  # "L,M,X,J,V"
    turno_preferido: Optional[str]  # "M", "T", null
    voluntarios_disponibles: int
    preferencias_taller: Optional[str]
    notas: Optional[str]


class ConfigTrimestralUpdate(BaseModel):
    tipo_participacion: Optional[str] = None
    escuela_propia: Optional[bool] = None
    permite_extras: Optional[bool] = None  # V22 (Cambio A)
    frecuencia_solicitada: Optional[int] = None
    frecuencia_ef: Optional[int] = None  # V24 (Cambio B)
    frecuencia_it: Optional[int] = None  # V24 (Cambio B)
    disponibilidad_dias: Optional[str] = None
    turno_preferido: Optional[str] = None
    voluntarios_disponibles: Optional[int] = None
    preferencias_taller: Optional[str] = None
    notas: Optional[str] = None


class ConfigBatchUpdateItem(BaseModel):
    empresa_id: int
    tipo_participacion: Optional[str] = None
    escuela_propia: Optional[bool] = None
    permite_extras: Optional[bool] = None  # V22 (Cambio A)
    frecuencia_solicitada: Optional[int] = None
    frecuencia_ef: Optional[int] = None  # V24 (Cambio B)
    frecuencia_it: Optional[int] = None  # V24 (Cambio B)
    disponibilidad_dias: Optional[str] = None
    turno_preferido: Optional[str] = None
    voluntarios_disponibles: Optional[int] = None
    preferencias_taller: Optional[str] = None
    notas: Optional[str] = None


class ConfigBatchUpdateInput(BaseModel):
    updates: list[ConfigBatchUpdateItem]


class InicializarInput(BaseModel):
    origen_trimestre: Optional[str] = None  # Si se proporciona, clona desde ese trimestre


class InicializarResult(BaseModel):
    trimestre: str
    total_configs: int
    clonadas: int
    nuevas: int
    warnings: list[str]


class ConfigResumen(BaseModel):
    trimestre: str
    total_configs: int
    por_tipo: dict[str, int]  # EF, IT, AMBAS
    con_frecuencia: int
    sin_frecuencia: int
    escuela_propia: int
    permite_extras: int  # V22 (Cambio A): count of CTs with permiteExtras=true.


class ImportPreviewItem(BaseModel):
    """V24 (Cambio B, decisiones D2/D3/D9): preview item del bulk CT importer
    en formato 10-columnas. Una fila por empresa con freqEF/freqIT separados,
    EP/PE explícitos, sin frecuencia única.
    """
    empresa_id: int
    nombre: str
    # V24: split EF/IT (NULL permitido si la fila los deja vacíos).
    frecuencia_ef: Optional[int] = None
    frecuencia_it: Optional[int] = None
    tipo: Optional[str] = None  # EF | IT | AMBAS
    dias: Optional[str] = None
    turno: Optional[str] = None  # M | T | null
    voluntarios: Optional[int] = None
    # V24 D3: override total — Excel manda. Cell vacío se interpreta como False.
    escuela_propia: bool = False
    permite_extras: bool = False
    notas: Optional[str] = None


class ImportarExcelResponse(BaseModel):
    """V24 (Cambio B): response del bulk CT importer. `formato_detectado`
    queda fijo en "v24-split-efit" — los formatos legacy/ideal fueron
    eliminados (decisión D2: NO mantener compat con formato viejo).
    """
    trimestre: str
    formato_detectado: str  # siempre "v24-split-efit" en V24+
    total_procesados: int
    aplicados: int  # 0 if dry_run
    preview: list[ImportPreviewItem]
    warnings: list[str]
    dry_run: bool


# V21 / F3a: empresas EP del trimestre — feeds the "Añadir EXTRA" modal Select.
class EmpresaEPOut(BaseModel):
    id: int
    nombre: str
    tipo: str  # EF | IT | AMBAS
    activa: bool


class ListaEmpresasEPResponse(BaseModel):
    trimestre: str
    total: int
    empresas: list[EmpresaEPOut]


# ── Helper Functions ────────────────────────────────────────

def parse_legacy_frequency(value) -> int:
    """
    Parse frequency strings like "3 + MP" → 3.
    Extracts first integer found, or 0 if none.
    Used by V24 import (10-col) and by other consumers of "first int in cell".
    """
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    if not s:
        return 0
    match = re.search(r"\d+", s)
    return int(match.group()) if match else 0


def _parse_freq_or_null(value) -> Optional[int]:
    """V24: int o None. Empty/whitespace → None (no participa en ese tipo).
    Distinto de parse_legacy_frequency que devuelve 0 para vacíos.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    match = re.search(r"\d+", s)
    return int(match.group()) if match else None


def _parse_bool_strict(value) -> bool:
    """V24: SI/NO/TRUE/FALSE/1/0 → bool. Empty/desconocido → False
    (decisión D3: Excel manda, ausencia interpretada como False)."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().upper() in ("SI", "SÍ", "YES", "TRUE", "1", "X")


def normalize_empresa_name(name: str) -> str:
    """Normalize empresa name for fuzzy matching."""
    # Remove MAD suffix for legacy format matching
    normalized = name.strip()
    if normalized.upper().endswith(" MAD"):
        normalized = normalized[:-4].strip()
    return normalized.lower()


# ── Endpoints ────────────────────────────────────────────────
# NOTE: Order matters! Specific routes must come before parameterized routes.


@router.get("/{trimestre}")
async def obtener_configs_trimestre(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Devuelve todas las configuraciones trimestrales para el trimestre dado,
    unidas con el nombre de la empresa.
    """
    result = await db.execute(
        text("""
            SELECT
                ct.id,
                ct."empresaId" AS empresa_id,
                e.nombre AS empresa_nombre,
                ct."tipoParticipacion" AS tipo_participacion,
                ct."escuelaPropia" AS escuela_propia,
                ct."permiteExtras" AS permite_extras,
                ct."frecuenciaSolicitada" AS frecuencia_solicitada,
                ct."frecuenciaEF" AS frecuencia_ef,
                ct."frecuenciaIT" AS frecuencia_it,
                ct."disponibilidadDias" AS disponibilidad_dias,
                ct."turnoPreferido" AS turno_preferido,
                ct."voluntariosDisponibles" AS voluntarios_disponibles,
                ct."preferenciasTaller" AS preferencias_taller,
                ct.notas
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct.trimestre = :trimestre
            ORDER BY e.nombre
        """),
        {"trimestre": trimestre},
    )
    configs = [dict(r) for r in result.mappings().all()]

    return {
        "trimestre": trimestre,
        "total": len(configs),
        "configs": configs,
    }


@router.get("/{trimestre}/resumen")
async def resumen_configs(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Devuelve un resumen rápido de las configuraciones del trimestre.
    """
    # Total y por tipo
    result = await db.execute(
        text("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN "tipoParticipacion" = 'EF' THEN 1 ELSE 0 END) AS ef,
                SUM(CASE WHEN "tipoParticipacion" = 'IT' THEN 1 ELSE 0 END) AS it,
                SUM(CASE WHEN "tipoParticipacion" = 'AMBAS' THEN 1 ELSE 0 END) AS ambas,
                SUM(CASE WHEN "frecuenciaSolicitada" IS NOT NULL AND "frecuenciaSolicitada" > 0 THEN 1 ELSE 0 END) AS con_freq,
                SUM(CASE WHEN "frecuenciaSolicitada" IS NULL OR "frecuenciaSolicitada" = 0 THEN 1 ELSE 0 END) AS sin_freq,
                SUM(CASE WHEN "escuelaPropia" = true THEN 1 ELSE 0 END) AS escuela_propia,
                SUM(CASE WHEN "permiteExtras" = true THEN 1 ELSE 0 END) AS permite_extras
            FROM "configTrimestral"
            WHERE trimestre = :tri
        """),
        {"tri": trimestre},
    )
    row = result.mappings().first()

    if not row or row["total"] == 0:
        return {
            "trimestre": trimestre,
            "total_configs": 0,
            "por_tipo": {"EF": 0, "IT": 0, "AMBAS": 0},
            "con_frecuencia": 0,
            "sin_frecuencia": 0,
            "escuela_propia": 0,
            "permite_extras": 0,
        }

    return {
        "trimestre": trimestre,
        "total_configs": row["total"] or 0,
        "por_tipo": {
            "EF": row["ef"] or 0,
            "IT": row["it"] or 0,
            "AMBAS": row["ambas"] or 0,
        },
        "con_frecuencia": row["con_freq"] or 0,
        "sin_frecuencia": row["sin_freq"] or 0,
        "escuela_propia": row["escuela_propia"] or 0,
        "permite_extras": row["permite_extras"] or 0,
    }


@router.get("/{trimestre}/empresas-ep", response_model=ListaEmpresasEPResponse)
async def listar_empresas_ep(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    V21 / F3a: lista empresas con escuelaPropia=true en el trimestre dado.
    Filtra también por empresa.activa=true. Orden alfabético por nombre.
    Usado por el modal "Añadir EXTRA" de Operación para poblar el Select.
    Trimestre inexistente → 200 con lista vacía (no 404).
    """
    result = await db.execute(
        text("""
            SELECT
                e.id,
                e.nombre,
                e.tipo,
                e.activa
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct.trimestre = :tri
              AND ct."escuelaPropia" = true
              AND e.activa = true
            ORDER BY e.nombre ASC
        """),
        {"tri": trimestre},
    )
    rows = result.mappings().all()
    empresas = [
        EmpresaEPOut(
            id=r["id"],
            nombre=r["nombre"],
            tipo=r["tipo"],
            activa=r["activa"],
        )
        for r in rows
    ]

    return ListaEmpresasEPResponse(
        trimestre=trimestre,
        total=len(empresas),
        empresas=empresas,
    )


@router.get(
    "/{trimestre}/empresas-permite-extras",
    response_model=ListaEmpresasEPResponse,
)
async def listar_empresas_permite_extras(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """V22 (Cambio A): lista empresas con permiteExtras=true en el trimestre.

    Filtra también por empresa.activa=true. Orden alfabético por nombre.
    Usado por el modal "Añadir EXTRA" de Operación (Fase 6) para poblar el Select
    de empresas elegibles. Trimestre inexistente → 200 con lista vacía.

    Análogo a /empresas-ep, pero el gate es permiteExtras en vez de escuelaPropia.
    Reusa el mismo response model porque la forma de la fila es idéntica.
    """
    result = await db.execute(
        text("""
            SELECT
                e.id,
                e.nombre,
                e.tipo,
                e.activa
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct.trimestre = :tri
              AND ct."permiteExtras" = true
              AND e.activa = true
            ORDER BY e.nombre ASC
        """),
        {"tri": trimestre},
    )
    rows = result.mappings().all()
    empresas = [
        EmpresaEPOut(
            id=r["id"],
            nombre=r["nombre"],
            tipo=r["tipo"],
            activa=r["activa"],
        )
        for r in rows
    ]

    return ListaEmpresasEPResponse(
        trimestre=trimestre,
        total=len(empresas),
        empresas=empresas,
    )


@router.get("/{trimestre}/exportar-excel")
async def exportar_excel(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Exporta las configuraciones trimestrales a Excel.
    V24 (Cambio B, decisión D9): la columna única "Frecuencia" se reemplaza por
    "Frecuencia EF" y "Frecuencia IT". Se añaden "Escuela Propia" y
    "Permite Extras" como SI/NO.
    Orden de columnas:
      Empresa | Frecuencia EF | Frecuencia IT | Tipo | Dias | Turno |
      Voluntarios | Escuela Propia | Permite Extras | Notas
    """
    result = await db.execute(
        text("""
            SELECT
                e.nombre AS empresa,
                ct."frecuenciaEF" AS frecuencia_ef,
                ct."frecuenciaIT" AS frecuencia_it,
                ct."tipoParticipacion" AS tipo,
                ct."disponibilidadDias" AS dias,
                ct."turnoPreferido" AS turno,
                ct."voluntariosDisponibles" AS voluntarios,
                ct."escuelaPropia" AS escuela_propia,
                ct."permiteExtras" AS permite_extras,
                ct.notas
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct.trimestre = :tri
            ORDER BY e.nombre
        """),
        {"tri": trimestre},
    )
    rows = [dict(r) for r in result.mappings().all()]

    # Create workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "ConfigTrimestral"

    # Header style
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center")
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    # Headers — V24 (Cambio B, decisión D9): 10 columnas, sin "Frecuencia" single.
    headers = [
        "Empresa", "Frecuencia EF", "Frecuencia IT", "Tipo", "Dias", "Turno",
        "Voluntarios", "Escuela Propia", "Permite Extras", "Notas",
    ]
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment
        cell.border = thin_border

    def _bool_str(v: object) -> str:
        """Render bool as SI/NO; None as NO."""
        return "SI" if bool(v) else "NO"

    # Data rows. Frecuencia EF / IT pueden ser NULL → celda vacía (decisión D2 +
    # D7: NULL no se baja como 0 en el export, así el bulk import distingue
    # "no tocar" de "explícitamente cero" si lo necesita más adelante).
    for row_idx, row_data in enumerate(rows, 2):
        ws.cell(row=row_idx, column=1, value=row_data["empresa"])
        ws.cell(row=row_idx, column=2, value=row_data["frecuencia_ef"])  # may be NULL
        ws.cell(row=row_idx, column=3, value=row_data["frecuencia_it"])  # may be NULL
        ws.cell(row=row_idx, column=4, value=row_data["tipo"] or "AMBAS")
        ws.cell(row=row_idx, column=5, value=row_data["dias"] or "L,M,X,J,V")
        ws.cell(row=row_idx, column=6, value=row_data["turno"] or "-")
        ws.cell(row=row_idx, column=7, value=row_data["voluntarios"] or 0)
        ws.cell(row=row_idx, column=8, value=_bool_str(row_data["escuela_propia"]))
        ws.cell(row=row_idx, column=9, value=_bool_str(row_data["permite_extras"]))
        ws.cell(row=row_idx, column=10, value=row_data["notas"] or "")

    # Adjust column widths
    ws.column_dimensions["A"].width = 35  # Empresa
    ws.column_dimensions["B"].width = 14  # Frecuencia EF
    ws.column_dimensions["C"].width = 14  # Frecuencia IT
    ws.column_dimensions["D"].width = 10  # Tipo
    ws.column_dimensions["E"].width = 15  # Dias
    ws.column_dimensions["F"].width = 8   # Turno
    ws.column_dimensions["G"].width = 12  # Voluntarios
    ws.column_dimensions["H"].width = 16  # Escuela Propia
    ws.column_dimensions["I"].width = 16  # Permite Extras
    ws.column_dimensions["J"].width = 40  # Notas

    # Save to buffer
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    filename = f"config_trimestral_{trimestre}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# V24 (Cambio B, D2): formato único 10 columnas. Mapping de header → canon
# (case-insensitive, espacios/guion-bajo ignorados).
_V24_HEADER_MAP: dict[str, str] = {
    "empresa": "empresa",
    "frecuenciaef": "freq_ef",
    "frecef": "freq_ef",
    "freqef": "freq_ef",
    "freq_ef": "freq_ef",
    "frecuenciait": "freq_it",
    "frecit": "freq_it",
    "freqit": "freq_it",
    "freq_it": "freq_it",
    "tipo": "tipo",
    "dias": "dias",
    "días": "dias",
    "turno": "turno",
    "voluntarios": "voluntarios",
    "escuelapropia": "escuela_propia",
    "escuela_propia": "escuela_propia",
    "ep": "escuela_propia",
    "permiteextras": "permite_extras",
    "permite_extras": "permite_extras",
    "pe": "permite_extras",
    "notas": "notas",
}

_V24_REQUIRED_HEADERS = {
    "empresa", "freq_ef", "freq_it", "escuela_propia", "permite_extras",
}


def _normalize_header(raw: object) -> str:
    """Normaliza un header de Excel: strip + lowercase + quita espacios y guion bajo."""
    if raw is None:
        return ""
    return str(raw).strip().lower().replace(" ", "").replace("_", "")


@router.post("/{trimestre}/importar-excel", response_model=ImportarExcelResponse)
async def importar_excel(
    trimestre: str,
    file: UploadFile = File(...),
    dry_run: bool = Form(True),
    db: AsyncSession = Depends(get_db),
):
    """
    V24 (Cambio B, decisiones D2/D3/D9): bulk import de configTrimestral en
    formato único 10-columnas.

    Columnas requeridas (case-insensitive, espacios/guion-bajo ignorados):
      - Empresa
      - Frecuencia EF (vacío = NULL = no participa en EF este trimestre)
      - Frecuencia IT (vacío = NULL = no participa en IT este trimestre)
      - Escuela Propia (SI / NO — override total, Excel manda, D3)
      - Permite Extras (SI / NO — override total, Excel manda, D3)

    Columnas opcionales: Tipo, Dias, Turno, Voluntarios, Notas. Si la columna
    falta o la celda está vacía, el UPDATE no toca ese campo en el CT existente;
    el INSERT usa defaults razonables.

    Comportamiento:
      - dry_run=true  → preview sin aplicar (cliente puede mostrarlo y pedir
        confirmación al planificador).
      - dry_run=false → UPSERT en `configTrimestral`. CT existente: UPDATE de
        freqEF/freqIT/EP/PE (siempre) + opcionales si vienen. CT no existente:
        INSERT con defaults para los campos no provistos.

    Los formatos legacy/ideal anteriores ya NO se aceptan (decisión D2).
    """
    # ── Cargar Excel ─────────────────────────────────────────
    try:
        content = await file.read()
        wb = load_workbook(BytesIO(content), data_only=True)
        ws = wb.active
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error leyendo Excel: {e}")

    # ── Resolver headers ─────────────────────────────────────
    headers: dict[str, int] = {}  # canon → col_index (1-based)
    for col_idx, cell in enumerate(ws[1], 1):
        canon = _V24_HEADER_MAP.get(_normalize_header(cell.value))
        if canon:
            headers[canon] = col_idx

    missing = _V24_REQUIRED_HEADERS - set(headers.keys())
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Columnas requeridas no encontradas: {sorted(missing)}. "
                f"Formato esperado V24 (Cambio B): Empresa, Frecuencia EF, "
                f"Frecuencia IT, Tipo (opcional), Dias (opc.), Turno (opc.), "
                f"Voluntarios (opc.), Escuela Propia, Permite Extras, "
                f"Notas (opc.). Los formatos legacy/ideal anteriores ya no se "
                f"aceptan."
            ),
        )

    # ── Cargar empresas activas para matching ────────────────
    emp_result = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE activa = true")
    )
    empresas = {
        normalize_empresa_name(r["nombre"]): {"id": r["id"], "nombre": r["nombre"]}
        for r in emp_result.mappings().all()
    }

    preview: list[ImportPreviewItem] = []
    warnings: list[str] = []
    total_procesados = 0

    def _cell(row: tuple, canon: str) -> object:
        col = headers.get(canon)
        if col is None:
            return None
        # row es 0-indexado, headers son 1-indexado.
        if col - 1 >= len(row):
            return None
        return row[col - 1]

    for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), 2):
        empresa_raw = _cell(row, "empresa")
        if not row or not empresa_raw:
            continue

        empresa_name = str(empresa_raw).strip()
        normalized = normalize_empresa_name(empresa_name)
        total_procesados += 1

        # Match empresa por nombre (exacto + fuzzy).
        match = empresas.get(normalized)
        if not match:
            for key, val in empresas.items():
                if normalized in key or key in normalized:
                    match = val
                    break
        if not match:
            warnings.append(f"Fila {row_idx}: Empresa '{empresa_name}' no encontrada")
            continue

        # Parseo de campos.
        freq_ef = _parse_freq_or_null(_cell(row, "freq_ef"))
        freq_it = _parse_freq_or_null(_cell(row, "freq_it"))
        ep_val = _parse_bool_strict(_cell(row, "escuela_propia"))
        pe_val = _parse_bool_strict(_cell(row, "permite_extras"))

        # Tipo opcional. Si viene, debe ser EF/IT/AMBAS.
        tipo_raw = _cell(row, "tipo")
        tipo: Optional[str] = None
        if tipo_raw not in (None, ""):
            tipo_candidate = str(tipo_raw).strip().upper()
            if tipo_candidate in ("EF", "IT", "AMBAS"):
                tipo = tipo_candidate
            else:
                warnings.append(
                    f"Fila {row_idx}: Tipo '{tipo_raw}' inválido — se ignora (debe ser EF/IT/AMBAS)"
                )

        # Dias opcional.
        dias_raw = _cell(row, "dias")
        dias = str(dias_raw).strip() if dias_raw not in (None, "") else None

        # Turno opcional (M/T).
        turno_raw = _cell(row, "turno")
        turno: Optional[str] = None
        if turno_raw not in (None, ""):
            turno_candidate = str(turno_raw).strip().upper()
            if turno_candidate in ("M", "T"):
                turno = turno_candidate
            else:
                warnings.append(
                    f"Fila {row_idx}: Turno '{turno_raw}' inválido — se ignora (debe ser M/T)"
                )

        # Voluntarios opcional.
        vol_raw = _cell(row, "voluntarios")
        voluntarios: Optional[int] = None
        if vol_raw not in (None, ""):
            try:
                voluntarios = int(vol_raw)
            except (ValueError, TypeError):
                warnings.append(
                    f"Fila {row_idx}: Voluntarios '{vol_raw}' inválido — se ignora"
                )

        # Notas opcional.
        notas_raw = _cell(row, "notas")
        notas = str(notas_raw).strip() if notas_raw not in (None, "") else None

        preview.append(ImportPreviewItem(
            empresa_id=match["id"],
            nombre=match["nombre"],
            frecuencia_ef=freq_ef,
            frecuencia_it=freq_it,
            tipo=tipo,
            dias=dias,
            turno=turno,
            voluntarios=voluntarios,
            escuela_propia=ep_val,
            permite_extras=pe_val,
            notas=notas,
        ))

    # ── Apply (solo si !dry_run) ─────────────────────────────
    aplicados = 0
    if not dry_run and preview:
        for item in preview:
            # Comprobar si existe CT para esta empresa+trimestre.
            existing = await db.execute(
                text(
                    'SELECT id FROM "configTrimestral" '
                    'WHERE "empresaId" = :eid AND trimestre = :tri'
                ),
                {"eid": item.empresa_id, "tri": trimestre},
            )
            ct_exists = existing.first() is not None

            if ct_exists:
                # UPDATE dinámico. freqEF, freqIT, EP, PE SIEMPRE se setean
                # (override total, decisión D3). Opcionales: solo si la fila
                # del Excel los trae (campo no-None).
                updates = [
                    '"frecuenciaEF" = :freq_ef',
                    '"frecuenciaIT" = :freq_it',
                    '"escuelaPropia" = :ep',
                    '"permiteExtras" = :pe',
                    '"updatedAt" = NOW()',
                ]
                params: dict = {
                    "eid": item.empresa_id,
                    "tri": trimestre,
                    "freq_ef": item.frecuencia_ef,
                    "freq_it": item.frecuencia_it,
                    "ep": item.escuela_propia,
                    "pe": item.permite_extras,
                }
                if item.tipo is not None:
                    updates.append('"tipoParticipacion" = :tipo')
                    params["tipo"] = item.tipo
                if item.dias is not None:
                    updates.append('"disponibilidadDias" = :dias')
                    params["dias"] = item.dias
                if item.turno is not None:
                    updates.append('"turnoPreferido" = :turno')
                    params["turno"] = item.turno
                if item.voluntarios is not None:
                    updates.append('"voluntariosDisponibles" = :vol')
                    params["vol"] = item.voluntarios
                if item.notas is not None:
                    updates.append("notas = :notas")
                    params["notas"] = item.notas

                query = (
                    'UPDATE "configTrimestral" SET '
                    + ", ".join(updates)
                    + ' WHERE "empresaId" = :eid AND trimestre = :tri'
                )
                result = await db.execute(text(query), params)
                if result.rowcount > 0:
                    aplicados += 1
            else:
                # INSERT con defaults para los campos opcionales no provistos.
                await db.execute(
                    text("""
                        INSERT INTO "configTrimestral" (
                            "empresaId", trimestre, "tipoParticipacion",
                            "escuelaPropia", "permiteExtras",
                            "frecuenciaEF", "frecuenciaIT",
                            "disponibilidadDias", "turnoPreferido",
                            "voluntariosDisponibles", notas, "updatedAt"
                        ) VALUES (
                            :eid, :tri, :tipo, :ep, :pe,
                            :freq_ef, :freq_it,
                            :dias, :turno, :vol, :notas, NOW()
                        )
                    """),
                    {
                        "eid": item.empresa_id,
                        "tri": trimestre,
                        "tipo": item.tipo or "AMBAS",
                        "ep": item.escuela_propia,
                        "pe": item.permite_extras,
                        "freq_ef": item.frecuencia_ef,
                        "freq_it": item.frecuencia_it,
                        "dias": item.dias or "L,M,X,J,V",
                        "turno": item.turno,
                        "vol": item.voluntarios if item.voluntarios is not None else 0,
                        "notas": item.notas,
                    },
                )
                aplicados += 1

        await db.commit()

    return ImportarExcelResponse(
        trimestre=trimestre,
        formato_detectado="v24-split-efit",
        total_procesados=total_procesados,
        aplicados=aplicados,
        preview=preview,
        warnings=warnings,
        dry_run=dry_run,
    )


@router.put("/{trimestre}/batch")
async def actualizar_configs_batch(
    trimestre: str,
    body: ConfigBatchUpdateInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Actualiza múltiples configuraciones en una sola llamada.
    Ideal para guardar cambios de la tabla editable.
    """
    updated = 0
    errors = []

    for item in body.updates:
        try:
            # Verificar que existe la config
            cfg_check = await db.execute(
                text("""
                    SELECT id FROM "configTrimestral"
                    WHERE "empresaId" = :eid AND trimestre = :tri
                """),
                {"eid": item.empresa_id, "tri": trimestre},
            )
            if not cfg_check.first():
                errors.append(f"Config para empresa {item.empresa_id} no encontrada")
                continue

            updates = []
            params = {"eid": item.empresa_id, "tri": trimestre}

            if item.tipo_participacion is not None:
                updates.append('"tipoParticipacion" = :tipo')
                params["tipo"] = item.tipo_participacion

            if item.escuela_propia is not None:
                updates.append('"escuelaPropia" = :escuela')
                params["escuela"] = item.escuela_propia

            if item.permite_extras is not None:
                updates.append('"permiteExtras" = :permite')
                params["permite"] = item.permite_extras

            if item.frecuencia_solicitada is not None:
                updates.append('"frecuenciaSolicitada" = :freq')
                params["freq"] = item.frecuencia_solicitada

            # V24 (Cambio B): EF e IT son los inputs reales de la matriz
            # semáforo. None significa "no tocar este campo"; explícito None
            # se manda via PUT específico si la planificadora quiere NULL-ear
            # una empresa para sacarla del cálculo.
            if item.frecuencia_ef is not None:
                updates.append('"frecuenciaEF" = :freq_ef')
                params["freq_ef"] = item.frecuencia_ef

            if item.frecuencia_it is not None:
                updates.append('"frecuenciaIT" = :freq_it')
                params["freq_it"] = item.frecuencia_it

            if item.disponibilidad_dias is not None:
                updates.append('"disponibilidadDias" = :dias')
                params["dias"] = item.disponibilidad_dias

            if item.turno_preferido is not None:
                updates.append('"turnoPreferido" = :turno')
                params["turno"] = item.turno_preferido if item.turno_preferido != "" else None

            if item.voluntarios_disponibles is not None:
                updates.append('"voluntariosDisponibles" = :vol')
                params["vol"] = item.voluntarios_disponibles

            if item.preferencias_taller is not None:
                updates.append('"preferenciasTaller" = :pref')
                params["pref"] = item.preferencias_taller

            if item.notas is not None:
                updates.append("notas = :notas")
                params["notas"] = item.notas

            if updates:
                updates.append('"updatedAt" = NOW()')
                query = f"""
                    UPDATE "configTrimestral"
                    SET {', '.join(updates)}
                    WHERE "empresaId" = :eid AND trimestre = :tri
                """
                await db.execute(text(query), params)
                updated += 1

        except Exception as e:
            errors.append(f"Empresa {item.empresa_id}: {str(e)}")

    await db.commit()
    return {"updated": updated, "errors": errors}


@router.post("/{trimestre}/inicializar")
async def inicializar_configs(
    trimestre: str,
    body: InicializarInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Inicializa configuraciones para un trimestre nuevo.

    Si origen_trimestre se proporciona:
      - Clona las configs de ese trimestre (como el endpoint clonar-trimestre)

    Si no se proporciona:
      - Crea configs por defecto para todas las empresas activas
    """
    warnings: list[str] = []
    clonadas = 0
    nuevas = 0

    if body.origen_trimestre:
        # ── Modo clonar ──────────────────────────────────────
        # V22 (Cambio A): permiteExtras se copia 1:1 desde el trimestre origen.
        # V24 (Cambio B): frecuenciaEF / frecuenciaIT también se copian 1:1.
        rows = await db.execute(
            text("""
                SELECT
                    ct."empresaId",
                    e.nombre,
                    e.activa,
                    ct."tipoParticipacion",
                    ct."escuelaPropia",
                    ct."permiteExtras",
                    ct."turnoPreferido",
                    ct."frecuenciaSolicitada",
                    ct."frecuenciaEF",
                    ct."frecuenciaIT",
                    ct."disponibilidadDias",
                    ct."voluntariosDisponibles",
                    ct."preferenciasTaller",
                    ct.notas
                FROM "configTrimestral" ct
                JOIN empresa e ON e.id = ct."empresaId"
                WHERE ct.trimestre = :origen
                ORDER BY e.nombre
            """),
            {"origen": body.origen_trimestre},
        )
        configs = [dict(r) for r in rows.mappings().all()]

        if not configs:
            raise HTTPException(
                status_code=404,
                detail=f"No hay configuraciones para el trimestre {body.origen_trimestre}",
            )

        saltadas = []
        for cfg in configs:
            if not cfg["activa"]:
                saltadas.append(cfg["nombre"])
                continue

            result = await db.execute(
                text("""
                    INSERT INTO "configTrimestral" (
                        "empresaId", trimestre, "tipoParticipacion",
                        "escuelaPropia", "permiteExtras", "disponibilidadDias",
                        "turnoPreferido", "frecuenciaSolicitada",
                        "frecuenciaEF", "frecuenciaIT",
                        "voluntariosDisponibles", "preferenciasTaller",
                        notas, "updatedAt"
                    )
                    VALUES (
                        :eid, :destino, :tipo,
                        :escuela, :permite, :dias,
                        :turno, :freq,
                        :freq_ef, :freq_it,
                        :vol, :pref,
                        :notas, NOW()
                    )
                    ON CONFLICT ("empresaId", trimestre) DO NOTHING
                """),
                {
                    "eid": cfg["empresaId"],
                    "destino": trimestre,
                    "tipo": cfg["tipoParticipacion"],
                    "escuela": cfg["escuelaPropia"] or False,
                    "permite": cfg["permiteExtras"] or False,
                    "dias": cfg["disponibilidadDias"] or "L,M,X,J,V",
                    "turno": cfg["turnoPreferido"],
                    "freq": cfg["frecuenciaSolicitada"],
                    "freq_ef": cfg["frecuenciaEF"],  # V24 (Cambio B): puede ser NULL
                    "freq_it": cfg["frecuenciaIT"],  # V24 (Cambio B): puede ser NULL
                    "vol": cfg["voluntariosDisponibles"] or 0,
                    "pref": cfg["preferenciasTaller"],
                    "notas": cfg["notas"],
                },
            )
            if result.rowcount > 0:
                clonadas += 1

        if saltadas:
            warnings.append(
                f"{len(saltadas)} empresa(s) inactiva(s) no clonadas: {', '.join(saltadas[:5])}"
                + ("..." if len(saltadas) > 5 else "")
            )

    else:
        # ── Modo crear por defecto ───────────────────────────
        # V22 (Cambio A): permiteExtras hereda de empresa.aceptaExtras (baseline
        # maestro). El planificador puede después flipearlo per-trimestre desde
        # la UI sin tocar la empresa maestra.
        rows = await db.execute(
            text("""
                SELECT e.id, e.nombre, e.tipo, e."turnoPreferido", e."aceptaExtras"
                FROM empresa e
                WHERE e.activa = true
                AND NOT EXISTS (
                    SELECT 1 FROM "configTrimestral" ct
                    WHERE ct."empresaId" = e.id AND ct.trimestre = :tri
                )
                ORDER BY e.nombre
            """),
            {"tri": trimestre},
        )
        empresas = [dict(r) for r in rows.mappings().all()]

        for emp in empresas:
            await db.execute(
                text("""
                    INSERT INTO "configTrimestral" (
                        "empresaId", trimestre, "tipoParticipacion",
                        "escuelaPropia", "permiteExtras", "disponibilidadDias",
                        "turnoPreferido", "voluntariosDisponibles",
                        "updatedAt"
                    )
                    VALUES (
                        :eid, :tri, :tipo,
                        false, :permite, 'L,M,X,J,V',
                        :turno, 0,
                        NOW()
                    )
                """),
                {
                    "eid": emp["id"],
                    "tri": trimestre,
                    "tipo": emp["tipo"] or "AMBAS",
                    "permite": bool(emp["aceptaExtras"]),
                    "turno": emp["turnoPreferido"],
                },
            )
            nuevas += 1

    await db.commit()

    # Contar total de configs para el trimestre
    count_result = await db.execute(
        text('SELECT COUNT(*) FROM "configTrimestral" WHERE trimestre = :tri'),
        {"tri": trimestre},
    )
    total = count_result.scalar() or 0

    return {
        "trimestre": trimestre,
        "total_configs": total,
        "clonadas": clonadas,
        "nuevas": nuevas,
        "warnings": warnings,
    }


# NOTE: This route MUST come AFTER specific routes like /resumen, /batch, /inicializar
# because it catches all remaining paths with {empresa_id}
@router.put("/{trimestre}/{empresa_id}")
async def actualizar_config(
    trimestre: str,
    empresa_id: int,
    body: ConfigTrimestralUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Actualiza la configuración trimestral de una empresa.
    Si no existe, la crea con valores por defecto.
    """
    # Verificar que la empresa existe
    emp_check = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE id = :eid"),
        {"eid": empresa_id},
    )
    empresa = emp_check.mappings().first()
    if not empresa:
        raise HTTPException(status_code=404, detail=f"Empresa {empresa_id} no encontrada")

    # Verificar si existe la config
    cfg_check = await db.execute(
        text("""
            SELECT id FROM "configTrimestral"
            WHERE "empresaId" = :eid AND trimestre = :tri
        """),
        {"eid": empresa_id, "tri": trimestre},
    )
    existing = cfg_check.first()

    if existing:
        # Update existente
        updates = []
        params = {"eid": empresa_id, "tri": trimestre}

        if body.tipo_participacion is not None:
            updates.append('"tipoParticipacion" = :tipo')
            params["tipo"] = body.tipo_participacion

        if body.escuela_propia is not None:
            updates.append('"escuelaPropia" = :escuela')
            params["escuela"] = body.escuela_propia

        if body.permite_extras is not None:
            updates.append('"permiteExtras" = :permite')
            params["permite"] = body.permite_extras

        if body.frecuencia_solicitada is not None:
            updates.append('"frecuenciaSolicitada" = :freq')
            params["freq"] = body.frecuencia_solicitada

        # V24 (Cambio B): EF/IT — inputs reales de la matriz semáforo.
        if body.frecuencia_ef is not None:
            updates.append('"frecuenciaEF" = :freq_ef')
            params["freq_ef"] = body.frecuencia_ef

        if body.frecuencia_it is not None:
            updates.append('"frecuenciaIT" = :freq_it')
            params["freq_it"] = body.frecuencia_it

        if body.disponibilidad_dias is not None:
            updates.append('"disponibilidadDias" = :dias')
            params["dias"] = body.disponibilidad_dias

        if body.turno_preferido is not None:
            updates.append('"turnoPreferido" = :turno')
            params["turno"] = body.turno_preferido if body.turno_preferido != "" else None

        if body.voluntarios_disponibles is not None:
            updates.append('"voluntariosDisponibles" = :vol')
            params["vol"] = body.voluntarios_disponibles

        if body.preferencias_taller is not None:
            updates.append('"preferenciasTaller" = :pref')
            params["pref"] = body.preferencias_taller

        if body.notas is not None:
            updates.append("notas = :notas")
            params["notas"] = body.notas

        if updates:
            updates.append('"updatedAt" = NOW()')
            query = f"""
                UPDATE "configTrimestral"
                SET {', '.join(updates)}
                WHERE "empresaId" = :eid AND trimestre = :tri
            """
            await db.execute(text(query), params)
    else:
        # Crear nueva config
        # V24 (Cambio B): freqEF/freqIT en INSERT, default NULL si el body no los manda.
        await db.execute(
            text("""
                INSERT INTO "configTrimestral" (
                    "empresaId", trimestre, "tipoParticipacion",
                    "escuelaPropia", "permiteExtras", "frecuenciaSolicitada",
                    "frecuenciaEF", "frecuenciaIT",
                    "disponibilidadDias", "turnoPreferido",
                    "voluntariosDisponibles", "preferenciasTaller",
                    notas, "updatedAt"
                )
                VALUES (
                    :eid, :tri, :tipo, :escuela, :permite, :freq,
                    :freq_ef, :freq_it,
                    :dias, :turno, :vol, :pref, :notas, NOW()
                )
            """),
            {
                "eid": empresa_id,
                "tri": trimestre,
                "tipo": body.tipo_participacion or "AMBAS",
                "escuela": body.escuela_propia or False,
                "permite": body.permite_extras or False,
                "freq": body.frecuencia_solicitada,
                "freq_ef": body.frecuencia_ef,
                "freq_it": body.frecuencia_it,
                "dias": body.disponibilidad_dias or "L,M,X,J,V",
                "turno": body.turno_preferido if body.turno_preferido != "" else None,
                "vol": body.voluntarios_disponibles or 0,
                "pref": body.preferencias_taller,
                "notas": body.notas,
            },
        )

    await db.commit()

    # Devolver config actualizada
    result = await db.execute(
        text("""
            SELECT
                ct.id,
                ct."empresaId" AS empresa_id,
                e.nombre AS empresa_nombre,
                ct."tipoParticipacion" AS tipo_participacion,
                ct."escuelaPropia" AS escuela_propia,
                ct."permiteExtras" AS permite_extras,
                ct."frecuenciaSolicitada" AS frecuencia_solicitada,
                ct."frecuenciaEF" AS frecuencia_ef,
                ct."frecuenciaIT" AS frecuencia_it,
                ct."disponibilidadDias" AS disponibilidad_dias,
                ct."turnoPreferido" AS turno_preferido,
                ct."voluntariosDisponibles" AS voluntarios_disponibles,
                ct."preferenciasTaller" AS preferencias_taller,
                ct.notas
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct."empresaId" = :eid AND ct.trimestre = :tri
        """),
        {"eid": empresa_id, "tri": trimestre},
    )
    config = result.mappings().first()

    return {"config": dict(config) if config else None}
