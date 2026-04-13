"""
IMPORTACIÓN MASIVA — Carga de datos desde Excel maestro (v2)
FIX: comparación case-insensitive para evitar duplicados.

Endpoints:
  1. POST /api/importar/empresas   → upsert empresa + empresaCiudad + configTrimestral
  2. POST /api/importar/historico   → importar histórico desde Excel Q1
  3. GET  /api/importar/estado/{t}  → resumen de datos cargados

Tablas Prisma (camelCase → SQL con comillas dobles).
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel
from io import BytesIO

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────


class ImportEmpresasResult(BaseModel):
    total_empresas: int
    creadas: int
    actualizadas: int
    ciudades_creadas: list[str]
    empresa_ciudad_links: int
    config_trimestral_creadas: int
    semanas_excluidas: int
    warnings: list[str]


class ImportHistoricoResult(BaseModel):
    trimestre: str
    total_filas: int
    importadas_ok: int
    importadas_cancelado: int
    vacantes_ignoradas: int
    empresas_no_encontradas: list[str]
    talleres_no_encontrados: list[str]
    warnings: list[str]
    
 


# ── Constantes ───────────────────────────────────────────────

CIUDAD_ABREV = {
    "MAD": "MADRID",
    "MADRID": "MADRID",
    "BCN": "BARCELONA",
    "BARCELONA": "BARCELONA",
    "VLC": "VALENCIA",
    "VALENCIA": "VALENCIA",
    "ZGZ": "ZARAGOZA",
    "ZARAGOZA": "ZARAGOZA",
    "SEV": "SEVILLA",
    "SEVILLA": "SEVILLA",
    "MAL": "MALAGA",
    "MÁLAGA": "MALAGA",
    "MALAGA": "MALAGA",
    "MALL": "MALLORCA",
    "MALLORCA": "MALLORCA",
    "CAN": "CANARIAS",
    "CANARIAS": "CANARIAS",
}

CIUDAD_COLUMNS = {"mad", "bcn", "vlc", "zgz", "sev", "mal", "málag", "mall", "can"}

HEADER_MAP = {
    "nombre": "nombre",
    "tipo": "tipo",
    "semaforo": "semaforo",
    "semáforo": "semaforo",
    "scorev3": "scoreV3",
    "score": "scoreV3",
    "score_v3": "scoreV3",
    "fiabilidadreciente": "fiabilidadReciente",
    "fiabilidad": "fiabilidadReciente",
    "escomodin": "esComodin",
    "comodin": "esComodin",
    "aceptaextras": "aceptaExtras",
    "extras": "aceptaExtras",
    "maxextrastrimestre": "maxExtrasTrimestre",
    "max_extras": "maxExtrasTrimestre",
    "prioridadreduccion": "prioridadReduccion",
    "prioridad": "prioridadReduccion",
    "tienebolsa": "tieneBolsa",
    "bolsa": "tieneBolsa",
    "turnopreferido": "turnoPreferido",
    "turno": "turnoPreferido",
    "activa": "activa",
    "notas": "notas",
    # Frecuencia solicitada (anula el cálculo automático)
    "frecuenciasolicitada": "frecuenciaSolicitada",
    "frecuencia": "frecuenciaSolicitada",
    "freq": "frecuenciaSolicitada",
    "talleres": "frecuenciaSolicitada",
}

# Normalización empresas Q1
NORMALIZE_EMPRESA_Q1 = {
    "ACCIONA ": "ACCIONA",
    "AECOM ": "AECOM",
    "ATREVIA ": "ATREVIA",
    "BANKINTER": "BANKINTER",
    " INDRA": "INDRA",
    "MP CAPGEMINI": "CAPGEMINI",
    "MP ENDESA": "ENDESA",
    "MP LDA": "LDA",
    "MP AON": "F. AON",
    "MP SANTANDER": "B.SANTANDER",
    "SERVITEC / CAPGEMINI": "SERVITEC",
    "SERVITEC / SERVITEC": "SERVITEC",
    "URBASER / SACYR": "URBASER",
}


# ── Parsers ──────────────────────────────────────────────────


def _bool(val) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    return str(val).strip().upper() in ("SI", "SÍ", "YES", "TRUE", "1", "X")


def _float(val, d=0.0) -> float:
    if val is None or str(val).strip() == "":
        return d
    try:
        return float(val)
    except:  # noqa: E722
        return d


def _int(val, d=0) -> int:
    if val is None or str(val).strip() == "":
        return d
    try:
        return int(float(val))
    except:  # noqa: E722
        return d


def _str(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _semaforo(val) -> str:
    if val is None:
        return "AMBAR"
    s = str(val).strip().upper().replace("Á", "A")
    return s if s in ("VERDE", "AMBAR", "ROJO") else "AMBAR"


def _programa(val) -> str:
    if val is None:
        return "AMBAS"
    s = str(val).strip().upper()
    return s if s in ("EF", "IT", "AMBAS") else "AMBAS"


def _prioridad(val) -> str:
    if val is None:
        return "MEDIA"
    s = str(val).strip().upper()
    return s if s in ("ALTA", "MEDIA", "BAJA") else "MEDIA"


# ══════════════════════════════════════════════════════════════
# ENDPOINT 1: Importar empresas
# ══════════════════════════════════════════════════════════════


@router.post("/empresas", response_model=ImportEmpresasResult)
async def importar_empresas(
    file: UploadFile = File(...),
    trimestre: str = Form("2026-Q2"),
    db: AsyncSession = Depends(get_db),
):
    import openpyxl

    content = await file.read()
    wb = openpyxl.load_workbook(BytesIO(content))
    warnings: list[str] = []

    # ── Buscar hoja Empresas ─────────────────────────────────
    sheet_map = {s.lower(): s for s in wb.sheetnames}
    ws_name = sheet_map.get("empresas") or sheet_map.get("empresa")
    if not ws_name:
        raise HTTPException(
            400, f"Hoja 'Empresas' no encontrada. Hojas: {wb.sheetnames}"
        )
    ws = wb[ws_name]

    # ── Detectar columnas ────────────────────────────────────
    headers = [str(c.value or "").strip().lower() for c in ws[1]]
    col_map: dict[str, int] = {}
    ciudad_cols: dict[str, int] = {}
    ciudades_csv_col: int | None = None

    for idx, h in enumerate(headers):
        h_clean = h.replace(" ", "").replace("_", "").lower()
        if h_clean in HEADER_MAP:
            col_map[HEADER_MAP[h_clean]] = idx
        elif h_clean in CIUDAD_COLUMNS or h.upper() in CIUDAD_ABREV:
            ciudad_cols[h.upper()] = idx
        elif h_clean == "ciudades":
            ciudades_csv_col = idx

    if "nombre" not in col_map:
        raise HTTPException(400, f"Columna 'nombre' no encontrada. Headers: {headers}")

    # ── Leer filas ───────────────────────────────────────────
    empresas_data: list[dict] = []
    seen_names: set[str] = set()

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, values_only=True):
        nombre_raw = (
            row[col_map["nombre"]] if col_map.get("nombre") is not None else None
        )
        if not nombre_raw or str(nombre_raw).strip() == "":
            continue

        nombre = str(nombre_raw).strip().upper()

        # Dedup dentro del mismo Excel
        if nombre in seen_names:
            warnings.append(f"Empresa duplicada en Excel (ignorada): {nombre}")
            continue
        seen_names.add(nombre)

        # Ciudades
        ciudades: list[str] = []
        for abrev, ci in ciudad_cols.items():
            val = row[ci] if ci < len(row) else None
            if val and str(val).strip().upper() in (
                "X",
                "SI",
                "SÍ",
                "YES",
                "1",
                "TRUE",
            ):
                cn = CIUDAD_ABREV.get(abrev.upper(), abrev.upper())
                if cn not in ciudades:
                    ciudades.append(cn)

        if ciudades_csv_col is not None and not ciudades:
            csv_val = row[ciudades_csv_col] if ciudades_csv_col < len(row) else None
            if csv_val:
                for c in str(csv_val).split(","):
                    cn = CIUDAD_ABREV.get(c.strip().upper(), c.strip().upper())
                    if cn and cn not in ciudades:
                        ciudades.append(cn)

        if not ciudades:
            ciudades = ["MADRID"]

        def _get(field, default=None):
            idx = col_map.get(field)
            if idx is None or idx >= len(row):
                return default
            return row[idx]

        empresas_data.append(
            {
                "nombre": nombre,
                "tipo": _programa(_get("tipo")),
                "semaforo": _semaforo(_get("semaforo")),
                "scoreV3": _float(_get("scoreV3")),
                "fiabilidadReciente": _float(_get("fiabilidadReciente")),
                "esComodin": _bool(_get("esComodin")),
                "aceptaExtras": _bool(_get("aceptaExtras")),
                "maxExtrasTrimestre": _int(_get("maxExtrasTrimestre")),
                "prioridadReduccion": _prioridad(_get("prioridadReduccion")),
                "tieneBolsa": _bool(_get("tieneBolsa")),
                "turnoPreferido": _str(_get("turnoPreferido")),
                "activa": _bool(_get("activa", "SI")),
                "notas": _str(_get("notas")),
                "ciudades": ciudades,
                # None = el motor calcula; número = fuerza esa frecuencia
                "frecuenciaSolicitada": _int(_get("frecuenciaSolicitada")) if _get("frecuenciaSolicitada") not in (None, "", 0) else None,
            }
        )

    if not empresas_data:
        raise HTTPException(400, "No se encontraron empresas en el Excel")

    # ── Cargar empresas existentes (CASE-INSENSITIVE) ────────
    existing_rows = await db.execute(text("SELECT id, nombre FROM empresa"))
    db_lookup: dict[str, tuple[int, str]] = {}
    for r in existing_rows.mappings().all():
        db_lookup[r["nombre"].strip().upper()] = (r["id"], r["nombre"])

    # ── Cargar ciudades existentes (CASE-INSENSITIVE) ────────
    existing_cities = await db.execute(text("SELECT id, nombre FROM ciudad"))
    city_lookup: dict[str, int] = {}
    for r in existing_cities.mappings().all():
        city_lookup[r["nombre"].strip().upper()] = r["id"]

    # ── 1. Crear ciudades nuevas ─────────────────────────────
    all_cities = set()
    for emp in empresas_data:
        all_cities.update(c.upper() for c in emp["ciudades"])

    ciudades_creadas = []
    for cn in sorted(all_cities):
        if cn not in city_lookup:
            r = await db.execute(
                text(
                    "INSERT INTO ciudad (nombre, activa) VALUES (:n, true) RETURNING id"
                ),
                {"n": cn},
            )
            city_lookup[cn] = r.scalar_one()
            ciudades_creadas.append(cn)

    # ── 2. Upsert empresas ───────────────────────────────────
    creadas = 0
    actualizadas = 0
    eid_map: dict[str, int] = {}

    for emp in empresas_data:
        key = emp["nombre"].upper()
        existing = db_lookup.get(key)

        if existing:
            eid, old_name = existing
            await db.execute(
                text("""
                    UPDATE empresa SET
                        nombre = :nombre, tipo = :tipo, semaforo = :semaforo,
                        "scoreV3" = :scoreV3, "fiabilidadReciente" = :fiabilidadReciente,
                        "esComodin" = :esComodin, "aceptaExtras" = :aceptaExtras,
                        "maxExtrasTrimestre" = :maxExtrasTrimestre,
                        "prioridadReduccion" = :prioridadReduccion,
                        "tieneBolsa" = :tieneBolsa, "turnoPreferido" = :turnoPreferido,
                        activa = :activa, notas = :notas, "updatedAt" = NOW()
                    WHERE id = :id
                """),
                {**emp, "id": eid},
            )
            actualizadas += 1
            eid_map[key] = eid
            if old_name != emp["nombre"]:
                warnings.append(f"'{old_name}' → '{emp['nombre']}'")
        else:
            r = await db.execute(
                text("""
                    INSERT INTO empresa (
                        nombre, tipo, semaforo, "scoreV3", "fiabilidadReciente",
                        "esComodin", "aceptaExtras", "maxExtrasTrimestre",
                        "prioridadReduccion", "tieneBolsa", "turnoPreferido",
                        activa, notas, "updatedAt"
                    ) VALUES (
                        :nombre, :tipo, :semaforo, :scoreV3, :fiabilidadReciente,
                        :esComodin, :aceptaExtras, :maxExtrasTrimestre,
                        :prioridadReduccion, :tieneBolsa, :turnoPreferido,
                        :activa, :notas, NOW()
                    ) RETURNING id
                """),
                emp,
            )
            eid = r.scalar_one()
            creadas += 1
            eid_map[key] = eid
            db_lookup[key] = (eid, emp["nombre"])

    # ── 3. Sync empresaCiudad ────────────────────────────────
    ec_count = 0
    for emp in empresas_data:
        eid = eid_map[emp["nombre"].upper()]
        await db.execute(
            text('DELETE FROM "empresaCiudad" WHERE "empresaId" = :eid'),
            {"eid": eid},
        )
        for cn in emp["ciudades"]:
            cid = city_lookup.get(cn.upper())
            if cid:
                await db.execute(
                    text("""
                        INSERT INTO "empresaCiudad" ("empresaId", "ciudadId", "activaReciente")
                        VALUES (:eid, :cid, true)
                        ON CONFLICT ("empresaId", "ciudadId") DO UPDATE SET "activaReciente" = true
                    """),
                    {"eid": eid, "cid": cid},
                )
                ec_count += 1

    # ── 4. Upsert configTrimestral ───────────────────────────
    cfg_count = 0
    for emp in empresas_data:
        if not emp["activa"]:
            continue
        eid = eid_map[emp["nombre"].upper()]
        await db.execute(
            text("""
                INSERT INTO "configTrimestral" (
                    "empresaId", trimestre, "tipoParticipacion",
                    "escuelaPropia", "disponibilidadDias", "turnoPreferido",
                    "frecuenciaSolicitada", "updatedAt"
                ) VALUES (:eid, :tri, :tipo, false, 'L,M,X,J,V', :turno, :freq, NOW())
                ON CONFLICT ("empresaId", trimestre)
                DO UPDATE SET
                    "tipoParticipacion" = EXCLUDED."tipoParticipacion",
                    "turnoPreferido" = EXCLUDED."turnoPreferido",
                    "frecuenciaSolicitada" = EXCLUDED."frecuenciaSolicitada",
                    "updatedAt" = NOW()
            """),
            {
                "eid": eid,
                "tri": trimestre,
                "tipo": emp["tipo"],
                "turno": emp["turnoPreferido"],
                "freq": emp.get("frecuenciaSolicitada"),
            },
        )
        cfg_count += 1

    # ── 5. Semanas excluidas ─────────────────────────────────
    excl_count = 0
    excl_sheet = None
    for c in ["semanasexcluidas", "semanas_excluidas", "semanas excluidas"]:
        if c in sheet_map:
            excl_sheet = wb[sheet_map[c]]
            break

    if excl_sheet:
        # Table "semanaExcluida" is now managed by Prisma migrations
        for row in excl_sheet.iter_rows(
            min_row=2, max_row=excl_sheet.max_row, values_only=True
        ):
            tri = _str(row[0]) if len(row) > 0 else None
            sem = _int(row[1]) if len(row) > 1 else 0
            motivo = _str(row[2]) if len(row) > 2 else None
            if not tri or sem <= 0:
                continue
            await db.execute(
                text("""
                    INSERT INTO "semanaExcluida" (trimestre, semana, motivo)
                    VALUES (:tri, :sem, :motivo)
                    ON CONFLICT (trimestre, semana) DO UPDATE SET motivo = EXCLUDED.motivo
                """),
                {"tri": tri, "sem": sem, "motivo": motivo},
            )
            excl_count += 1

    await db.commit()

    return ImportEmpresasResult(
        total_empresas=len(empresas_data),
        creadas=creadas,
        actualizadas=actualizadas,
        ciudades_creadas=ciudades_creadas,
        empresa_ciudad_links=ec_count,
        config_trimestral_creadas=cfg_count,
        semanas_excluidas=excl_count,
        warnings=warnings,
    )


# ══════════════════════════════════════════════════════════════
# ENDPOINT 2: Importar histórico (unificado)
# ══════════════════════════════════════════════════════════════
 
 
@router.post("/historico", response_model=ImportHistoricoResult)
async def importar_historico(
    file: UploadFile = File(...),
    trimestre: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Importa un calendario completado como histórico del trimestre.
 
    Acepta el Excel exportado por /exportar-excel (formato estándar):
        Semana | Día | Horario | Turno | Empresa | Taller | Programa | Ciudad | Tipo | Estado | Confirmado
 
    También acepta formatos legacy con columnas:
        SEM | D | TALLER | EMPRESA | CIUDAD | FECHA | TIPO
 
    Lógica de Estado:
    - OK / PLANIFICADO / SÍ → se importa como 'OK'
    - CANCELADO / NO → se importa como 'CANCELADO'
    - VACANTE sin empresa → se ignora
    - VACANTE con empresa (rellenada manualmente) → se importa como 'OK'
 
    Idempotente: borra histórico anterior del mismo trimestre.
    """
    import openpyxl
    from datetime import date, timedelta, datetime
 
    content = await file.read()
    filename = (file.filename or "").lower()
 
    # Soportar CSV legacy también
    if filename.endswith(".csv"):
        import pandas as pd
        df = pd.read_csv(BytesIO(content))
        # Convertir a openpyxl workbook en memoria
        wb = openpyxl.Workbook()
        ws_tmp = wb.active
        ws_tmp.append(list(df.columns))
        for _, row in df.iterrows():
            ws_tmp.append(list(row))
    else:
        wb = openpyxl.load_workbook(BytesIO(content))
 
    ws = wb.active
    warnings: list[str] = []
 
    # ── Detectar columnas (flexible, soporta ambos formatos) ──
    raw_headers = [str(c.value or "").strip() for c in ws[1]]
    headers_lower = [h.lower().replace("í", "i").replace("á", "a") for h in raw_headers]
 
    COLUMN_ALIASES = {
        "semana": ["semana", "sem", "week"],
        "dia": ["dia", "día", "d", "day"],
        "horario": ["horario", "hora", "time"],
        "turno": ["turno", "shift"],
        "empresa": ["empresa", "company", "emp"],
        "taller": ["taller", "workshop", "tal"],
        "programa": ["programa", "prog", "tipo", "type"],
        "ciudad": ["ciudad", "city"],
        "estado": ["estado", "status"],
        "fecha": ["fecha", "date", "fecha_taller"],
    }
 
    col_idx: dict[str, int] = {}
    for field, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            for i, h in enumerate(headers_lower):
                if h == alias and field not in col_idx:
                    col_idx[field] = i
                    break
            if field in col_idx:
                break
 
    # Validar mínimo
    if "empresa" not in col_idx or "taller" not in col_idx:
        raise HTTPException(
            400,
            f"Columnas requeridas 'Empresa' y 'Taller' no encontradas. "
            f"Headers: {raw_headers}",
        )
 
    has_semana = "semana" in col_idx
    has_fecha = "fecha" in col_idx
    has_estado = "estado" in col_idx
 
    if not has_semana and not has_fecha:
        warnings.append(
            "No se encontró columna 'Semana' ni 'Fecha'. "
            "Se usará fecha genérica para todas las filas."
        )
 
    # ── Lookups de BD ────────────────────────────────────────
    emp_rows = await db.execute(text("SELECT id, nombre FROM empresa"))
    emp_map = {
        r["nombre"].strip().upper(): r["id"]
        for r in emp_rows.mappings().all()
    }
 
    taller_rows = await db.execute(
        text("SELECT id, nombre FROM taller WHERE activo = true")
    )
    talleres = [dict(r) for r in taller_rows.mappings().all()]
 
    def match_taller(n: str) -> int | None:
        nl = n.strip().lower()
        for t in talleres:
            if t["nombre"].strip().lower() == nl:
                return t["id"]
        for t in talleres:
            if nl in t["nombre"].strip().lower() or t["nombre"].strip().lower() in nl:
                return t["id"]
        return None
 
    # ── Función para calcular fecha real ─────────────────────
    def fecha_from_semana(sem_rel: int, dia: str) -> date:
        year = int(trimestre.split("-")[0])
        q = trimestre.split("-")[1]
        q_start_month = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}
        month = q_start_month.get(q, 1)
        first_day = date(year, month, 1)
        days_to_monday = (7 - first_day.weekday()) % 7
        first_monday = first_day + timedelta(days=days_to_monday)
        if first_day.weekday() == 0:
            first_monday = first_day
        target_monday = first_monday + timedelta(weeks=sem_rel - 1)
        DIA_OFFSET = {"L": 0, "M": 1, "X": 2, "J": 3, "V": 4}
        return target_monday + timedelta(days=DIA_OFFSET.get(dia, 0))
 
    def fecha_fallback() -> date:
        """Fecha genérica si no hay semana ni fecha."""
        year = int(trimestre.split("-")[0])
        q = trimestre.split("-")[1]
        q_start_month = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}
        return date(year, q_start_month.get(q, 1), 1)
 
    # Normalización legacy (del importar.py original)
    NORMALIZE_EMPRESA = {
        "ACCIONA ": "ACCIONA",
        "AECOM ": "AECOM",
        "ATREVIA ": "ATREVIA",
        " INDRA": "INDRA",
        "MP CAPGEMINI": "CAPGEMINI",
        "MP ENDESA": "ENDESA",
        "MP LDA": "LDA",
        "MP AON": "F. AON",
        "MP SANTANDER": "BANCO SANTANDER",
        "SERVITEC / CAPGEMINI": "SERVITEC",
        "SERVITEC / SERVITEC": "SERVITEC",
        "URBASER / SACYR": "URBASER",
    }
 
    # ── Borrar histórico anterior ────────────────────────────
    await db.execute(
        text('DELETE FROM "historicoTaller" WHERE trimestre = :tri'),
        {"tri": trimestre},
    )
 
    # ── Procesar filas ───────────────────────────────────────
    emp_404: set[str] = set()
    taller_404: set[str] = set()
    importadas_ok = 0
    importadas_cancelado = 0
    vacantes_ignoradas = 0
    total_filas = 0
 
    def _cell(row_vals, field: str, default=None):
        idx = col_idx.get(field)
        if idx is None or idx >= len(row_vals):
            return default
        return row_vals[idx]
 
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, values_only=True):
        total_filas += 1
 
        empresa_raw = _cell(row, "empresa")
        taller_raw = _cell(row, "taller")
 
        # Ignorar filas sin empresa ni taller
        if not empresa_raw or str(empresa_raw).strip() == "":
            vacantes_ignoradas += 1
            continue
        if not taller_raw or str(taller_raw).strip() == "":
            continue
 
        # Estado
        if has_estado:
            estado_raw = str(_cell(row, "estado", "OK")).strip().upper()
        else:
            estado_raw = "OK"  # Legacy sin columna estado → todo OK
 
        if estado_raw in ("VACANTE",) and str(empresa_raw).strip() == "":
            vacantes_ignoradas += 1
            continue
 
        # Normalizar estado
        if estado_raw in ("OK", "PLANIFICADO", "SI", "SÍ", "YES", "1", "VACANTE"):
            # VACANTE con empresa → fue rellenada manualmente → OK
            estado_db = "OK"
        elif estado_raw in ("CANCELADO", "NO", "CANCEL"):
            estado_db = "CANCELADO"
        else:
            estado_db = "OK"
 
        # Resolver empresa
        empresa_nombre = str(empresa_raw).strip()
        empresa_upper = empresa_nombre.upper()
        # Intentar normalización legacy
        empresa_upper = NORMALIZE_EMPRESA.get(empresa_nombre, empresa_upper)
        eid = emp_map.get(empresa_upper)
        if not eid:
            emp_404.add(empresa_upper)
            continue
 
        # Resolver taller
        tid = match_taller(str(taller_raw).strip())
        if not tid:
            taller_404.add(str(taller_raw).strip())
            continue
 
        # Resolver fecha
        fecha = None
        if has_fecha:
            fecha_raw = _cell(row, "fecha")
            if isinstance(fecha_raw, datetime):
                fecha = fecha_raw.date()
            elif isinstance(fecha_raw, date):
                fecha = fecha_raw
 
        if fecha is None and has_semana:
            try:
                sem = int(float(_cell(row, "semana", 1)))
                dia = str(_cell(row, "dia", "L")).strip().upper()
                fecha = fecha_from_semana(sem, dia)
            except Exception:
                fecha = fecha_fallback()
 
        if fecha is None:
            fecha = fecha_fallback()
 
        # Ciudad
        ciudad = str(_cell(row, "ciudad", "MADRID")).strip()
        if not ciudad:
            ciudad = "MADRID"
 
        # Insertar
        await db.execute(
            text("""
                INSERT INTO "historicoTaller"
                    ("empresaId", "tallerId", fecha, estado, ciudad, trimestre)
                VALUES (:eid, :tid, :fecha, :estado, :ciudad, :tri)
            """),
            {
                "eid": eid,
                "tid": tid,
                "fecha": fecha,
                "estado": estado_db,
                "ciudad": ciudad,
                "tri": trimestre,
            },
        )
 
        if estado_db == "OK":
            importadas_ok += 1
        else:
            importadas_cancelado += 1
 
    await db.commit()
 
    if emp_404:
        warnings.append(f"Empresas no encontradas: {sorted(emp_404)}")
    if taller_404:
        warnings.append(f"Talleres no encontrados: {sorted(taller_404)}")
 
    return ImportHistoricoResult(
        trimestre=trimestre,
        total_filas=total_filas,
        importadas_ok=importadas_ok,
        importadas_cancelado=importadas_cancelado,
        vacantes_ignoradas=vacantes_ignoradas,
        empresas_no_encontradas=sorted(emp_404),
        talleres_no_encontrados=sorted(taller_404),
        warnings=warnings,
    )


# ══════════════════════════════════════════════════════════════
# ENDPOINT 3: Estado
# ══════════════════════════════════════════════════════════════


@router.get("/estado/{trimestre}")
async def estado_importacion(trimestre: str, db: AsyncSession = Depends(get_db)):
    counts = {}
    for key, q in [
        ("empresas_activas", "SELECT COUNT(*) FROM empresa WHERE activa = true"),
        (
            "config_trimestral",
            'SELECT COUNT(*) FROM "configTrimestral" WHERE trimestre = :tri',
        ),
        ("frecuencias", "SELECT COUNT(*) FROM frecuencia WHERE trimestre = :tri"),
        ("historico", 'SELECT COUNT(*) FROM "historicoTaller" WHERE trimestre = :tri'),
        ("restricciones", "SELECT COUNT(*) FROM restriccion"),
        ("ciudades", "SELECT COUNT(*) FROM ciudad"),
        ("empresa_ciudad", 'SELECT COUNT(*) FROM "empresaCiudad"'),
    ]:
        r = await db.execute(text(q), {"tri": trimestre} if ":tri" in q else {})
        counts[key] = r.scalar()

    try:
        r = await db.execute(
            text('SELECT COUNT(*) FROM "semanaExcluida" WHERE trimestre = :tri'),
            {"tri": trimestre},
        )
        counts["semanas_excluidas"] = r.scalar()
    except:  # noqa: E722
        counts["semanas_excluidas"] = 0

    return {
        "trimestre": trimestre,
        **counts,
        "listo_fase_1": counts["config_trimestral"] > 0,
        "listo_fase_2": counts["frecuencias"] > 0,
    }


# ══════════════════════════════════════════════════════════════
# ENDPOINT 4: Clonar trimestre
# ══════════════════════════════════════════════════════════════

class ClonarTrimestreResult(BaseModel):
    trimestre_origen: str
    trimestre_destino: str
    configs_clonadas: int
    configs_ya_existentes: int
    empresas_saltadas: list[str]   # inactivas, no se clonan
    warnings: list[str]


@router.post("/clonar-trimestre", response_model=ClonarTrimestreResult)
async def clonar_trimestre(
    trimestre_origen: str = Form(...),   # "2026-Q2"
    trimestre_destino: str = Form(...),  # "2026-Q3"
    db: AsyncSession = Depends(get_db),
):
    """
    Clona las configTrimestral activas de un trimestre al siguiente.

    Copia: tipoParticipacion, turnoPreferido, frecuenciaSolicitada,
           disponibilidadDias.
    NO copia: frecuencias calculadas, planificaciones, semanas excluidas
              (las semanas excluidas ya deben estar cargadas para todo el año).

    Comportamiento:
    - Si ya existe config para (empresa, destino) → NO sobreescribe (ON CONFLICT DO NOTHING)
    - Empresas inactivas (activa=false) → se omiten y se listan en empresas_saltadas
    """
    warnings: list[str] = []

    # ── Leer configs del trimestre origen ───────────────────
    rows = await db.execute(
        text("""
            SELECT
                ct."empresaId",
                e.nombre,
                e.activa,
                ct."tipoParticipacion",
                ct."turnoPreferido",
                ct."frecuenciaSolicitada",
                ct."disponibilidadDias"
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct.trimestre = :origen
            ORDER BY e.nombre
        """),
        {"origen": trimestre_origen},
    )
    configs = [dict(r) for r in rows.mappings().all()]

    if not configs:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=404,
            detail=f"No hay configuraciones para el trimestre {trimestre_origen}. "
                   "Importa primero ese trimestre.",
        )

    clonadas = 0
    ya_existentes = 0
    saltadas: list[str] = []

    for cfg in configs:
        # Saltar empresas dadas de baja definitivamente
        if not cfg["activa"]:
            saltadas.append(cfg["nombre"])
            continue

        result = await db.execute(
            text("""
                INSERT INTO "configTrimestral" (
                    "empresaId", trimestre, "tipoParticipacion",
                    "escuelaPropia", "disponibilidadDias",
                    "turnoPreferido", "frecuenciaSolicitada", "updatedAt"
                )
                VALUES (
                    :eid, :destino, :tipo,
                    false, :dias,
                    :turno, :freq, NOW()
                )
                ON CONFLICT ("empresaId", trimestre) DO NOTHING
            """),
            {
                "eid": cfg["empresaId"],
                "destino": trimestre_destino,
                "tipo": cfg["tipoParticipacion"],
                "dias": cfg["disponibilidadDias"] or "L,M,X,J,V",
                "turno": cfg["turnoPreferido"],
                "freq": cfg["frecuenciaSolicitada"],
            },
        )
        # rowcount = 0 → ya existía (DO NOTHING), 1 → insertado
        if result.rowcount == 0:
            ya_existentes += 1
        else:
            clonadas += 1

    await db.commit()

    if saltadas:
        warnings.append(
            f"{len(saltadas)} empresa(s) inactiva(s) no clonadas: {', '.join(saltadas)}"
        )
    if ya_existentes > 0:
        warnings.append(
            f"{ya_existentes} config(s) ya existían en {trimestre_destino} y no se modificaron."
        )

    return ClonarTrimestreResult(
        trimestre_origen=trimestre_origen,
        trimestre_destino=trimestre_destino,
        configs_clonadas=clonadas,
        configs_ya_existentes=ya_existentes,
        empresas_saltadas=saltadas,
        warnings=warnings,
    )
    
