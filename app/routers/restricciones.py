"""
CRUD de Restricciones por empresa + Importación desde Excel.

Endpoints:
  GET    /api/restricciones                → listar todas (con filtro opcional por empresa)
  GET    /api/restricciones/{empresa_id}   → restricciones de una empresa
  POST   /api/restricciones/{empresa_id}   → crear restricción
  PUT    /api/restricciones/{id}           → editar restricción
  DELETE /api/restricciones/{id}           → borrar restricción
  POST   /api/restricciones/importar       → importar desde Excel

Claves soportadas (clave → descripción para la UI):
  solo_dia        → Empresa solo disponible ese día (L/M/X/J/V)
  solo_taller     → Empresa solo imparte ese taller (FK a taller via tallerId)
  no_comodin      → No usar como comodín en contingencias
  max_extras      → Máximo de extras por trimestre (valor numérico)
  franja_horaria  → Empresa solo imparte en una franja canónica concreta
                    (V16: '09:30-11:30' | '12:00-14:00' | '15:00-17:00')
  franja_por_dia  → Empresa solo imparte en cierta franja un día concreto
                    (V16: 'D:HH:MM-HH:MM', máx una por (empresa, día))
"""

import re

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class RestriccionIn(BaseModel):
    tipo: str        # "HARD" | "SOFT"
    clave: str       # "solo_dia" | "solo_taller" | "no_comodin" | "max_extras"
    valor: str       # "V" | "Gestión de ingresos" | "true" | "1"
    taller_id: int | None = None  # FK to taller, required for solo_taller
    descripcion: str | None = None


class RestriccionOut(BaseModel):
    id: int
    empresa_id: int
    empresa_nombre: str
    tipo: str
    clave: str
    valor: str
    taller_id: int | None = None
    taller_nombre_ref: str | None = None
    descripcion: str | None


# ── Helpers ──────────────────────────────────────────────────

CLAVES_VALIDAS = {
    "solo_dia",
    "solo_taller",
    "no_comodin",
    "max_extras",
    "franja_horaria",   # V16
    "franja_por_dia",   # V16
}
TIPOS_VALIDOS  = {"HARD", "SOFT"}
DIAS_VALIDOS   = frozenset({"L", "M", "X", "J", "V"})
FRANJAS_CANONICAS = frozenset({"09:30-11:30", "12:00-14:00", "15:00-17:00"})

# Strict regexes for V16 franja keys
_RE_FRANJA_HORARIA = re.compile(r"^(09:30-11:30|12:00-14:00|15:00-17:00)$")
_RE_FRANJA_POR_DIA = re.compile(r"^[LMXJV]:(09:30-11:30|12:00-14:00|15:00-17:00)$")


def _validar(data: RestriccionIn):
    if data.tipo not in TIPOS_VALIDOS:
        raise HTTPException(400, f"tipo debe ser HARD o SOFT, recibido: {data.tipo}")
    if data.clave not in CLAVES_VALIDAS:
        raise HTTPException(
            400,
            f"clave '{data.clave}' no reconocida. Válidas: {sorted(CLAVES_VALIDAS)}"
        )
    if data.clave == "solo_dia" and data.valor.upper() not in DIAS_VALIDOS:
        raise HTTPException(
            400,
            f"Para solo_dia el valor debe ser L/M/X/J/V, recibido: {data.valor}"
        )
    if data.clave == "max_extras":
        try:
            int(data.valor)
        except ValueError:
            raise HTTPException(400, "Para max_extras el valor debe ser un número entero")
    if data.clave == "solo_taller" and data.taller_id is None:
        raise HTTPException(
            400,
            "Para solo_taller es obligatorio `taller_id` (FK a taller del catálogo)"
        )
    if data.clave == "franja_horaria" and not _RE_FRANJA_HORARIA.match(data.valor or ""):
        raise HTTPException(
            400,
            "franja_horaria requiere valor exacto entre "
            "'09:30-11:30', '12:00-14:00', '15:00-17:00'",
        )
    if data.clave == "franja_por_dia" and not _RE_FRANJA_POR_DIA.match(data.valor or ""):
        raise HTTPException(
            400,
            "franja_por_dia requiere formato 'D:HH:MM-HH:MM' con "
            "D ∈ {L,M,X,J,V} y franja canónica",
        )


async def _verificar_taller(db: AsyncSession, taller_id: int) -> str:
    """Verify taller exists and is active. Returns taller name for valor sync."""
    result = await db.execute(
        text("SELECT nombre, activo FROM taller WHERE id = :id"),
        {"id": taller_id},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(400, f"Taller {taller_id} no encontrado en el catálogo")
    if not row["activo"]:
        raise HTTPException(400, f"Taller {taller_id} ('{row['nombre']}') no está activo")
    return row["nombre"]


# ── Endpoints ────────────────────────────────────────────────

@router.get("", response_model=list[RestriccionOut])
async def listar_todas(
    empresa_id: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Lista todas las restricciones, opcionalmente filtradas por empresa."""
    q = """
        SELECT r.id, r."empresaId" AS empresa_id, e.nombre AS empresa_nombre,
               r.tipo, r.clave, r.valor, r."tallerId" AS taller_id,
               tl.nombre AS taller_nombre_ref, r.descripcion
        FROM restriccion r
        JOIN empresa e ON e.id = r."empresaId"
        LEFT JOIN taller tl ON tl.id = r."tallerId"
    """
    params: dict = {}
    if empresa_id is not None:
        q += ' WHERE r."empresaId" = :eid'
        params["eid"] = empresa_id
    q += " ORDER BY e.nombre, r.tipo, r.clave"

    result = await db.execute(text(q), params)
    return [dict(r) for r in result.mappings().all()]


@router.get("/{empresa_id}", response_model=list[RestriccionOut])
async def listar_por_empresa(
    empresa_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Lista las restricciones de una empresa concreta."""
    emp = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE id = :id"),
        {"id": empresa_id},
    )
    if not emp.mappings().first():
        raise HTTPException(404, f"Empresa {empresa_id} no encontrada")

    result = await db.execute(
        text("""
            SELECT r.id, r."empresaId" AS empresa_id, e.nombre AS empresa_nombre,
                   r.tipo, r.clave, r.valor, r."tallerId" AS taller_id,
                   tl.nombre AS taller_nombre_ref, r.descripcion
            FROM restriccion r
            JOIN empresa e ON e.id = r."empresaId"
            LEFT JOIN taller tl ON tl.id = r."tallerId"
            WHERE r."empresaId" = :eid
            ORDER BY r.tipo, r.clave
        """),
        {"eid": empresa_id},
    )
    return [dict(r) for r in result.mappings().all()]


@router.post("/importar")
async def importar_restricciones(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Importa restricciones desde un archivo Excel.

    Columnas esperadas:
      nombre       → nombre de la empresa (debe existir en BD)
      tipo         → HARD | SOFT
      clave        → solo_dia | solo_taller | no_comodin | max_extras
      valor        → V, nombre del taller, true, 1, etc.
      trimestre    → (opcional) para contexto, no se usa en la restricción
      descripcion  → (opcional) nota interna

    Para `solo_taller` se intenta resolver `valor` (nombre del taller) a
    `tallerId` consultando el catálogo. Si no se encuentra coincidencia,
    se inserta la fila con `tallerId=NULL` y se añade un warning.
    """
    import openpyxl
    from io import BytesIO

    content = await file.read()
    wb = openpyxl.load_workbook(BytesIO(content), read_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(min_row=1, values_only=True))
    if not rows:
        raise HTTPException(400, "El archivo está vacío")

    # Detectar headers
    headers = [str(h).strip().lower() if h else "" for h in rows[0]]
    required = {"nombre", "tipo", "clave", "valor"}
    missing = required - set(headers)
    if missing:
        raise HTTPException(
            400,
            f"Faltan columnas obligatorias: {sorted(missing)}. "
            f"Columnas encontradas: {headers}"
        )

    col = {h: i for i, h in enumerate(headers)}

    # Cargar empresas de BD para mapear nombre → id
    emp_result = await db.execute(text("SELECT id, nombre FROM empresa"))
    empresa_map = {}
    for r in emp_result.mappings().all():
        # Normalizar: trim, uppercase para matching flexible
        empresa_map[r["nombre"].strip().upper()] = r["id"]

    # Cargar talleres activos para resolver solo_taller → tallerId
    taller_result = await db.execute(
        text("SELECT id, nombre FROM taller WHERE activo = true")
    )
    taller_map = {
        r["nombre"].strip().lower(): r["id"]
        for r in taller_result.mappings().all()
    }

    creadas = 0
    actualizadas = 0
    errores: list[str] = []

    for i, row in enumerate(rows[1:], start=2):
        try:
            nombre = str(row[col["nombre"]] or "").strip()
            tipo = str(row[col["tipo"]] or "").strip().upper()
            clave = str(row[col["clave"]] or "").strip().lower()
            valor = str(row[col["valor"]] or "").strip()
            descripcion = str(row[col.get("descripcion", -1)] or "").strip() if "descripcion" in col else None

            if not nombre or not tipo or not clave or not valor:
                errores.append(f"Fila {i}: campos vacíos (nombre={nombre}, tipo={tipo}, clave={clave}, valor={valor})")
                continue

            # Buscar empresa
            empresa_id = empresa_map.get(nombre.upper())
            if not empresa_id:
                # Buscar parcial
                matches = [
                    eid for ename, eid in empresa_map.items()
                    if nombre.upper() in ename or ename in nombre.upper()
                ]
                if len(matches) == 1:
                    empresa_id = matches[0]
                else:
                    errores.append(f"Fila {i}: empresa '{nombre}' no encontrada")
                    continue

            # Validar
            if tipo not in TIPOS_VALIDOS:
                errores.append(f"Fila {i}: tipo '{tipo}' inválido (HARD/SOFT)")
                continue
            if clave not in CLAVES_VALIDAS:
                errores.append(f"Fila {i}: clave '{clave}' inválida")
                continue
            if clave == "solo_dia" and valor.upper() not in DIAS_VALIDOS:
                errores.append(f"Fila {i}: día '{valor}' inválido para solo_dia")
                continue
            # V16: franja keys with strict regex (single source of truth = _validar)
            if clave == "franja_horaria" and not _RE_FRANJA_HORARIA.match(valor):
                errores.append(
                    f"Fila {i}: franja_horaria '{valor}' no es canónica. "
                    f"Usar 09:30-11:30 | 12:00-14:00 | 15:00-17:00"
                )
                continue
            if clave == "franja_por_dia" and not _RE_FRANJA_POR_DIA.match(valor):
                errores.append(
                    f"Fila {i}: franja_por_dia '{valor}' inválida. "
                    f"Formato esperado 'D:HH:MM-HH:MM' con D ∈ L,M,X,J,V y franja canónica"
                )
                continue

            # Resolve solo_taller → tallerId (exact, case-insensitive)
            taller_id = None
            if clave == "solo_taller":
                taller_id = taller_map.get(valor.strip().lower())
                if taller_id is None:
                    errores.append(
                        f"Fila {i}: taller '{valor}' no encontrado en catálogo — "
                        f"tallerId queda NULL. Edite manualmente desde la UI."
                    )

            # Check duplicado
            dup = await db.execute(
                text("""
                    SELECT id FROM restriccion
                    WHERE "empresaId" = :eid AND clave = :clave AND valor = :valor
                """),
                {"eid": empresa_id, "clave": clave, "valor": valor},
            )
            existing = dup.mappings().first()

            if existing:
                # Actualizar tipo, descripcion y tallerId si cambian
                await db.execute(
                    text("""
                        UPDATE restriccion
                        SET tipo = :tipo, descripcion = :desc, "tallerId" = :tid
                        WHERE id = :id
                    """),
                    {
                        "tipo": tipo,
                        "desc": descripcion or None,
                        "tid": taller_id,
                        "id": existing["id"],
                    },
                )
                actualizadas += 1
            else:
                await db.execute(
                    text("""
                        INSERT INTO restriccion ("empresaId", tipo, clave, valor, "tallerId", descripcion)
                        VALUES (:eid, :tipo, :clave, :valor, :tid, :desc)
                    """),
                    {
                        "eid": empresa_id,
                        "tipo": tipo,
                        "clave": clave,
                        "valor": valor,
                        "tid": taller_id,
                        "desc": descripcion or None,
                    },
                )
                creadas += 1

        except Exception as ex:
            errores.append(f"Fila {i}: {str(ex)}")

    await db.commit()

    return {
        "creadas": creadas,
        "actualizadas": actualizadas,
        "total_filas": len(rows) - 1,
        "errores": errores,
    }


@router.post("/{empresa_id}", response_model=RestriccionOut, status_code=201)
async def crear_restriccion(
    empresa_id: int,
    data: RestriccionIn,
    db: AsyncSession = Depends(get_db),
):
    """Crea una nueva restricción para una empresa."""
    _validar(data)

    emp = await db.execute(
        text("SELECT id, nombre FROM empresa WHERE id = :id"),
        {"id": empresa_id},
    )
    emp_row = emp.mappings().first()
    if not emp_row:
        raise HTTPException(404, f"Empresa {empresa_id} no encontrada")

    # For solo_taller, validate taller exists and sync valor with the taller's nombre
    taller_nombre_ref: str | None = None
    valor_final = data.valor
    if data.clave == "solo_taller":
        taller_nombre_ref = await _verificar_taller(db, data.taller_id)  # type: ignore[arg-type]
        # Keep valor in sync with the canonical taller name (helps backward compat)
        valor_final = taller_nombre_ref

    # V16 uniqueness for franja_horaria: at most one per empresa
    if data.clave == "franja_horaria":
        existing = await db.execute(
            text("""
                SELECT id, valor FROM restriccion
                WHERE "empresaId" = :eid AND clave = 'franja_horaria'
            """),
            {"eid": empresa_id},
        )
        row = existing.mappings().first()
        if row:
            raise HTTPException(
                409,
                f"La empresa ya tiene una franja_horaria activa "
                f"(id={row['id']}, valor='{row['valor']}'). Edita o elimina la existente.",
            )

    # V16 uniqueness for franja_por_dia: at most one per (empresa, día)
    if data.clave == "franja_por_dia":
        dia_letter = (data.valor or "").split(":", 1)[0]
        existing = await db.execute(
            text("""
                SELECT id, valor FROM restriccion
                WHERE "empresaId" = :eid AND clave = 'franja_por_dia'
                  AND valor LIKE :dia_prefix
            """),
            {"eid": empresa_id, "dia_prefix": f"{dia_letter}:%"},
        )
        row = existing.mappings().first()
        if row:
            raise HTTPException(
                409,
                f"La empresa ya tiene una franja_por_dia para el día {dia_letter} "
                f"(id={row['id']}, valor='{row['valor']}'). Edita o elimina la existente.",
            )

    dup = await db.execute(
        text("""
            SELECT id FROM restriccion
            WHERE "empresaId" = :eid AND clave = :clave AND valor = :valor
        """),
        {"eid": empresa_id, "clave": data.clave, "valor": valor_final},
    )
    if dup.mappings().first():
        raise HTTPException(
            409,
            f"Ya existe una restricción '{data.clave}={valor_final}' para esta empresa"
        )

    result = await db.execute(
        text("""
            INSERT INTO restriccion ("empresaId", tipo, clave, valor, "tallerId", descripcion)
            VALUES (:eid, :tipo, :clave, :valor, :tid, :desc)
            RETURNING id
        """),
        {
            "eid": empresa_id,
            "tipo": data.tipo,
            "clave": data.clave,
            "valor": valor_final,
            "tid": data.taller_id,
            "desc": data.descripcion,
        },
    )
    new_id = result.scalar()
    await db.commit()

    return {
        "id": new_id,
        "empresa_id": empresa_id,
        "empresa_nombre": emp_row["nombre"],
        "tipo": data.tipo,
        "clave": data.clave,
        "valor": valor_final,
        "taller_id": data.taller_id,
        "taller_nombre_ref": taller_nombre_ref,
        "descripcion": data.descripcion,
    }


@router.put("/{restriccion_id}", response_model=RestriccionOut)
async def editar_restriccion(
    restriccion_id: int,
    data: RestriccionIn,
    db: AsyncSession = Depends(get_db),
):
    """Edita una restricción existente."""
    _validar(data)

    existing = await db.execute(
        text("""
            SELECT r.id, r."empresaId", e.nombre AS empresa_nombre
            FROM restriccion r
            JOIN empresa e ON e.id = r."empresaId"
            WHERE r.id = :id
        """),
        {"id": restriccion_id},
    )
    row = existing.mappings().first()
    if not row:
        raise HTTPException(404, f"Restricción {restriccion_id} no encontrada")

    # For solo_taller, validate taller exists and sync valor
    taller_nombre_ref: str | None = None
    valor_final = data.valor
    if data.clave == "solo_taller":
        taller_nombre_ref = await _verificar_taller(db, data.taller_id)  # type: ignore[arg-type]
        valor_final = taller_nombre_ref

    # V16 uniqueness for franja_horaria (excluding self)
    if data.clave == "franja_horaria":
        existing = await db.execute(
            text("""
                SELECT id, valor FROM restriccion
                WHERE "empresaId" = :eid AND clave = 'franja_horaria' AND id != :rid
            """),
            {"eid": row["empresaId"], "rid": restriccion_id},
        )
        clash = existing.mappings().first()
        if clash:
            raise HTTPException(
                409,
                f"La empresa ya tiene otra franja_horaria activa "
                f"(id={clash['id']}, valor='{clash['valor']}').",
            )

    # V16 uniqueness for franja_por_dia (excluding self)
    if data.clave == "franja_por_dia":
        dia_letter = (data.valor or "").split(":", 1)[0]
        existing = await db.execute(
            text("""
                SELECT id, valor FROM restriccion
                WHERE "empresaId" = :eid AND clave = 'franja_por_dia'
                  AND valor LIKE :dia_prefix AND id != :rid
            """),
            {
                "eid": row["empresaId"],
                "dia_prefix": f"{dia_letter}:%",
                "rid": restriccion_id,
            },
        )
        clash = existing.mappings().first()
        if clash:
            raise HTTPException(
                409,
                f"La empresa ya tiene otra franja_por_dia para el día {dia_letter} "
                f"(id={clash['id']}, valor='{clash['valor']}').",
            )

    await db.execute(
        text("""
            UPDATE restriccion
            SET tipo = :tipo, clave = :clave, valor = :valor,
                "tallerId" = :tid, descripcion = :desc
            WHERE id = :id
        """),
        {
            "tipo": data.tipo,
            "clave": data.clave,
            "valor": valor_final,
            "tid": data.taller_id,
            "desc": data.descripcion,
            "id": restriccion_id,
        },
    )
    await db.commit()

    return {
        "id": restriccion_id,
        "empresa_id": row["empresaId"],
        "empresa_nombre": row["empresa_nombre"],
        "tipo": data.tipo,
        "clave": data.clave,
        "valor": valor_final,
        "taller_id": data.taller_id,
        "taller_nombre_ref": taller_nombre_ref,
        "descripcion": data.descripcion,
    }


@router.delete("/{restriccion_id}", status_code=204)
async def borrar_restriccion(
    restriccion_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Elimina una restricción."""
    existing = await db.execute(
        text("SELECT id FROM restriccion WHERE id = :id"),
        {"id": restriccion_id},
    )
    if not existing.mappings().first():
        raise HTTPException(404, f"Restricción {restriccion_id} no encontrada")

    await db.execute(
        text("DELETE FROM restriccion WHERE id = :id"),
        {"id": restriccion_id},
    )
    await db.commit()
