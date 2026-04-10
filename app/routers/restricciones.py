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
  solo_dia     → Empresa solo disponible ese día (L/M/X/J/V)
  solo_taller  → Empresa solo imparte ese taller (nombre del taller)
  no_comodin   → No usar como comodín en contingencias
  max_extras   → Máximo de extras por trimestre (valor numérico)
"""

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
    descripcion: str | None = None


class RestriccionOut(BaseModel):
    id: int
    empresa_id: int
    empresa_nombre: str
    tipo: str
    clave: str
    valor: str
    descripcion: str | None


# ── Helpers ──────────────────────────────────────────────────

CLAVES_VALIDAS = {"solo_dia", "solo_taller", "no_comodin", "max_extras"}
TIPOS_VALIDOS  = {"HARD", "SOFT"}
DIAS_VALIDOS   = {"L", "M", "X", "J", "V"}

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


# ── Endpoints ────────────────────────────────────────────────

@router.get("", response_model=list[RestriccionOut])
async def listar_todas(
    empresa_id: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Lista todas las restricciones, opcionalmente filtradas por empresa."""
    q = """
        SELECT r.id, r."empresaId" AS empresa_id, e.nombre AS empresa_nombre,
               r.tipo, r.clave, r.valor, r.descripcion
        FROM restriccion r
        JOIN empresa e ON e.id = r."empresaId"
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
                   r.tipo, r.clave, r.valor, r.descripcion
            FROM restriccion r
            JOIN empresa e ON e.id = r."empresaId"
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
                # Actualizar tipo y descripcion si cambian
                await db.execute(
                    text("""
                        UPDATE restriccion
                        SET tipo = :tipo, descripcion = :desc
                        WHERE id = :id
                    """),
                    {"tipo": tipo, "desc": descripcion or None, "id": existing["id"]},
                )
                actualizadas += 1
            else:
                await db.execute(
                    text("""
                        INSERT INTO restriccion ("empresaId", tipo, clave, valor, descripcion)
                        VALUES (:eid, :tipo, :clave, :valor, :desc)
                    """),
                    {
                        "eid": empresa_id,
                        "tipo": tipo,
                        "clave": clave,
                        "valor": valor,
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

    dup = await db.execute(
        text("""
            SELECT id FROM restriccion
            WHERE "empresaId" = :eid AND clave = :clave AND valor = :valor
        """),
        {"eid": empresa_id, "clave": data.clave, "valor": data.valor},
    )
    if dup.mappings().first():
        raise HTTPException(
            409,
            f"Ya existe una restricción '{data.clave}={data.valor}' para esta empresa"
        )

    result = await db.execute(
        text("""
            INSERT INTO restriccion ("empresaId", tipo, clave, valor, descripcion)
            VALUES (:eid, :tipo, :clave, :valor, :desc)
            RETURNING id
        """),
        {
            "eid": empresa_id,
            "tipo": data.tipo,
            "clave": data.clave,
            "valor": data.valor,
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
        "valor": data.valor,
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

    await db.execute(
        text("""
            UPDATE restriccion
            SET tipo = :tipo, clave = :clave, valor = :valor, descripcion = :desc
            WHERE id = :id
        """),
        {
            "tipo": data.tipo,
            "clave": data.clave,
            "valor": data.valor,
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
        "valor": data.valor,
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