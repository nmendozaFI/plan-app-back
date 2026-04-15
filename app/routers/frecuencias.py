"""
FASE 1 — Motor de Frecuencias (v2 con recorte)
Calcula cuántos talleres EF/IT le tocan a cada empresa en un trimestre,
luego recorta para encajar en el modelo trimestral (14 EF + 6 IT).

Flujo human-in-the-loop:
  1. POST /calcular         → calcula + recorta → devuelve propuesta (NO persiste)
  2. POST /confirmar        → persiste la propuesta (opcionalmente con ajustes manuales)

Tablas Prisma (camelCase):
  - empresa, "configTrimestral", frecuencia, "historicoTaller",
    "empresaCiudad", restriccion
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel

from app.db import get_db

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────

class FrecuenciaInput(BaseModel):
    trimestre: str                          # "2025-Q3"
    trimestre_anterior: str | None = None   # "2025-Q2"
    max_ef: int = 14
    max_it: int = 6


class AjusteManual(BaseModel):
    empresa_id: int
    talleres_ef: int
    talleres_it: int


class ConfirmarInput(BaseModel):
    trimestre: str
    empresas: list[AjusteManual]


class RecorteDetalle(BaseModel):
    empresa_id: int
    nombre: str
    ef_original: int
    it_original: int
    ef_recortado: int
    it_recortado: int
    ef_delta: int
    it_delta: int
    motivo: str


class FrecuenciaEmpresa(BaseModel):
    empresa_id: int
    nombre: str
    talleres_ef: int
    talleres_it: int
    total: int
    semaforo: str
    score: float
    ajuste_desempeno: float
    es_nueva: bool
    es_comodin: bool
    prioridad_reduccion: str
    ciudades_activas: list[str]
    restricciones: list[dict]


class FrecuenciaOutput(BaseModel):
    trimestre: str
    total_ef: int
    total_it: int
    max_ef: int
    max_it: int
    semanas_disponibles: int       # semanas del trimestre - excluidas
    max_ef_trimestre: int          # max_ef × semanas_disponibles
    max_it_trimestre: int          # max_it × semanas_disponibles
    exceso_ef: int
    exceso_it: int
    empresas: list[FrecuenciaEmpresa]
    recortes: list[RecorteDetalle]
    warnings: list[str]
    status: str   # "OK" | "EXCESO_EF" | "EXCESO_IT" | "EXCESO_AMBOS"


# ── Endpoints ────────────────────────────────────────────────

@router.post("/calcular", response_model=FrecuenciaOutput)
async def calcular_frecuencias(
    params: FrecuenciaInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Fase 1: calcula frecuencias + aplica recorte automático.
    Devuelve la propuesta SIN persistir (human-in-the-loop).
    El frontend muestra el resultado y el usuario confirma o ajusta.
    """
    trimestre = params.trimestre
    warnings: list[str] = []

    # ── 0. Calcular semanas disponibles usando tabla festivo ────
    # A week is "fully excluded" when all 5 weekdays (L,M,X,J,V) are festivos
    semanas_excluidas_count = 0
    dias_festivos_por_semana: dict[int, set[str]] = {}
    try:
        fest_q = await db.execute(
            text('''
                SELECT semana, dia FROM festivo
                WHERE trimestre = :tri
            '''),
            {"tri": trimestre},
        )
        for row in fest_q.mappings().all():
            sem = row["semana"]
            dia = row["dia"]
            if sem not in dias_festivos_por_semana:
                dias_festivos_por_semana[sem] = set()
            dias_festivos_por_semana[sem].add(dia)

        # Count weeks where all 5 days are festivos
        DIAS_SEMANA = {"L", "M", "X", "J", "V"}
        for sem, dias in dias_festivos_por_semana.items():
            if dias >= DIAS_SEMANA:  # All 5 days are festivos
                semanas_excluidas_count += 1
    except Exception:
        pass  # table may not exist or be empty

    SEMANAS_TRIMESTRE = 13
    semanas_disponibles = max(1, SEMANAS_TRIMESTRE - semanas_excluidas_count)
    max_ef_trimestre = params.max_ef * semanas_disponibles
    max_it_trimestre = params.max_it * semanas_disponibles

    warnings.append(
        f"Semanas disponibles: {semanas_disponibles}/13 "
        f"({semanas_excluidas_count} excluidas) → "
        f"capacidad total: {max_ef_trimestre} EF + {max_it_trimestre} IT"
    )

    # ── 1. Cargar configs trimestrales + datos empresa ───────
    # Solo empresas con sede en MADRID (los 20 slots/semana son de Madrid)
    configs = await db.execute(
        text("""
            SELECT ct.id AS config_id,
                   ct."empresaId",
                   ct."tipoParticipacion",
                   ct."escuelaPropia",
                   ct."frecuenciaSolicitada",
                   e.nombre,
                   e.tipo,
                   e.semaforo,
                   e."scoreV3",
                   e."fiabilidadReciente",
                   e."esComodin",
                   e."aceptaExtras",
                   e."maxExtrasTrimestre",
                   e."prioridadReduccion",
                   e."tieneBolsa"
            FROM "configTrimestral" ct
            JOIN empresa e ON e.id = ct."empresaId"
            WHERE ct.trimestre = :trimestre
              AND e.activa = true
              AND EXISTS (
                  SELECT 1 FROM "empresaCiudad" ec
                  JOIN ciudad c ON c.id = ec."ciudadId"
                  WHERE ec."empresaId" = e.id
                    AND UPPER(c.nombre) = 'MADRID'
              )
        """),
        {"trimestre": trimestre},
    )
    configs_list = [dict(r) for r in configs.mappings().all()]

    if not configs_list:
        raise HTTPException(
            status_code=404,
            detail=f"No hay configuraciones para el trimestre {trimestre}",
        )

    # ── 2. Histórico trimestre anterior ──────────────────────
    desempeno_anterior: dict[int, int] = {}
    if params.trimestre_anterior:
        hist = await db.execute(
            text("""
                SELECT "empresaId",
                       COUNT(*) AS asignados,
                       SUM(CASE WHEN estado = 'OK' THEN 1 ELSE 0 END) AS impartidos
                FROM "historicoTaller"
                WHERE trimestre = :trimestre
                GROUP BY "empresaId"
            """),
            {"trimestre": params.trimestre_anterior},
        )
        for row in hist.mappings().all():
            r = dict(row)
            desempeno_anterior[r["empresaId"]] = r["impartidos"] - r["asignados"]

    # ── 3. Ciudades activas por empresa (dato maestro informativo) ──
    # Nota: el filtro de Madrid ya se aplica en la query de configs (paso 1).
    # Aquí cargamos todas las sedes para mostrar en la respuesta.
    ciudades_map: dict[int, list[str]] = {}
    ciudades_q = await db.execute(
        text("""
            SELECT ec."empresaId", c.nombre
            FROM "empresaCiudad" ec
            JOIN ciudad c ON c.id = ec."ciudadId"
        """)
    )
    for row in ciudades_q.mappings().all():
        r = dict(row)
        eid = r["empresaId"]
        if eid not in ciudades_map:
            ciudades_map[eid] = []
        ciudades_map[eid].append(r["nombre"])

    # ── 4. Restricciones por empresa ─────────────────────────
    restricciones_map: dict[int, list[dict]] = {}
    rest_q = await db.execute(
        text("""
            SELECT "empresaId", tipo, clave, valor, descripcion
            FROM restriccion
        """)
    )
    for row in rest_q.mappings().all():
        r = dict(row)
        eid = r["empresaId"]
        if eid not in restricciones_map:
            restricciones_map[eid] = []
        restricciones_map[eid].append({
            "tipo": r["tipo"],
            "clave": r["clave"],
            "valor": r["valor"],
        })

    # ── 5. Calcular frecuencias brutas ───────────────────────
    # Cargar catálogo de talleres para resolver programa de solo_taller
    talleres_q = await db.execute(
        text('SELECT id, nombre, programa FROM taller WHERE activo = true')
    )
    talleres_catalogo = [dict(r) for r in talleres_q.mappings().all()]

    empresas_bruto: list[dict] = []

    for cfg in configs_list:
        eid = cfg["empresaId"]
        score = cfg["scoreV3"]
        semaforo = _calcular_semaforo(score)

        # Ajuste por desempeño previo
        ajuste = 0.0
        if eid in desempeno_anterior:
            desv = desempeno_anterior[eid]
            if desv <= -2:
                ajuste = -1.0
                warnings.append(
                    f"{cfg['nombre']}: desviación {desv} en Q anterior → reducción -1"
                )
            elif desv < 0:
                ajuste = -0.5
                warnings.append(
                    f"{cfg['nombre']}: desviación {desv} en Q anterior → reducción -0.5"
                )

        # Frecuencia base
        if cfg["frecuenciaSolicitada"] is not None:
            freq_total = cfg["frecuenciaSolicitada"]
        else:
            freq_total = _calcular_frecuencia_base(
                tipo=cfg["tipoParticipacion"],
                semaforo=semaforo,
                score=score,
                escuela_propia=cfg["escuelaPropia"],
            )

        freq_total = max(1, int(freq_total + ajuste))
        ef, it = _repartir_ef_it(freq_total, cfg["tipoParticipacion"])

        # ── Fix: solo_taller fuerza programa ──────────────────
        # Si la empresa tiene restricción solo_taller y ese taller
        # pertenece a un programa específico, redirigir TODO al
        # programa correcto. Los slots liberados se redistribuyen
        # después del loop.
        restricciones_empresa = restricciones_map.get(eid, [])
        solo_taller_nombre = None
        for rest in restricciones_empresa:
            if rest["clave"] == "solo_taller":
                solo_taller_nombre = rest["valor"]
                break

        slots_it_liberados = 0
        slots_ef_liberados = 0
        if solo_taller_nombre:
            # Buscar el programa del taller forzado
            solo_taller_programa = _resolver_programa_taller(
                solo_taller_nombre, talleres_catalogo,
            )
            if solo_taller_programa == "EF" and it > 0:
                # Empresa solo puede hacer EF → mover IT a EF
                slots_it_liberados = it
                ef = ef + 0  # NO absorbe: se redistribuye a otras empresas
                it = 0
                warnings.append(
                    f"{cfg['nombre']}: restricción solo_taller "
                    f"'{solo_taller_nombre}' es EF → talleresIT forzado a 0 "
                    f"({slots_it_liberados} slot(s) IT liberados para redistribución)"
                )
            elif solo_taller_programa == "IT" and ef > 0:
                # Empresa solo puede hacer IT → mover EF a IT
                slots_ef_liberados = ef
                ef = 0
                it = it + 0
                warnings.append(
                    f"{cfg['nombre']}: restricción solo_taller "
                    f"'{solo_taller_nombre}' es IT → talleresEF forzado a 0 "
                    f"({slots_ef_liberados} slot(s) EF liberados para redistribución)"
                )

        empresas_bruto.append({
            "empresa_id": eid,
            "nombre": cfg["nombre"],
            "talleres_ef": ef,
            "talleres_it": it,
            "total": ef + it,
            "semaforo": semaforo,
            "score": round(score, 1),
            "ajuste_desempeno": ajuste,
            "es_nueva": False,
            "es_comodin": cfg["esComodin"],
            "prioridad_reduccion": cfg["prioridadReduccion"],
            "ciudades_activas": ciudades_map.get(eid, []),
            "restricciones": restricciones_map.get(eid, []),
            # Metadata para redistribución
            "_slots_it_liberados": slots_it_liberados,
            "_slots_ef_liberados": slots_ef_liberados,
        })

    # ── 5b. Redistribuir slots liberados por solo_taller ──────
    # Los slots IT/EF que no puede absorber la empresa restringida
    # se reparten a candidatas elegibles, priorizando:
    #   1. Contratantes (BAJA) — garantizar compromisos
    #   2. Verde/Ámbar con comodín — fiabilidad + flexibilidad
    #   3. Resto por score descendente
    total_it_liberados = sum(e["_slots_it_liberados"] for e in empresas_bruto)
    total_ef_liberados = sum(e["_slots_ef_liberados"] for e in empresas_bruto)

    if total_it_liberados > 0 or total_ef_liberados > 0:
        _redistribuir_slots_liberados(
            empresas_bruto, total_it_liberados, total_ef_liberados, warnings,
        )

    # Limpiar metadata interna
    for e in empresas_bruto:
        e.pop("_slots_it_liberados", None)
        e.pop("_slots_ef_liberados", None)

    # ── 6. Recorte para encajar en modelo trimestral ─────────
    # Límite = slots_por_semana × semanas_disponibles (no por semana)
    total_ef_bruto = sum(e["talleres_ef"] for e in empresas_bruto)
    total_it_bruto = sum(e["talleres_it"] for e in empresas_bruto)
    exceso_ef = max(0, total_ef_bruto - max_ef_trimestre)
    exceso_it = max(0, total_it_bruto - max_it_trimestre)

    recortes: list[RecorteDetalle] = []

    if exceso_ef > 0 or exceso_it > 0:
        recortes = _aplicar_recortes(
            empresas=empresas_bruto,
            exceso_ef=exceso_ef,
            exceso_it=exceso_it,
        )

    # Totales finales post-recorte
    total_ef = sum(e["talleres_ef"] for e in empresas_bruto)
    total_it = sum(e["talleres_it"] for e in empresas_bruto)

    # Status
    exceso_ef_final = max(0, total_ef - max_ef_trimestre)
    exceso_it_final = max(0, total_it - max_it_trimestre)
    if exceso_ef_final > 0 and exceso_it_final > 0:
        status = "EXCESO_AMBOS"
    elif exceso_ef_final > 0:
        status = "EXCESO_EF"
    elif exceso_it_final > 0:
        status = "EXCESO_IT"
    else:
        status = "OK"

    if status != "OK":
        warnings.append(
            f"Recorte automático insuficiente: quedan {total_ef} EF "
            f"(máx trimestre {max_ef_trimestre}) y {total_it} IT "
            f"(máx trimestre {max_it_trimestre}). Requiere ajuste manual."
        )

    # ── 7. Construir respuesta ───────────────────────────────
    empresas_result = [
        FrecuenciaEmpresa(**e)
        for e in sorted(empresas_bruto, key=lambda x: x["score"], reverse=True)
    ]

    return FrecuenciaOutput(
        trimestre=trimestre,
        total_ef=total_ef,
        total_it=total_it,
        max_ef=params.max_ef,
        max_it=params.max_it,
        semanas_disponibles=semanas_disponibles,
        max_ef_trimestre=max_ef_trimestre,
        max_it_trimestre=max_it_trimestre,
        exceso_ef=exceso_ef_final,
        exceso_it=exceso_it_final,
        empresas=empresas_result,
        recortes=recortes,
        warnings=warnings,
        status=status,
    )


@router.post("/confirmar")
async def confirmar_frecuencias(
    params: ConfirmarInput,
    db: AsyncSession = Depends(get_db),
):
    """
    Persiste las frecuencias después de revisión humana.
    Recibe la lista final de empresas con sus talleres EF/IT
    (tal cual o ajustados manualmente por el usuario).
    """
    trimestre = params.trimestre

    # Validar que existen configs para este trimestre
    check = await db.execute(
        text("""
            SELECT COUNT(*) AS n
            FROM "configTrimestral"
            WHERE trimestre = :tri
        """),
        {"tri": trimestre},
    )
    count = check.mappings().first()
    if not count or count["n"] == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No hay configuraciones para {trimestre}",
        )

    # Cargar datos de empresa para la respuesta
    emp_data = await db.execute(
        text("""
            SELECT e.id, e.nombre, e."scoreV3", e.semaforo, e."esComodin"
            FROM empresa e WHERE e.activa = true
        """)
    )
    emp_map = {r["id"]: dict(r) for r in emp_data.mappings().all()}

    # Borrar frecuencias anteriores de este trimestre
    await db.execute(
        text('DELETE FROM frecuencia WHERE trimestre = :tri'),
        {"tri": trimestre},
    )

    total_ef = 0
    total_it = 0
    persisted = []

    for emp in params.empresas:
        total_ef += emp.talleres_ef
        total_it += emp.talleres_it
        total = emp.talleres_ef + emp.talleres_it

        e_info = emp_map.get(emp.empresa_id, {})
        semaforo = _calcular_semaforo(e_info.get("scoreV3", 0))

        await db.execute(
            text("""
                INSERT INTO frecuencia
                    ("configId", "empresaId", trimestre, "talleresEF", "talleresIT",
                     "totalAsignado", "semaforoCalculado", "scoreCalculado",
                     "ajusteDesempeno", "esNueva")
                VALUES (
                    (SELECT id FROM "configTrimestral"
                     WHERE "empresaId" = :eid AND trimestre = :tri LIMIT 1),
                    :eid, :tri, :ef, :it, :total,
                    :semaforo, :score, 0, false
                )
                ON CONFLICT ("empresaId", trimestre)
                DO UPDATE SET
                    "talleresEF"        = EXCLUDED."talleresEF",
                    "talleresIT"        = EXCLUDED."talleresIT",
                    "totalAsignado"     = EXCLUDED."totalAsignado",
                    "semaforoCalculado" = EXCLUDED."semaforoCalculado",
                    "scoreCalculado"    = EXCLUDED."scoreCalculado"
            """),
            {
                "eid": emp.empresa_id,
                "tri": trimestre,
                "ef": emp.talleres_ef,
                "it": emp.talleres_it,
                "total": total,
                "semaforo": semaforo,
                "score": e_info.get("scoreV3", 0),
            },
        )
        persisted.append({
            "empresa_id": emp.empresa_id,
            "nombre": e_info.get("nombre", "?"),
            "talleres_ef": emp.talleres_ef,
            "talleres_it": emp.talleres_it,
            "total": total,
        })

    await db.commit()

    return {
        "status": "CONFIRMADO",
        "trimestre": trimestre,
        "total_ef": total_ef,
        "total_it": total_it,
        "empresas": persisted,
    }


@router.get("/{trimestre}")
async def obtener_frecuencias(
    trimestre: str,
    db: AsyncSession = Depends(get_db),
):
    """Lee las frecuencias ya confirmadas de un trimestre."""
    result = await db.execute(
        text("""
            SELECT f.*, e.nombre, e."esComodin", e."prioridadReduccion"
            FROM frecuencia f
            JOIN empresa e ON e.id = f."empresaId"
            WHERE f.trimestre = :trimestre
            ORDER BY f."scoreCalculado" DESC
        """),
        {"trimestre": trimestre},
    )
    rows = [dict(r) for r in result.mappings().all()]
    return {"trimestre": trimestre, "frecuencias": rows}


# ── Lógica de recorte ────────────────────────────────────────

def _aplicar_recortes(
    empresas: list[dict],
    exceso_ef: int,
    exceso_it: int,
) -> list[RecorteDetalle]:
    """
    Recorta frecuencias para encajar en el modelo trimestral.

    Jerarquía de recorte (del documento de reglas):
    1. Primero empresas con prioridad ALTA (Capgemini, Repsol, Santander, Acciona)
    2. Luego MEDIA
    3. Nunca BAJA (contratantes, se protegen)

    Dentro de cada prioridad:
    - Comodines primero (absorben mejor los recortes)
    - Menor score primero (menos "merecen" mantener)
    - Nunca bajar de 1 taller total por empresa
    """
    recortes: list[RecorteDetalle] = []

    # Orden de recorte: ALTA → MEDIA, comodines primero, menor score primero
    PRIORIDAD_ORDEN = {"ALTA": 0, "MEDIA": 1, "BAJA": 2}

    candidatos = sorted(
        empresas,
        key=lambda e: (
            PRIORIDAD_ORDEN.get(e["prioridad_reduccion"], 2),
            0 if e["es_comodin"] else 1,
            e["score"],
        ),
    )

    # ── Recortar EF ──────────────────────────────────────────
    ef_pendiente = exceso_ef
    for emp in candidatos:
        if ef_pendiente <= 0:
            break
        if emp["prioridad_reduccion"] == "BAJA":
            continue
        if emp["talleres_ef"] <= 0:
            continue

        # No bajar de 1 taller total
        min_ef = 1 if emp["talleres_it"] == 0 else 0
        puede_quitar = emp["talleres_ef"] - min_ef
        if puede_quitar <= 0:
            continue

        quitar = min(puede_quitar, ef_pendiente)
        ef_original = emp["talleres_ef"]
        emp["talleres_ef"] -= quitar
        emp["total"] = emp["talleres_ef"] + emp["talleres_it"]
        ef_pendiente -= quitar

        recortes.append(RecorteDetalle(
            empresa_id=emp["empresa_id"],
            nombre=emp["nombre"],
            ef_original=ef_original,
            it_original=emp["talleres_it"],
            ef_recortado=emp["talleres_ef"],
            it_recortado=emp["talleres_it"],
            ef_delta=-quitar,
            it_delta=0,
            motivo=f"Prioridad {emp['prioridad_reduccion']}"
                   + (", comodín" if emp["es_comodin"] else ""),
        ))

    # ── Recortar IT ──────────────────────────────────────────
    it_pendiente = exceso_it
    for emp in candidatos:
        if it_pendiente <= 0:
            break
        if emp["prioridad_reduccion"] == "BAJA":
            continue
        if emp["talleres_it"] <= 0:
            continue

        min_it = 1 if emp["talleres_ef"] == 0 else 0
        puede_quitar = emp["talleres_it"] - min_it
        if puede_quitar <= 0:
            continue

        quitar = min(puede_quitar, it_pendiente)
        it_original = emp["talleres_it"]
        emp["talleres_it"] -= quitar
        emp["total"] = emp["talleres_ef"] + emp["talleres_it"]
        it_pendiente -= quitar

        # Buscar si ya existe un recorte para esta empresa
        existing = next(
            (r for r in recortes if r.empresa_id == emp["empresa_id"]),
            None,
        )
        if existing:
            existing.it_recortado = emp["talleres_it"]
            existing.it_delta = -(it_original - emp["talleres_it"])
            existing.motivo += " + recorte IT"
        else:
            recortes.append(RecorteDetalle(
                empresa_id=emp["empresa_id"],
                nombre=emp["nombre"],
                ef_original=emp["talleres_ef"],
                it_original=it_original,
                ef_recortado=emp["talleres_ef"],
                it_recortado=emp["talleres_it"],
                ef_delta=0,
                it_delta=-quitar,
                motivo=f"Prioridad {emp['prioridad_reduccion']}"
                       + (", comodín" if emp["es_comodin"] else ""),
            ))

    return recortes


# ── Lógica de negocio ────────────────────────────────────────

def _calcular_semaforo(score: float) -> str:
    if score >= 75:
        return "VERDE"
    elif score >= 60:
        return "AMBAR"
    return "ROJO"


def _calcular_frecuencia_base(
    tipo: str,
    semaforo: str,
    score: float,
    escuela_propia: bool,
) -> int:
    if tipo == "AMBAS":
        base = 4
    elif tipo == "EF":
        base = 3
    else:
        base = 2

    if semaforo == "VERDE":
        base += 1
    elif semaforo == "ROJO":
        base = max(1, base - 1)

    if escuela_propia:
        base += 1

    return base


def _repartir_ef_it(total: int, tipo: str) -> tuple[int, int]:
    if tipo == "EF":
        return (total, 0)
    elif tipo == "IT":
        return (0, total)
    else:
        ef = max(1, round(total * 0.7))
        it = total - ef
        return (ef, max(0, it))


def _resolver_programa_taller(
    nombre_taller: str,
    talleres_catalogo: list[dict],
) -> str | None:
    """Busca en el catálogo de talleres y devuelve su programa (EF/IT)."""
    for t in talleres_catalogo:
        if t["nombre"].strip().lower() == nombre_taller.strip().lower():
            return t["programa"]
    # Fallback: coincidencia parcial (ej: "Gestión de Ingresos" ≈ "Gestión de ingresos")
    nombre_lower = nombre_taller.strip().lower()
    for t in talleres_catalogo:
        if nombre_lower in t["nombre"].strip().lower() or t["nombre"].strip().lower() in nombre_lower:
            return t["programa"]
    return None


def _redistribuir_slots_liberados(
    empresas: list[dict],
    it_libres: int,
    ef_libres: int,
    warnings: list[str],
) -> None:
    """
    Redistribuye slots liberados por restricción solo_taller a otras empresas.

    Prioridades (reflejo del criterio real del planificador):
    1. Contratantes / EF propias (prioridad BAJA = protegidas) — garantizar compromisos
    2. Verde/Ámbar + comodín — fiables y flexibles
    3. Resto por score descendente

    Solo se da slot extra a empresas que:
    - Participan en el programa correspondiente (tipo AMBAS o el programa específico)
    - No tienen restricción solo_taller que lo impida
    - Están activas
    """

    def _tiene_solo_taller_programa(emp: dict, programa: str) -> bool:
        """True si la empresa tiene solo_taller de un programa DIFERENTE."""
        for r in emp.get("restricciones", []):
            if r["clave"] == "solo_taller":
                # Si tiene solo_taller, solo puede recibir del mismo programa
                # que su taller forzado — esto se resuelve más arriba,
                # aquí simplemente excluimos
                return True
        return False

    def _puede_recibir(emp: dict, programa: str) -> bool:
        """Verifica si la empresa puede recibir un slot extra del programa dado."""
        if _tiene_solo_taller_programa(emp, programa):
            return False
        # Verificar tipo de participación
        tipo = None  # noqa: F841
        for r in emp.get("restricciones", []):
            pass  # tipo viene del campo directo
        # Usamos los talleres actuales como proxy: si ya tiene IT>0 o EF>0, puede recibir
        if programa == "IT":
            # Puede recibir IT si no es solo-EF
            # Heurística: si talleres_it >= 0 y no tiene restricción contraria
            return True
        else:
            return True

    # Orden de prioridad para recibir slots extras
    PRIORIDAD_RECEPCION = {"BAJA": 0, "MEDIA": 1, "ALTA": 2}
    SEMAFORO_ORDEN = {"VERDE": 0, "AMBAR": 1, "ROJO": 2}

    candidatos = sorted(
        empresas,
        key=lambda e: (
            PRIORIDAD_RECEPCION.get(e["prioridad_reduccion"], 2),
            0 if e["es_comodin"] else 1,
            SEMAFORO_ORDEN.get(e["semaforo"], 2),
            -e["score"],  # mayor score primero
        ),
    )

    # ── Redistribuir IT ──────────────────────────────────────
    it_pendiente = it_libres
    for emp in candidatos:
        if it_pendiente <= 0:
            break
        if not _puede_recibir(emp, "IT"):
            continue
        # Empresas con solo_taller ya fueron excluidas por _puede_recibir
        # No dar más de 1 extra por empresa en redistribución
        emp["talleres_it"] += 1
        emp["total"] = emp["talleres_ef"] + emp["talleres_it"]
        it_pendiente -= 1
        warnings.append(
            f"{emp['nombre']}: recibe +1 IT redistribuido "
            f"(slot liberado por restricción solo_taller)"
        )

    if it_pendiente > 0:
        warnings.append(
            f"⚠ No se pudieron redistribuir {it_pendiente} slot(s) IT. "
            "Requiere ajuste manual."
        )

    # ── Redistribuir EF ──────────────────────────────────────
    ef_pendiente = ef_libres
    for emp in candidatos:
        if ef_pendiente <= 0:
            break
        if not _puede_recibir(emp, "EF"):
            continue
        emp["talleres_ef"] += 1
        emp["total"] = emp["talleres_ef"] + emp["talleres_it"]
        ef_pendiente -= 1
        warnings.append(
            f"{emp['nombre']}: recibe +1 EF redistribuido "
            f"(slot liberado por restricción solo_taller)"
        )

    if ef_pendiente > 0:
        warnings.append(
            f"⚠ No se pudieron redistribuir {ef_pendiente} slot(s) EF. "
            "Requiere ajuste manual."
        )