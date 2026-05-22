"""V27 — Pre-validación de Configuración Trimestral.

Antes de invocar el solver (que puede tardar segundos y reportar INFEASIBLE
sin explicar el porqué), este servicio inspecciona la CT del trimestre y
señala inconsistencias que el solver no podría resolver. Tres validaciones:

  V1 (ERROR)   freq_ef_sin_dias_disponibles / freq_it_sin_dias_disponibles:
               la empresa pide N talleres de un programa pero sus
               `disponibilidadDias` no incluyen ningún día con talleres de
               ese programa en el catálogo del trimestre. Caso real EY del
               Q3 2026 que motivó la feature.

  V2 (ERROR)   tipo_incoherente_ef / tipo_incoherente_it: tipoParticipacion
               restringe a un solo programa pero la frecuencia del programa
               opuesto es >0. tipoParticipacion=NULL se trata como 'AMBAS'
               (V24 D4 compat defensiva) y no dispara.

  V3 (WARNING) empresa_fantasma: ambas frecuencias = 0/NULL. Puede ser
               intencional (CT preparada para ajustar después), por eso es
               warning y no error.

El catálogo se calcula como **unión** de talleres aplicables a cada una de
las semanas del trimestre — si un taller IT existe solo en alguna semana,
sigue contando como "IT disponible". Reutiliza `cargar_talleres_semana` del
router calendario_anual (misma función que usa el solver). Costo: ~13
queries on-demand, trivial para el caso de uso.

Empresas inactivas se excluyen (consistente con `check_empresa_activa`).
"""

from typing import Literal

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.routers.calendario_anual import cargar_talleres_semana, trimestre_to_weeks


DIAS_DEFAULT = "L,M,X,J,V"


class ValidacionItem(BaseModel):
    empresa_id: int
    empresa_nombre: str
    severidad: Literal["error", "warning"]
    tipo: str
    detalle: str
    sugerencia: str


class ValidarCTResponse(BaseModel):
    trimestre: str
    total_empresas_revisadas: int
    errores: list[ValidacionItem]
    warnings: list[ValidacionItem]
    resumen: str


def _parse_dias(dias_raw: str | None) -> set[str]:
    """Normaliza el string de disponibilidadDias a un set de códigos L/M/X/J/V.

    Acepta "L,M,X", "L, M, X" y NULL (→ default L-V). Strip + uppercase para
    tolerar variantes históricas en Excel imports.
    """
    if dias_raw is None or not dias_raw.strip():
        dias_raw = DIAS_DEFAULT
    return {d.strip().upper() for d in dias_raw.split(",") if d.strip()}


async def _cargar_cts_activas(db: AsyncSession, trimestre: str) -> list[dict]:
    """Devuelve CTs del trimestre con join a empresa, filtrando inactivas."""
    result = await db.execute(
        text(
            'SELECT ct."empresaId" AS empresa_id, e.nombre AS empresa_nombre, '
            '       ct."tipoParticipacion" AS tipo_participacion, '
            '       ct."frecuenciaEF" AS frecuencia_ef, '
            '       ct."frecuenciaIT" AS frecuencia_it, '
            '       ct."disponibilidadDias" AS disponibilidad_dias '
            'FROM "configTrimestral" ct '
            'JOIN empresa e ON e.id = ct."empresaId" '
            'WHERE ct.trimestre = :tri AND e.activa = true '
            'ORDER BY e.nombre'
        ),
        {"tri": trimestre},
    )
    return [dict(r) for r in result.mappings().all()]


async def _cargar_dias_disponibles(
    db: AsyncSession, trimestre: str
) -> tuple[set[str], set[str]]:
    """Días con al menos un taller EF / IT en alguna semana del trimestre.

    Unión sobre las semanas, no intersección — un taller que existe en S8
    cuenta como "disponible" para la planificación trimestral aunque no esté
    en S1.
    """
    anio, week_start, week_end = trimestre_to_weeks(trimestre)
    dias_ef: set[str] = set()
    dias_it: set[str] = set()
    for semana in range(week_start, week_end + 1):
        talleres = await cargar_talleres_semana(db, anio, semana)
        for t in talleres:
            dia = (t.get("dia_semana") or "").strip().upper()
            programa = (t.get("programa") or "").strip().upper()
            if not dia:
                continue
            if programa == "EF":
                dias_ef.add(dia)
            elif programa == "IT":
                dias_it.add(dia)
    return dias_ef, dias_it


def _validar_dias_programa(
    ct: dict,
    programa: Literal["EF", "IT"],
    dias_disponibles: set[str],
) -> ValidacionItem | None:
    """V1 — chequea que freq>0 tenga algún día compatible con el catálogo."""
    freq = ct.get(f"frecuencia_{programa.lower()}") or 0
    if freq <= 0:
        return None
    dias_empresa = _parse_dias(ct.get("disponibilidad_dias"))
    if dias_empresa & dias_disponibles:
        return None
    dias_disp_str = ",".join(sorted(dias_disponibles)) or "(ninguno)"
    dias_emp_str = ",".join(sorted(dias_empresa)) or "(ninguno)"
    return ValidacionItem(
        empresa_id=ct["empresa_id"],
        empresa_nombre=ct["empresa_nombre"],
        severidad="error",
        tipo=f"freq_{programa.lower()}_sin_dias_disponibles",
        detalle=(
            f"frecuencia {programa}={freq} pero días configurados ({dias_emp_str}) "
            f"no incluyen ningún día con talleres {programa} en el catálogo "
            f"({programa} disponible: {dias_disp_str})."
        ),
        sugerencia=(
            f"Cambiar frecuencia{programa} a 0, o ampliar días a incluir "
            f"alguno de: {dias_disp_str}."
        ),
    )


def _validar_tipo_incoherente(ct: dict) -> list[ValidacionItem]:
    """V2 — tipoParticipacion EF/IT pero frecuencia del opuesto > 0."""
    tipo = (ct.get("tipo_participacion") or "").upper()
    freq_ef = ct.get("frecuencia_ef") or 0
    freq_it = ct.get("frecuencia_it") or 0
    items: list[ValidacionItem] = []
    if tipo == "EF" and freq_it > 0:
        items.append(
            ValidacionItem(
                empresa_id=ct["empresa_id"],
                empresa_nombre=ct["empresa_nombre"],
                severidad="error",
                tipo="tipo_incoherente_it",
                detalle=(
                    f"tipoParticipacion='EF' pero frecuenciaIT={freq_it}. "
                    f"Una empresa tipo EF no debería recibir talleres IT."
                ),
                sugerencia=(
                    "Cambiar tipoParticipacion a 'AMBAS', o poner "
                    "frecuenciaIT a 0/NULL."
                ),
            )
        )
    if tipo == "IT" and freq_ef > 0:
        items.append(
            ValidacionItem(
                empresa_id=ct["empresa_id"],
                empresa_nombre=ct["empresa_nombre"],
                severidad="error",
                tipo="tipo_incoherente_ef",
                detalle=(
                    f"tipoParticipacion='IT' pero frecuenciaEF={freq_ef}. "
                    f"Una empresa tipo IT no debería recibir talleres EF."
                ),
                sugerencia=(
                    "Cambiar tipoParticipacion a 'AMBAS', o poner "
                    "frecuenciaEF a 0/NULL."
                ),
            )
        )
    return items


def _validar_empresa_fantasma(ct: dict) -> ValidacionItem | None:
    """V3 — ambas frecuencias 0/NULL → CT sin efecto este trimestre."""
    freq_ef = ct.get("frecuencia_ef") or 0
    freq_it = ct.get("frecuencia_it") or 0
    if freq_ef > 0 or freq_it > 0:
        return None
    return ValidacionItem(
        empresa_id=ct["empresa_id"],
        empresa_nombre=ct["empresa_nombre"],
        severidad="warning",
        tipo="empresa_fantasma",
        detalle=(
            "Empresa tiene Config Trimestral pero ambas frecuencias son "
            "0/NULL. No recibirá talleres este trimestre."
        ),
        sugerencia=(
            "Si la empresa no participa este trimestre, considerá eliminar "
            "su CT. Si participa, definí al menos una frecuencia."
        ),
    )


async def validar_configuracion_trimestral(
    db: AsyncSession, trimestre: str
) -> ValidarCTResponse:
    """Punto de entrada del endpoint GET /api/config-trimestral/{tri}/validar."""
    cts = await _cargar_cts_activas(db, trimestre)
    dias_ef, dias_it = await _cargar_dias_disponibles(db, trimestre)

    errores: list[ValidacionItem] = []
    warnings: list[ValidacionItem] = []

    for ct in cts:
        v1_ef = _validar_dias_programa(ct, "EF", dias_ef)
        if v1_ef:
            errores.append(v1_ef)
        v1_it = _validar_dias_programa(ct, "IT", dias_it)
        if v1_it:
            errores.append(v1_it)
        errores.extend(_validar_tipo_incoherente(ct))
        v3 = _validar_empresa_fantasma(ct)
        if v3:
            warnings.append(v3)

    resumen = f"{len(errores)} errores, {len(warnings)} warnings detectados."
    return ValidarCTResponse(
        trimestre=trimestre,
        total_empresas_revisadas=len(cts),
        errores=errores,
        warnings=warnings,
        resumen=resumen,
    )
