from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def calcular_fecha_slot(trimestre: str, semana: int, dia: str) -> str:
    """Returns formatted date string like '13 Abr 2026' for a slot."""
    from datetime import date, timedelta

    MESES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
             "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

    year = int(trimestre[:4])
    quarter = int(trimestre[-1])
    month_start = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]
    first_day = date(year, month_start, 1)

    # Find first Monday
    days_until_monday = (7 - first_day.weekday()) % 7
    if first_day.weekday() == 0:
        first_monday = first_day
    else:
        first_monday = first_day + timedelta(days=days_until_monday)

    week_start = first_monday + timedelta(weeks=semana - 1)
    dia_offset = {"L": 0, "M": 1, "X": 2, "J": 3, "V": 4}
    fecha = week_start + timedelta(days=dia_offset.get(dia, 0))

    return f"{fecha.day} {MESES[fecha.month - 1]} {fecha.year}"


async def _guardar_log(db: AsyncSession, trimestre: str, resultado: dict):
    """Guarda el log del solver."""
    await db.execute(
        text("""
            INSERT INTO "solverLog"
                (trimestre, status, "tiempoSegundos",
                 "inviolablesCumplidas", "preferentesCumplidas", warnings)
            VALUES (:tri, :status, :tiempo, :inv, :pref, :warn)
        """),
        {
            "tri": trimestre,
            "status": resultado["status"],
            "tiempo": resultado["tiempo_segundos"],
            "inv": resultado["inviolables_pct"],
            "pref": resultado["preferentes_pct"],
            "warn": str(resultado.get("warnings", [])),
        },
    )


def _asignar_ciudades(
    slots_raw: list[dict],
    frecuencias: list[dict],
    madrid_id: int | None,
    restricciones: list[dict],
    warnings: list[str],
) -> list[dict]:
    """
    Post-proceso simplificado: todos los talleres son de Madrid.
    Genera sugerencias de contingencia para slots vacantes.
    """
    # Indexar restricciones
    no_comodin_ids: set[int] = set()
    max_extras_map: dict[int, int] = {}
    solo_taller_map: dict[int, str] = {}  # empresaId → nombre del taller permitido
    for r in restricciones:
        if r["clave"] == "no_comodin":
            no_comodin_ids.add(r["empresaId"])
        if r["clave"] == "max_extras":
            try:
                max_extras_map[r["empresaId"]] = int(r["valor"])
            except ValueError:
                pass
        if r["clave"] == "solo_taller":
            solo_taller_map[r["empresaId"]] = r["valor"].strip().lower()

    # Empresas comodín: esComodin=true Y no están en no_comodin
    comodines = [
        f for f in frecuencias
        if f.get("esComodin") and f["empresaId"] not in no_comodin_ids
    ]
    # Ordenar por score descendente para priorizar mejores candidatos
    comodines.sort(key=lambda f: f.get("scoreCalculado", 0), reverse=True)

    slots_completos = []
    for slot in slots_raw:
        eid = slot["empresa_id"]

        # Sugerencias de contingencia para slots vacantes
        sugerencias = None
        if eid == 0 and comodines:
            sugerencias = []
            taller_nombre_lower = slot.get("taller_nombre", "").strip().lower()
            for com in comodines:
                com_id = com["empresaId"]
                # Filtrar: si el comodín tiene solo_taller, solo sugerirlo
                # para slots cuyo taller coincide con su restricción
                if com_id in solo_taller_map:
                    nombre_permitido = solo_taller_map[com_id]
                    if (nombre_permitido not in taller_nombre_lower
                            and taller_nombre_lower not in nombre_permitido):
                        continue  # No es su taller → no sugerir
                max_ex = max_extras_map.get(com_id)
                motivo = "Comodín disponible"
                if max_ex is not None:
                    motivo += f" (max {max_ex} extras/trimestre)"
                if com_id in solo_taller_map:
                    motivo += f" (solo taller: {solo_taller_map[com_id]})"
                sugerencias.append({
                    "empresa_id": com_id,
                    "empresa_nombre": com["nombre"],
                    "motivo": motivo,
                    "prioridad": len(sugerencias) + 1,
                })
                if len(sugerencias) >= 4:
                    break

        slots_completos.append({
            **slot,
            "ciudad_id": madrid_id if eid != 0 else None,
            "ciudad": "MADRID" if eid != 0 else None,
            "tipo_asignacion": "BASE",
            "sugerencias": sugerencias,
        })

    return slots_completos
