import os
import time
import math

from app.schemas.calendario import CalendarioInput


def _franja_preferida(
    restricciones_empresa: list[dict],
    dia_codigo: str,  # "L" | "M" | "X" | "J" | "V"
) -> tuple[str | None, str | None]:
    """
    Returns (franja, tipo) for the given empresa and day.

    Priority: `franja_por_dia` for the specific day wins over the global
    `franja_horaria`. `tipo` is "HARD" or "SOFT". Returns (None, None) if no
    preference exists.

    V16 helper. Shared between solver pre-filter, solver SOFT penalty pass,
    and validar_asignacion in calendario.py — DO NOT duplicate.
    """
    franja_dia: str | None = None
    tipo_dia: str | None = None
    franja_global: str | None = None
    tipo_global: str | None = None

    for r in restricciones_empresa:
        clave = r.get("clave")
        valor = r.get("valor") or ""
        tipo = r.get("tipo")  # "HARD" | "SOFT"

        if clave == "franja_por_dia" and valor.startswith(f"{dia_codigo}:"):
            # "L:12:00-14:00" -> "12:00-14:00"
            franja_dia = valor.split(":", 1)[1]
            tipo_dia = tipo
        elif clave == "franja_horaria":
            franja_global = valor
            tipo_global = tipo

    if franja_dia is not None:
        return franja_dia, tipo_dia
    return franja_global, tipo_global


def _dias_exclusivos_hard(
    restricciones_empresa: list[dict],
) -> set[str] | None:
    """
    If the empresa has at least one HARD `franja_por_dia`, return the set of
    declared day codes (subset of {"L","M","X","J","V"}). Days not in that set
    are NOT valid candidates for this empresa.

    Returns None when the empresa has zero HARD `franja_por_dia` restrictions,
    meaning no day-exclusivity filter applies (solo_dia / configTrimestral /
    franja_horaria HARD still apply through their own paths).

    SOFT `franja_por_dia` does NOT trigger day-exclusivity — a SOFT preference
    must remain a scoring penalty via `_franja_preferida`, never a HARD filter.

    V16.1 helper. Sibling of `_franja_preferida`. Shared with validar_asignacion.
    """
    dias: set[str] = set()
    for r in restricciones_empresa:
        if r.get("clave") != "franja_por_dia":
            continue
        if r.get("tipo") != "HARD":
            continue
        valor = r.get("valor") or ""
        if len(valor) >= 1 and valor[0] in {"L", "M", "X", "J", "V"}:
            dias.add(valor[0])
    return dias if dias else None


def _generate_hints(
    possible: set,
    empresa_ids: list[int],
    empresas: dict,
    SEMANAS: list[int],
    taller_ids: list[int],
    taller_map: dict,
    taller_ids_ef: list[int],
    taller_ids_it: list[int],
) -> dict:
    """
    Generates a greedy initial solution to warm-start the solver.
    Returns dict of (empresa, semana, taller) -> 1 for hinted assignments.
    """
    hints = {}

    # Sort companies by most constrained first (fewer possible slots)
    flexibility = {}
    for e in empresa_ids:
        flexibility[e] = sum(1 for s in SEMANAS for t in taller_ids if (e, s, t) in possible)

    companies_by_flexibility = sorted(empresa_ids, key=lambda e: flexibility[e])

    slot_taken = set()  # (semana, taller) already assigned
    empresa_week_used = set()  # (empresa, semana) to enforce max 1/week
    empresa_ef_count = {e: 0 for e in empresa_ids}
    empresa_it_count = {e: 0 for e in empresa_ids}

    for e in companies_by_flexibility:
        ef_needed = int(empresas[e].get("talleresEF", 0) or 0)
        it_needed = int(empresas[e].get("talleresIT", 0) or 0)
        total_needed = int(empresas[e].get("totalAsignado", 0) or 0)

        # Empresas with >= 6 talleres (escuela propia) can have multiple per week
        max_per_week = 20 if total_needed >= 6 else 1

        # Collect available slots for this company
        available_ef = []
        available_it = []
        for s in SEMANAS:
            week_count = sum(1 for (ee, ss, _) in hints if ee == e and ss == s)
            if week_count >= max_per_week:
                continue
            for t in taller_ids:
                if (e, s, t) not in possible:
                    continue
                if (s, t) in slot_taken:
                    continue
                prog = taller_map[t]["programa"]
                if prog == "EF":
                    available_ef.append((s, t))
                else:
                    available_it.append((s, t))

        # Assign EF — spread across weeks
        available_ef.sort(key=lambda x: x[0])
        assigned_ef = 0
        for s, t in available_ef:
            if assigned_ef >= ef_needed:
                break
            week_count = sum(1 for (ee, ss, _) in hints if ee == e and ss == s)
            if week_count >= max_per_week:
                continue
            if (s, t) not in slot_taken:
                hints[(e, s, t)] = 1
                slot_taken.add((s, t))
                assigned_ef += 1

        # Assign IT — spread across weeks
        available_it.sort(key=lambda x: x[0])
        assigned_it = 0
        for s, t in available_it:
            if assigned_it >= it_needed:
                break
            week_count = sum(1 for (ee, ss, _) in hints if ee == e and ss == s)
            if week_count >= max_per_week:
                continue
            if (s, t) not in slot_taken:
                hints[(e, s, t)] = 1
                slot_taken.add((s, t))
                assigned_it += 1

    return hints


def _ejecutar_solver(
    frecuencias: list[dict],
    restricciones: list[dict],
    talleres: list[dict],
    talleres_por_semana: dict[int, list[dict]],  # NEW: per-week taller lists
    disponibilidad_map: dict[int, list[str]],
    semanas_excluidas: set[int],
    dias_excluidos: set[tuple[int, str]],
    params: CalendarioInput,
) -> dict:
    """
    Asigna empresas a slots fijos semanales.
    OPTIMIZADO V11: Pre-filtering + hints + solution callback.

    Variables: assign[empresa_id, semana, taller_id] ∈ {0,1}
    Solo se crean variables para combinaciones POSIBLES.
    """
    from ortools.sat.python import cp_model
    import time
    import math

    model = cp_model.CpModel()
    start_time = time.time()
    warnings: list[str] = []

    # ─── DEBUG: Constraint counters ──────────────────────────
    debug_stats = {
        "empresas": 0,
        "semanas": 0,
        "talleres": 0,
        "talleres_ef": 0,
        "talleres_it": 0,
        "decision_vars_naive": 0,
        "decision_vars_actual": 0,
        "vars_filtered_out": 0,
        "total_frecuencias": 0,
        "total_slots_available": 0,
        "slots_blocked_festivos": 0,
        "H1_constraints": 0,
        "H2_constraints": 0,
        "H3_constraints": 0,
        "H4_filtered": 0,
        "H5_filtered": 0,
        "H6_constraints": 0,
        "H7_filtered": 0,
        "H8_filtered": 0,
        "H_franja_filtered": 0,  # V16
        "H_franja_dia_filtered": 0,  # V16.1: día no en set HARD franja_por_dia
        "S1_penalties": 0,
        "S2_penalties": 0,
        "S3_penalties": 0,
        "S4_penalties": 0,
        "S5_penalties": 0,
        "S_franja_penalties": 0,  # V16
        "hints_generated": 0,
    }

    SEMANAS = [s for s in range(1, params.semanas + 1) if s not in semanas_excluidas]
    if semanas_excluidas:
        warnings.append(
            f"Solver omite semanas {sorted(semanas_excluidas)} — excluidas del trimestre"
        )

    empresas = {f["empresaId"]: f for f in frecuencias}
    # FIX: Robust filter — handle string "0", None, or missing totalAsignado
    empresa_ids = [e for e in empresas if int(empresas[e].get("totalAsignado", 0) or 0) > 0]

    talleres_ef = [t for t in talleres if t["programa"] == "EF"]
    talleres_it = [t for t in talleres if t["programa"] == "IT"]
    taller_ids_ef = [t["id"] for t in talleres_ef]
    taller_ids_it = [t["id"] for t in talleres_it]
    taller_ids = [t["id"] for t in talleres]
    taller_map = {t["id"]: t for t in talleres}

    # ─── DEBUG: Log initial dimensions ───────────────────────
    debug_stats["empresas"] = len(empresa_ids)
    debug_stats["semanas"] = len(SEMANAS)
    debug_stats["talleres"] = len(taller_ids)
    debug_stats["talleres_ef"] = len(taller_ids_ef)
    debug_stats["talleres_it"] = len(taller_ids_it)
    debug_stats["decision_vars_naive"] = len(empresa_ids) * len(SEMANAS) * len(taller_ids)
    debug_stats["total_frecuencias"] = sum(int(empresas[e].get("totalAsignado", 0) or 0) for e in empresa_ids)
    debug_stats["total_slots_available"] = len(SEMANAS) * len(taller_ids)

    print(f"\n{'='*60}")
    print(f"SOLVER DEBUG — {params.trimestre} [OPTIMIZADO V11]")
    print(f"{'='*60}")
    print(f"Empresas con talleres: {debug_stats['empresas']}")
    print(f"Semanas activas: {debug_stats['semanas']} ({SEMANAS})")
    print(f"Talleres: {debug_stats['talleres']} (EF={debug_stats['talleres_ef']}, IT={debug_stats['talleres_it']})")
    print(f"Naive decision variables: {debug_stats['decision_vars_naive']:,}")
    print(f"Total frecuencias a asignar: {debug_stats['total_frecuencias']}")
    print(f"Total slots disponibles (sin festivos): {debug_stats['total_slots_available']}")
    print(f"Días excluidos (festivos): {len(dias_excluidos)} → {sorted(dias_excluidos)[:10]}{'...' if len(dias_excluidos) > 10 else ''}")

    # Check feasibility early
    if debug_stats["total_frecuencias"] > debug_stats["total_slots_available"]:
        warnings.append(
            f"⚠ INFEASIBLE: frecuencias ({debug_stats['total_frecuencias']}) > slots ({debug_stats['total_slots_available']})"
        )
        print(f"❌ EARLY INFEASIBILITY: {debug_stats['total_frecuencias']} frecuencias > {debug_stats['total_slots_available']} slots")
        return {
            "status": "INFEASIBLE",
            "tiempo_segundos": 0,
            "total_slots": 0,
            "total_ef": 0,
            "total_it": 0,
            "slots": [],
            "inviolables_pct": 0,
            "preferentes_pct": 0,
            "warnings": warnings + [f"Total frecuencias ({debug_stats['total_frecuencias']}) excede slots disponibles ({debug_stats['total_slots_available']})"],
            "debug_stats": debug_stats,
        }

    # Restricciones indexadas
    rest_por_empresa: dict[int, list[dict]] = {}
    for r in restricciones:
        eid = r["empresaId"]
        if eid not in rest_por_empresa:
            rest_por_empresa[eid] = []
        rest_por_empresa[eid].append(r)

    # Días disponibles por empresa
    dias_disponibles: dict[int, list[str]] = {}
    for eid in empresa_ids:
        solo_dia = None
        for r in rest_por_empresa.get(eid, []):
            if r["clave"] == "solo_dia":
                solo_dia = r["valor"]
        if solo_dia:
            dias_disponibles[eid] = [solo_dia]
        else:
            dias_disponibles[eid] = disponibilidad_map.get(eid, ["L", "M", "X", "J", "V"])

    # Solo_taller por empresa: prioriza tallerId (FK). Si NULL, fallback al
    # match fuzzy por nombre (compatibilidad con filas legadas pre-V15).
    solo_taller_ids: dict[int, list[int]] = {}
    for eid in empresa_ids:
        for r in rest_por_empresa.get(eid, []):
            if r["clave"] == "solo_taller":
                if r.get("tallerId") is not None:
                    solo_taller_ids.setdefault(eid, []).append(r["tallerId"])
                else:
                    nombre = r["valor"].strip().lower()
                    matching = [
                        t["id"] for t in talleres
                        if nombre in t["nombre"].strip().lower()
                        or t["nombre"].strip().lower() in nombre
                    ]
                    if matching:
                        solo_taller_ids.setdefault(eid, []).extend(matching)

    # No_comodin: empresas excluidas de contingencias (para post-proceso y sugerencias)
    no_comodin_ids: set[int] = set()
    for eid in empresa_ids:
        for r in rest_por_empresa.get(eid, []):
            if r["clave"] == "no_comodin":
                no_comodin_ids.add(eid)

    # Max_extras por empresa (para validación y post-proceso de contingencias)
    max_extras_map: dict[int, int] = {}
    for eid in empresa_ids:
        for r in rest_por_empresa.get(eid, []):
            if r["clave"] == "max_extras":
                try:
                    max_extras_map[eid] = int(r["valor"])
                except ValueError:
                    pass

    if no_comodin_ids:
        warnings.append(
            f"Empresas excluidas de comodín: {sorted(no_comodin_ids)} "
            f"({', '.join(empresas[e]['nombre'] for e in sorted(no_comodin_ids))})"
        )
    if max_extras_map:
        extras_list = [f"{empresas[e]['nombre']}={v}" for e, v in max_extras_map.items()]
        warnings.append(f"Límite de extras: {', '.join(extras_list)}")

    # ═══════════════════════════════════════════════════════════
    # OPTIMIZATION 1: Pre-filter impossible assignments
    # This handles H4, H5, H7, H8 implicitly by NOT creating variables
    # ═══════════════════════════════════════════════════════════

    print(f"\n{'─'*40}")
    print("PRE-FILTERING impossible assignments...")

    # Build a map of available talleres per week (by taller ID)
    talleres_disponibles_semana: dict[int, set[int]] = {}
    taller_map_semana: dict[int, dict[int, dict]] = {}  # semana -> taller_id -> taller_info
    for s in SEMANAS:
        if s in talleres_por_semana:
            talleres_disponibles_semana[s] = {t["id"] for t in talleres_por_semana[s]}
            taller_map_semana[s] = {t["id"]: t for t in talleres_por_semana[s]}
        else:
            # Fallback to base talleres if no per-week config
            talleres_disponibles_semana[s] = set(taller_ids)
            taller_map_semana[s] = taller_map

    possible: set[tuple[int, int, int]] = set()

    for e in empresa_ids:
        dias_ok = set(dias_disponibles[e])
        allowed_talleres = solo_taller_ids.get(e)  # None means all allowed
        is_nueva = empresas[e].get("esNueva", False)
        ef_needed = int(empresas[e].get("talleresEF", 0) or 0)
        it_needed = int(empresas[e].get("talleresIT", 0) or 0)
        rest_empresa = rest_por_empresa.get(e, [])
        # V16.1: hoisted set of HARD-allowed days from franja_por_dia.
        # None means no day-exclusivity (zero HARD franja_por_dia rows).
        dias_permitidos_hard = _dias_exclusivos_hard(rest_empresa)

        for s in SEMANAS:
            # H7: New companies not in weeks 1-4
            if is_nueva and s <= 4:
                debug_stats["H7_filtered"] += len(talleres_disponibles_semana.get(s, taller_ids))
                continue

            # Get talleres available THIS WEEK (from annual calendar)
            talleres_esta_semana = talleres_disponibles_semana.get(s, set(taller_ids))
            taller_info_semana = taller_map_semana.get(s, taller_map)

            for t_id in talleres_esta_semana:
                taller = taller_info_semana.get(t_id, taller_map.get(t_id))
                if not taller:
                    continue

                # H4: Day availability - use the EFFECTIVE day for this week
                effective_day = taller.get("diaSemana") or taller.get("dia_semana")
                if effective_day not in dias_ok:
                    debug_stats["H4_filtered"] += 1
                    continue

                # H8: Festivos - use effective day (could be overridden in intensive weeks)
                if (s, effective_day) in dias_excluidos:
                    debug_stats["H8_filtered"] += 1
                    continue

                # H5: solo_taller
                if allowed_talleres is not None and t_id not in allowed_talleres:
                    debug_stats["H5_filtered"] += 1
                    continue

                # V16.1: HARD day-exclusivity from `franja_por_dia`.
                # If empresa has at least one HARD franja_por_dia row, the
                # declared days become the ONLY valid days. Days outside the
                # set are filtered, regardless of franja match.
                if (
                    dias_permitidos_hard is not None
                    and effective_day not in dias_permitidos_hard
                ):
                    debug_stats["H_franja_dia_filtered"] += 1
                    continue

                # V16: HARD franja_horaria / franja_por_dia pre-filter.
                # If empresa has a HARD franja for this day (or globally) and the
                # taller's horario does not match, drop the candidate. SOFT
                # variants are handled in the penalty pass below.
                franja_pref, tipo_franja = _franja_preferida(rest_empresa, effective_day)
                if (
                    franja_pref is not None
                    and tipo_franja == "HARD"
                    and (taller.get("horario") or "") != franja_pref
                ):
                    debug_stats["H_franja_filtered"] += 1
                    continue

                # Program type match — no point creating var if empresa doesn't need this program
                if taller["programa"] == "EF" and ef_needed == 0:
                    continue
                if taller["programa"] == "IT" and it_needed == 0:
                    continue

                possible.add((e, s, t_id))

    debug_stats["decision_vars_actual"] = len(possible)
    debug_stats["vars_filtered_out"] = debug_stats["decision_vars_naive"] - debug_stats["decision_vars_actual"]

    print(f"Naive vars: {debug_stats['decision_vars_naive']:,}")
    print(f"Possible vars: {debug_stats['decision_vars_actual']:,}")
    print(f"Filtered out: {debug_stats['vars_filtered_out']:,} ({debug_stats['vars_filtered_out']*100//max(1,debug_stats['decision_vars_naive'])}%)")
    print(f"  H4 (day): {debug_stats['H4_filtered']:,}")
    print(f"  H5 (solo_taller): {debug_stats['H5_filtered']:,}")
    print(f"  H7 (new company): {debug_stats['H7_filtered']:,}")
    print(f"  H8 (festivo): {debug_stats['H8_filtered']:,}")
    print(f"  H_franja (HARD V16): {debug_stats['H_franja_filtered']:,}")
    print(f"  H_franja_dia (HARD V16.1): {debug_stats['H_franja_dia_filtered']:,}")
    print(f"{'─'*40}\n")

    # ─── Helper to get assign var or 0 for impossible ────────
    def get_assign(e: int, s: int, t_id: int):
        """Returns the variable if it exists, or 0 (constant) if impossible."""
        return assign.get((e, s, t_id), 0)

    # ─── Variables de decisión (SOLO para posibles) ──────────
    assign = {}
    for (e, s, t_id) in possible:
        assign[(e, s, t_id)] = model.new_bool_var(f"a_{e}_{s}_{t_id}")

    # ─── HARD CONSTRAINTS ────────────────────────────────────

    # H1. Cada slot (semana, taller) tiene A LO SUMO 1 empresa
    # Only sum over companies that can actually reach this slot
    # UPDATED: iterate over per-week talleres (not global taller_ids)
    for s in SEMANAS:
        talleres_esta_semana = talleres_disponibles_semana.get(s, set(taller_ids))
        taller_info_semana = taller_map_semana.get(s, taller_map)
        for t_id in talleres_esta_semana:
            taller = taller_info_semana.get(t_id, taller_map.get(t_id))
            if not taller:
                continue
            # Skip festivo slots - use effective day for this week
            effective_day = taller.get("diaSemana") or taller.get("dia_semana")
            if (s, effective_day) in dias_excluidos:
                continue
            vars_for_slot = [assign[(e, s, t_id)] for e in empresa_ids if (e, s, t_id) in possible]
            if vars_for_slot:
                model.add(sum(vars_for_slot) <= 1)
                debug_stats["H1_constraints"] += 1

    # H1b. Total asignaciones = suma de frecuencias (todo lo confirmado se coloca)
    total_frecuencias = sum(int(empresas[e].get("totalAsignado", 0) or 0) for e in empresa_ids)
    all_assign_vars = list(assign.values())
    model.add(sum(all_assign_vars) == total_frecuencias)
    print(f"H1: {debug_stats['H1_constraints']} slot constraints + H1b: total={total_frecuencias}")

    # H2. Frecuencia EF por empresa = talleresEF confirmado
    for e in empresa_ids:
        ef_requerido = int(empresas[e].get("talleresEF", 0) or 0)
        ef_vars = [assign[(e, s, t_id)] for s in SEMANAS for t_id in taller_ids_ef if (e, s, t_id) in possible]
        if ef_vars:
            model.add(sum(ef_vars) == ef_requerido)
            debug_stats["H2_constraints"] += 1
        elif ef_requerido > 0:
            # Infeasible: company needs EF but has no possible EF slots
            warnings.append(f"⚠ INFEASIBLE: {empresas[e]['nombre']} necesita {ef_requerido} EF pero no tiene slots posibles")
    print(f"H2: {debug_stats['H2_constraints']} EF frequency constraints")

    # H3. Frecuencia IT por empresa = talleresIT confirmado
    for e in empresa_ids:
        it_requerido = int(empresas[e].get("talleresIT", 0) or 0)
        it_vars = [assign[(e, s, t_id)] for s in SEMANAS for t_id in taller_ids_it if (e, s, t_id) in possible]
        if it_vars:
            model.add(sum(it_vars) == it_requerido)
            debug_stats["H3_constraints"] += 1
        elif it_requerido > 0:
            # Infeasible: company needs IT but has no possible IT slots
            warnings.append(f"⚠ INFEASIBLE: {empresas[e]['nombre']} necesita {it_requerido} IT pero no tiene slots posibles")
    print(f"H3: {debug_stats['H3_constraints']} IT frequency constraints")

    # H4, H5, H7, H8 are handled implicitly by pre-filtering (no variables created)
    print(f"H4, H5, H7, H8: handled by pre-filtering ({debug_stats['vars_filtered_out']:,} vars eliminated)")

    # H6. Max 1 taller por empresa por semana (planificación base)
    # Excepción: empresas con escuela propia pueden tener hasta 20 (toda la semana)
    for e in empresa_ids:
        max_per_week = 1  # Default: regla inviolable
        total_empresa = int(empresas[e].get("totalAsignado", 0) or 0)
        if total_empresa >= 6:
            max_per_week = 20  # Sin límite práctico (escuela propia)
        for s in SEMANAS:
            week_vars = [assign[(e, s, t_id)] for t_id in taller_ids if (e, s, t_id) in possible]
            if week_vars:
                model.add(sum(week_vars) <= max_per_week)
                debug_stats["H6_constraints"] += 1
    print(f"H6: {debug_stats['H6_constraints']} max-per-week constraints")

    # Log empresas nuevas warning (even though H7 is handled by pre-filter)
    empresas_nuevas = [e for e in empresa_ids if empresas[e].get("esNueva", False)]
    if empresas_nuevas:
        nombres_nuevas = [empresas[e]["nombre"] for e in empresas_nuevas]
        warnings.append(
            f"Empresas nuevas ({len(empresas_nuevas)}): {', '.join(nombres_nuevas)} "
            f"→ programadas a partir de semana 5"
        )

    # ─── SOFT CONSTRAINTS ────────────────────────────────────

    penalties = []

    # S1. Equilibrio mensual: penalizar desequilibrio entre meses
    MONTH_WEEKS = {
        1: [s for s in SEMANAS if 1 <= s <= 4],
        2: [s for s in SEMANAS if 5 <= s <= 9],
        3: [s for s in SEMANAS if 10 <= s <= 13],
    }

    for e in empresa_ids:
        total = int(empresas[e].get("totalAsignado", 0) or 0)
        if total == 0:
            continue
        ideal_per_month = total / 3.0

        for month_num, month_weeks in MONTH_WEEKS.items():
            if not month_weeks:
                continue
            month_vars = [assign[(e, s, t_id)] for s in month_weeks for t_id in taller_ids if (e, s, t_id) in possible]
            if not month_vars:
                continue
            month_total = sum(month_vars)
            excess = model.new_int_var(0, 20, f"excess_{e}_{month_num}")
            model.add(excess >= month_total - int(ideal_per_month + 1))
            penalties.append(excess * params.peso_equilibrio)
            debug_stats["S1_penalties"] += 1
    print(f"S1: {debug_stats['S1_penalties']} equilibrio penalties")

    # S2. Penalizar semanas consecutivas para misma empresa
    # OPTIMIZED: Linear formulation, only for companies with >= 2 talleres
    s2_empresas = 0
    for e in empresa_ids:
        total = int(empresas[e].get("totalAsignado", 0) or 0)
        if total < 2:  # Can't have consecutives with 0-1 talleres
            continue
        s2_empresas += 1
        for i in range(len(SEMANAS) - 1):
            s1 = SEMANAS[i]
            s2 = SEMANAS[i + 1]
            # Solo penalizar si realmente son consecutivas (diferencia = 1)
            if s2 - s1 != 1:
                continue

            # Sum of assignments in both weeks (using only possible vars)
            week1_vars = [assign[(e, s1, t)] for t in taller_ids if (e, s1, t) in possible]
            week2_vars = [assign[(e, s2, t)] for t in taller_ids if (e, s2, t) in possible]

            if not week1_vars or not week2_vars:
                continue  # Can't have both weeks if one has no possible slots

            sum_both = sum(week1_vars) + sum(week2_vars)
            consec_penalty = model.new_int_var(0, 2, f"consec_{e}_{s1}")
            model.add(consec_penalty >= sum_both - 1)
            penalties.append(consec_penalty * params.peso_no_consecutivas)
            debug_stats["S2_penalties"] += 1
    print(f"S2: {debug_stats['S2_penalties']} consecutivas penalties ({s2_empresas} empresas with >=2 talleres)")

    # S3. Turno preferido — use assign vars directly (no new penalty vars needed)
    # OPTIMIZED: Only iterate over possible combinations
    for e in empresa_ids:
        turno_pref = empresas[e].get("turnoPreferido")
        if not turno_pref:
            continue
        for s in SEMANAS:
            for t_id in taller_ids:
                if (e, s, t_id) not in possible:
                    continue  # Skip impossible assignments
                taller = taller_map[t_id]
                if taller.get("turno") and taller["turno"] != turno_pref:
                    penalties.append(assign[(e, s, t_id)] * params.peso_turno_preferido)
                    debug_stats["S3_penalties"] += 1
    print(f"S3: {debug_stats['S3_penalties']} turno preferido penalties [OPTIMIZED]")

    # S4. Intercalar EF/IT en meses distintos
    # OPTIMIZED: Only for companies with EF >= 2 AND IT >= 1
    empresas_mixtas = 0
    for e in empresa_ids:
        ef_total = int(empresas[e].get("talleresEF", 0) or 0)
        it_total = int(empresas[e].get("talleresIT", 0) or 0)

        # Solo aplicar si empresa tiene AMBOS EF >= 2 e IT >= 1
        if ef_total < 2 or it_total < 1:
            continue

        empresas_mixtas += 1

        for month_num, month_weeks in MONTH_WEEKS.items():
            if not month_weeks:
                continue

            # Contar asignaciones EF en este mes (only possible vars)
            ef_vars = [assign[(e, s, t_id)] for s in month_weeks for t_id in taller_ids_ef if (e, s, t_id) in possible]
            # Contar asignaciones IT en este mes (only possible vars)
            it_vars = [assign[(e, s, t_id)] for s in month_weeks for t_id in taller_ids_it if (e, s, t_id) in possible]

            max_ef_per_month = math.ceil(ef_total / 3)
            max_it_per_month = math.ceil(it_total / 3)

            if ef_vars:
                ef_excess = model.new_int_var(0, 20, f"ef_excess_{e}_{month_num}")
                model.add(ef_excess >= sum(ef_vars) - max_ef_per_month)
                penalties.append(ef_excess * params.peso_intercalar_ef_it)
                debug_stats["S4_penalties"] += 1

            if it_vars:
                it_excess = model.new_int_var(0, 20, f"it_excess_{e}_{month_num}")
                model.add(it_excess >= sum(it_vars) - max_it_per_month)
                penalties.append(it_excess * params.peso_intercalar_ef_it)
                debug_stats["S4_penalties"] += 1

    print(f"S4: {debug_stats['S4_penalties']} intercalar EF/IT penalties ({empresas_mixtas} empresas mixtas)")
    if empresas_mixtas > 0:
        warnings.append(f"S4 Intercalar EF/IT: {empresas_mixtas} empresas con ambos programas")

    # S5. Diversidad de talleres — penalizar repetición del mismo taller
    # OPTIMIZED: Only check REACHABLE talleres for each company
    empresas_diversidad = 0
    for e in empresa_ids:
        total = int(empresas[e].get("totalAsignado", 0) or 0)
        # Skip empresas with < 3 talleres (1-2 talleres can't meaningfully repeat anyway)
        if total < 3:
            continue
        # Excluir escuelas propias / alta frecuencia (>=6 talleres) — repiten por diseño
        if total >= 6:
            continue

        empresas_diversidad += 1
        ef = int(empresas[e].get("talleresEF", 0) or 0)
        it = int(empresas[e].get("talleresIT", 0) or 0)

        # Only penalize EF repetition if empresa has 2+ EF talleres
        if ef >= 2:
            # Only check EF talleres this company can actually reach
            reachable_ef = [t_id for t_id in taller_ids_ef
                          if any((e, s, t_id) in possible for s in SEMANAS)]
            for t_id in reachable_ef:
                times_vars = [assign[(e, s, t_id)] for s in SEMANAS if (e, s, t_id) in possible]
                if len(times_vars) <= 1:
                    continue  # Can only visit once, no repetition possible
                times_at_taller = sum(times_vars)
                repeat = model.new_int_var(0, 13, f"rep_{e}_{t_id}")
                model.add(repeat >= times_at_taller - 1)
                penalties.append(repeat * params.peso_diversidad_talleres)
                debug_stats["S5_penalties"] += 1

        # Only penalize IT repetition if empresa has 2+ IT talleres
        if it >= 2:
            # Only check IT talleres this company can actually reach
            reachable_it = [t_id for t_id in taller_ids_it
                          if any((e, s, t_id) in possible for s in SEMANAS)]
            for t_id in reachable_it:
                times_vars = [assign[(e, s, t_id)] for s in SEMANAS if (e, s, t_id) in possible]
                if len(times_vars) <= 1:
                    continue  # Can only visit once, no repetition possible
                times_at_taller = sum(times_vars)
                repeat = model.new_int_var(0, 13, f"rep_{e}_{t_id}")
                model.add(repeat >= times_at_taller - 1)
                penalties.append(repeat * params.peso_diversidad_talleres)
                debug_stats["S5_penalties"] += 1

    print(f"S5: {debug_stats['S5_penalties']} diversidad penalties ({empresas_diversidad} empresas 3-5 talleres) [OPTIMIZED]")
    if empresas_diversidad > 0:
        warnings.append(f"S5 Diversidad talleres: {empresas_diversidad} empresas (excluye escuelas propias >=6 talleres)")

    # ─── V16 ─ SOFT franja_horaria / franja_por_dia penalty ──
    # Mirrors S3 (turno preferido) magnitude — `peso_turno_preferido` —
    # since franja is conceptually a tighter horario preference of the same
    # type. HARD variants are already enforced by the pre-filter above.
    s_franja_empresas = 0
    for e in empresa_ids:
        rest_empresa = rest_por_empresa.get(e, [])
        # Skip empresa entirely if no franja restriction declared
        if not any(r.get("clave") in ("franja_horaria", "franja_por_dia") for r in rest_empresa):
            continue
        s_franja_empresas += 1

        # Penalize only over candidates the empresa can actually take.
        for s in SEMANAS:
            talleres_esta_semana = talleres_disponibles_semana.get(s, set(taller_ids))
            taller_info_semana = taller_map_semana.get(s, taller_map)
            for t_id in talleres_esta_semana:
                if (e, s, t_id) not in possible:
                    continue
                taller = taller_info_semana.get(t_id, taller_map.get(t_id))
                if not taller:
                    continue
                effective_day = taller.get("diaSemana") or taller.get("dia_semana")
                franja_pref, tipo_franja = _franja_preferida(rest_empresa, effective_day)
                if (
                    franja_pref is not None
                    and tipo_franja == "SOFT"
                    and (taller.get("horario") or "") != franja_pref
                ):
                    penalties.append(assign[(e, s, t_id)] * params.peso_turno_preferido)
                    debug_stats["S_franja_penalties"] += 1
    print(
        f"S_franja: {debug_stats['S_franja_penalties']} franja penalties "
        f"({s_franja_empresas} empresas con franja SOFT) [V16, weight=peso_turno_preferido]"
    )
    if s_franja_empresas > 0:
        warnings.append(
            f"S_franja: {s_franja_empresas} empresa(s) con preferencia de franja SOFT"
        )

    # ═══════════════════════════════════════════════════════════
    # OPTIMIZATION 2: Generate solver hints (warm-start)
    # ═══════════════════════════════════════════════════════════

    print(f"\n{'─'*40}")
    print("Generating solver hints...")

    hints = _generate_hints(
        possible=possible,
        empresa_ids=empresa_ids,
        empresas=empresas,
        SEMANAS=SEMANAS,
        taller_ids=taller_ids,
        taller_map=taller_map,
        taller_ids_ef=taller_ids_ef,
        taller_ids_it=taller_ids_it,
    )
    debug_stats["hints_generated"] = len(hints)

    # Apply hints to model
    for (e, s, t_id), val in hints.items():
        if (e, s, t_id) in assign:
            model.add_hint(assign[(e, s, t_id)], val)

    # Also hint 0 for non-hinted possible vars (helps solver)
    for (e, s, t_id) in possible:
        if (e, s, t_id) not in hints:
            model.add_hint(assign[(e, s, t_id)], 0)

    print(f"Hints generated: {debug_stats['hints_generated']} assignments")
    print(f"{'─'*40}\n")

    # ─── Summary before solve ────────────────────────────────
    total_penalties = len(penalties)
    build_time = time.time() - start_time
    print(f"\n{'─'*40}")
    print(f"MODEL SUMMARY")
    print(f"  Variables: {debug_stats['decision_vars_actual']:,} (was {debug_stats['decision_vars_naive']:,})")
    print(f"  Penalties: {total_penalties}")
    print(f"  Hints: {debug_stats['hints_generated']}")
    print(f"  Build time: {build_time:.2f}s")
    print(f"{'─'*40}\n")

    if penalties:
        model.minimize(sum(penalties))

    # ═══════════════════════════════════════════════════════════
    # OPTIMIZATION 3: Solve with solution callback + gap limit
    # ═══════════════════════════════════════════════════════════

    solver = cp_model.CpSolver()
    # Timeout y workers configurables por env var (Render suele ir con menos CPU que local)
    solver_workers = int(os.getenv("SOLVER_WORKERS", "8"))
    solver_timeout = int(os.getenv("SOLVER_TIMEOUT", str(params.timeout_seconds)))
    solver.parameters.max_time_in_seconds = solver_timeout
    solver.parameters.num_workers = solver_workers

    # Accept solutions within 5% of optimal (don't insist on OPTIMAL)
    solver.parameters.relative_gap_limit = 0.05

    # Solution callback for logging progress
    class SolutionCallback(cp_model.CpSolverSolutionCallback):
        def __init__(self):
            super().__init__()
            self.solution_count = 0
            self.best_objective = float('inf')

        def on_solution_callback(self):
            self.solution_count += 1
            obj = self.objective_value
            if obj < self.best_objective:
                self.best_objective = obj
            elapsed = self.wall_time
            print(f"  Solution #{self.solution_count}: objective={obj:.0f}, elapsed={elapsed:.1f}s")

    print(f"Starting solver with timeout={solver_timeout}s, workers={solver_workers}, gap_limit=5%...")
    callback = SolutionCallback()
    status_code = solver.solve(model, callback)
    elapsed = time.time() - start_time

    status_map = {
        cp_model.OPTIMAL: "OPTIMAL",
        cp_model.FEASIBLE: "FEASIBLE",
        cp_model.INFEASIBLE: "INFEASIBLE",
        cp_model.MODEL_INVALID: "INFEASIBLE",
        cp_model.UNKNOWN: "TIMEOUT",
    }
    status = status_map.get(status_code, "TIMEOUT")
    print(f"Solver finished: status={status}, solutions={callback.solution_count}, elapsed={elapsed:.2f}s")

    if status in ("INFEASIBLE", "TIMEOUT"):
        print(f"❌ SOLVER FAILED: {status}")
        print(f"Debug stats: {debug_stats}")
        return {
            "status": status,
            "tiempo_segundos": round(elapsed, 2),
            "total_slots": 0,
            "total_ef": 0,
            "total_it": 0,
            "slots": [],
            "inviolables_pct": 0,
            "preferentes_pct": 0,
            "warnings": [f"Solver terminó con status: {status}. Revisar restricciones."] + warnings,
            "debug_stats": debug_stats,
        }

    # ─── Extraer solución ────────────────────────────────────
    # UPDATED: iterate over per-week talleres (not global taller_ids)
    # This respects intensive weeks where some talleres are OFF or have different day/horario

    slots_raw: list[dict] = []
    vacios = 0
    festivos_skipped = 0
    total_slots_posibles = 0

    for s in SEMANAS:
        talleres_esta_semana = talleres_disponibles_semana.get(s, set(taller_ids))
        taller_info_semana = taller_map_semana.get(s, taller_map)

        for t_id in talleres_esta_semana:
            taller = taller_info_semana.get(t_id, taller_map.get(t_id))
            if not taller:
                continue

            # Use effective day/horario for this specific week
            effective_day = taller.get("diaSemana") or taller.get("dia_semana")
            effective_horario = taller.get("horario", "")
            effective_turno = taller.get("turno", "")

            # Skip festivo slots — they don't exist in the calendar
            if (s, effective_day) in dias_excluidos:
                festivos_skipped += 1
                continue

            total_slots_posibles += 1
            assigned = False
            for e in empresa_ids:
                # Check dict membership directly — don't compare BoolVar to int
                if (e, s, t_id) in assign and solver.value(assign[(e, s, t_id)]) == 1:
                    slots_raw.append({
                        "semana": s,
                        "dia": effective_day,
                        "horario": effective_horario,
                        "turno": effective_turno,
                        "empresa_id": e,
                        "empresa_nombre": empresas[e]["nombre"],
                        "programa": taller["programa"],
                        "taller_id": t_id,
                        "taller_nombre": taller["nombre"],
                    })
                    assigned = True
                    break  # Only one company per slot (H1 constraint)

            if not assigned:
                vacios += 1
                # Incluir slot vacío para que el frontend lo muestre
                slots_raw.append({
                    "semana": s,
                    "dia": effective_day,
                    "horario": effective_horario,
                    "turno": effective_turno,
                    "empresa_id": 0,
                    "empresa_nombre": "— Vacante —",
                    "programa": taller["programa"],
                    "taller_id": t_id,
                    "taller_nombre": taller["nombre"],
                })
    if vacios > 0:
        warnings.append(
            f"{vacios}/{total_slots_posibles} slots vacantes "
            f"({round(vacios/total_slots_posibles*100)}%). "
            f"Añadir más empresas o aumentar frecuencias para llenar."
        )

    # Ordenar por semana, día, horario
    DIA_ORD = {"L": 1, "M": 2, "X": 3, "J": 4, "V": 5}
    slots_raw.sort(key=lambda x: (
        x["semana"],
        DIA_ORD.get(x["dia"], 9),
        x["horario"],
    ))

    # Métricas
    total_penalty = solver.objective_value if penalties else 0
    max_possible = max(1, len(empresa_ids) * params.semanas * params.peso_equilibrio)
    preferentes_pct = round(
        (1 - total_penalty / max_possible) * 100, 1
    )

    print(f"\n{'='*60}")
    print(f"SOLVER SUCCESS — {status}")
    print(f"  Time: {elapsed:.2f}s")
    print(f"  Solutions found: {callback.solution_count}")
    print(f"  Objective: {total_penalty:.0f}")
    print(f"  Slots: {len(slots_raw)} ({vacios} vacantes)")
    print(f"{'='*60}\n")

    return {
        "status": status,
        "tiempo_segundos": round(elapsed, 2),
        "total_slots": len(slots_raw),
        "total_ef": sum(1 for s in slots_raw if s["programa"] == "EF"),
        "total_it": sum(1 for s in slots_raw if s["programa"] == "IT"),
        "slots": slots_raw,
        "inviolables_pct": 100.0,
        "preferentes_pct": max(0, preferentes_pct),
        "warnings": warnings,
        "debug_stats": debug_stats,
    }
