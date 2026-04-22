from typing import Literal

from pydantic import BaseModel


# V17: planificacion.estado dropped "OK". CONFIRMADO is the terminal state.
# VACANTE is kept in the user-settable literal because actualizar_slot uses it
# to mark a slot as vacant when the empresa is cleared (existing behavior).
EstadoSlotInput = Literal["PLANIFICADO", "CONFIRMADO", "CANCELADO", "VACANTE"]


class CalendarioInput(BaseModel):
    trimestre: str
    timeout_seconds: int = 120
    max_ef: int = 14
    max_it: int = 6
    semanas: int = 13
    peso_equilibrio: int = 10
    peso_no_consecutivas: int = 8
    peso_turno_preferido: int = 3
    peso_intercalar_ef_it: int = 6      # S4: Intercalar EF/IT en meses distintos
    peso_diversidad_talleres: int = 4   # S5: Penalizar repetición del mismo taller


class SugerenciaContingencia(BaseModel):
    empresa_id: int
    empresa_nombre: str
    motivo: str
    prioridad: int


class SlotCalendario(BaseModel):
    id: int | None = None
    semana: int
    dia: str
    horario: str
    turno: str
    empresa_id: int | None  # nullable for vacancies
    empresa_nombre: str | None  # nullable for vacancies
    programa: str
    taller_id: int
    taller_nombre: str
    ciudad_id: int | None
    ciudad: str | None
    tipo_asignacion: str
    estado: str = "PLANIFICADO"  # PLANIFICADO | CONFIRMADO | OK | CANCELADO | VACANTE
    confirmado: bool = False
    notas: str | None = None
    sugerencias: list[SugerenciaContingencia] | None = None


class CalendarioOutput(BaseModel):
    trimestre: str
    status: str
    tiempo_segundos: float
    total_slots: int
    total_ef: int
    total_it: int
    slots: list[SlotCalendario]
    inviolables_pct: float
    preferentes_pct: float
    warnings: list[str]


class SlotUpdateInput(BaseModel):
    """Input for updating a single slot."""
    estado: EstadoSlotInput | None = None  # V17: "OK" rejected with 422
    confirmado: bool | None = None
    empresa_id: int | None = None  # Can be null to clear (make vacancy)
    notas: str | None = None
    motivo_cambio: str | None = None  # "EMPRESA_CANCELO" | "DECISION_PLANIFICADOR"


class SlotBatchUpdateItem(BaseModel):
    """Single item in a batch update."""
    slot_id: int
    estado: EstadoSlotInput | None = None  # V17: "OK" rejected with 422
    confirmado: bool | None = None
    empresa_id: int | None = None
    notas: str | None = None
    motivo_cambio: str | None = None  # "EMPRESA_CANCELO" | "DECISION_PLANIFICADOR"


class SlotBatchUpdateInput(BaseModel):
    """Input for batch updating multiple slots."""
    updates: list[SlotBatchUpdateItem]


class ValidarAsignacionInput(BaseModel):
    """Input for validating a company assignment to a slot."""
    slot_id: int
    empresa_id: int


class ValidarAsignacionResult(BaseModel):
    """Result of validating a company assignment."""
    ok: bool  # True if no warnings
    warnings: list[str]
    restricciones_violadas: list[str]  # e.g. ["solo_dia: EY solo puede Viernes, slot es Martes"]


class EmpresaAnalisis(BaseModel):
    """Per-company analysis metrics."""
    empresa_id: int
    empresa_nombre: str
    asignados_solver: int
    cumplidos: int
    sustituida: int
    cancelados: int
    pendientes: int
    extras_cubiertos: int
    tasa_cumplimiento: float
    tasa_sustitucion: float
    sugerencia: str  # REDUCIR | REVISAR | MANTENER | SOLO_COMODIN


class CambioSlot(BaseModel):
    """Detail of a slot where company was substituted."""
    semana: int
    dia: str
    taller: str
    programa: str
    empresa_original: str
    empresa_final: str


class AnalisisResumen(BaseModel):
    """Global summary metrics."""
    total_slots_asignados: int
    cumplidos_sin_cambio: int
    sustituidos: int
    cancelados: int
    pendientes: int
    tasa_cumplimiento_global: float
    tasa_sustitucion_global: float


class AnalisisResponse(BaseModel):
    """Full analysis response."""
    trimestre: str
    resumen: AnalisisResumen
    por_empresa: list[EmpresaAnalisis]
    cambios: list[CambioSlot]
    total_empresas: int


class CerrarTrimestreInput(BaseModel):
    confirmar: bool = False  # Si es False, hace dry run (preview)


class EmpresaCambiada(BaseModel):
    """Detalle de una empresa que cambió en un slot."""
    slot_id: int
    semana: int
    dia: str
    taller_nombre: str
    empresa_anterior: str | None
    empresa_nueva: str


class CambioDetalle(BaseModel):
    """Detalle de un cambio detectado en un slot (estado, confirmado o empresa)."""
    slot_id: int
    semana: int
    dia: str
    taller_nombre: str
    empresa_nombre: str | None
    campo: str  # "estado" | "confirmado" | "empresa"
    valor_anterior: str
    valor_nuevo: str


class ImportarExcelResult(BaseModel):
    """Resultado de importar Excel editado."""
    trimestre: str
    total_procesados: int
    actualizados: int
    sin_cambios: int
    errores: int
    empresas_cambiadas: list[EmpresaCambiada]
    cambios_detalle: list[CambioDetalle]
    warnings: list[str]


class ImportarExcelInput(BaseModel):
    """Input for dry_run mode."""
    dry_run: bool = False


class RecalcularScoresResult(BaseModel):
    empresas_actualizadas: int
    detalle: list[dict]
    warnings: list[str]


class CerrarTrimestreResult(BaseModel):
    trimestre: str
    total_ok: int
    total_cancelado: int
    total_ignorado: int  # VACANTE + PLANIFICADO slots
    preview: bool
    scores_actualizados: int = 0
    score_warnings: list[str] = []
