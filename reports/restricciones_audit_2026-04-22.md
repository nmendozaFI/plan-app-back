# Auditoría de restricciones — 2026-04-22

Total restricciones en BD: **6**  
Empresas activas: **80**

Este reporte es **solo lectura**. No se modifican datos. Las correcciones sugeridas se emiten como SQL comentado al final para revisión manual.

## A. Current state

| id | empresa | tipo | clave | valor | tallerId | taller (FK) | descripcion |
|---:|---------|------|-------|-------|---------:|-------------|-------------|
| 6 | AENA | SOFT | max_extras | 4 | — | — | — |
| 5 | B.SANTANDER | SOFT | solo_taller | Cómo gestionar mis primeros ingresos | — | — | — |
| 4 | BBVA | SOFT | max_extras | 1 | — | — | BBVA: máximo 1 extra por trimestre |
| 3 | EULEN | SOFT | no_comodin | true | — | — | Eulen cierra mal, no usar como comodín |
| 1 | EY | HARD | solo_dia | V | — | — | EY solo imparte en viernes |
| 2 | GARRIGUES | HARD | solo_taller | Mis derechos y obligaciones como empleado | — | — | Garrigues laboral → solo D&O (verificar cada Q) |

## B. Orphan rows (missing description) — needs review by planner

| id | empresa | tipo | clave | valor |
|---:|---------|------|-------|-------|
| 6 | AENA | SOFT | max_extras | 4 |
| 5 | B.SANTANDER | SOFT | solo_taller | Cómo gestionar mis primeros ingresos |

_Acción sugerida: el planificador revisa cada fila y añade descripción explicativa (origen: documento oficial, decisión, etc.)._

## C. Missing restrictions per planner doc

- ⚠ No se encontró ninguna empresa con 'SANTANDER FXM' o 'FXM' en el nombre. Verificar nomenclatura.
- ❌ **Telefónica → solo_dia='X' MISSING (presente en Excel maestro pero no en BD).** Empresas detectadas: TELEFONICA.

## D. Suggested SQL fixes (commented out — review before running)

```sql
-- Suggested fix for Telefónica:
-- INSERT INTO restriccion ("empresaId", tipo, clave, valor, descripcion)
-- SELECT id, 'HARD', 'solo_dia', 'X',
--        'Telefónica solo miércoles (del Excel maestro)'
-- FROM empresa WHERE UPPER(nombre) LIKE 'TELEFONICA%';

```
