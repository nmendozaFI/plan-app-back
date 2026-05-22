-- ============================================================
-- V25 Capa 10 — Seed catálogo contratantes
-- Cambio C, decisión C7 (RESUMEN_CONTEXTO_V25.md §10).
-- ============================================================
-- Marca taller.esContratante = true para los 5 talleres del catálogo
-- contratante. La regla H_contratante del solver (Capa 7) garantiza que
-- empresas con empresa.esContratante = true cubran target_cat = min(freq, 5)
-- asignaciones dentro de este catálogo antes de tocar el general.
--
-- Sin orden estricto dentro del catálogo (decisión V25 Q2 — orden libre
-- entre los 5).
--
-- Catálogo objetivo (V25 §10 C7):
--   1. Mi CV
--   2. Mi marca personal (práctica)
--   3. Afrontar una entrevista de trabajo
--   4. Superar una dinámica grupal
--   5. Practicando la entrevista de trabajo (Role Play)
-- ============================================================


-- ============================================================
-- PARTE 1: SELECT de validación (correr PRIMERO)
-- ============================================================
-- Match flexible por nombre (ILIKE) para localizar los talleres
-- candidatos en BD aunque varíen ligeramente respecto al catálogo V25.
--
-- Output esperado: 5 filas, una por taller del catálogo, todas con
-- esContratante = false (estado pre-seed).
--   - Más filas que las del catálogo → revisar manualmente la ambigüedad
--     antes de marcar.
--   - Menos filas → algún taller no existe con ese nombre en BD; no
--     descomentar la PARTE 2 hasta resolver.

SELECT id, nombre, programa, "esContratante"
FROM taller
WHERE
  nombre ILIKE '%Mi CV%'
  OR nombre ILIKE '%marca personal%'
  OR nombre ILIKE '%entrevista de trabajo%'
  OR nombre ILIKE '%dinámica grupal%'
  OR nombre ILIKE '%dinamica grupal%'   -- variante sin tilde por seguridad
  OR nombre ILIKE '%Role Play%'
ORDER BY nombre;


-- ============================================================
-- PARTE 2: UPDATE de marcado
-- (correr SOLO después de validar los IDs en el SELECT anterior —
--  DESCOMENTAR PARA EJECUTAR)
-- ============================================================
-- IDs resueltos contra BD (5 filas, ambas variantes "teoría" y "práctica"
-- de "Mi CV, mi marca personal" según decisión del usuario). Si el SELECT
-- de la PARTE 1 devuelve IDs distintos en tu entorno, ajustar antes de
-- descomentar.
--
-- Idempotente: re-ejecutarlo no cambia el resultado (sigue siendo true).

-- UPDATE taller SET "esContratante" = true
-- WHERE id IN (
--   10,  -- Mi CV, mi marca personal (teoría)
--   11,  -- Mi CV, mi marca personal (práctica)
--   14,  -- Afrontar una entrevista de trabajo
--   16,  -- Superar una dinámica grupal
--   17   -- Practicando la entrevista de trabajo (Role Play)
-- );


-- ============================================================
-- PARTE 3: Verificación post-UPDATE
-- (descomentar tras correr la PARTE 2)
-- ============================================================
-- Esperado: 5 filas, las del catálogo, todas con esContratante = true.

-- SELECT id, nombre, programa, "esContratante"
-- FROM taller
-- WHERE "esContratante" = true
-- ORDER BY nombre;
