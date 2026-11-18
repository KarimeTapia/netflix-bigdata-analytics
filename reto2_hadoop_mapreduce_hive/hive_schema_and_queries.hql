-- =============================================================================
-- RETO 2: ESQUEMA EXTERNO Y CONSULTAS HQL COMPLEJAS EN APACHE HIVE
-- =============================================================================
-- Este archivo contiene las sentencias HiveQL (HQL) requeridas para:
-- 1. Crear una tabla externa sobre el directorio de HDFS (/data/netflix/raw/).
-- 2. Ejecutar 3 consultas analíticas complejas demostrando el uso de:
--    - GROUP BY y funciones de agregación.
--    - LATERAL VIEW explode(split(...)) para desarticular campos multivaluados.
--    - Funciones de ventana (Window Functions) como ROW_NUMBER() / DENSE_RANK()
--      para calcular el Top 5 de géneros más populares por país.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS netflix_analytics;
USE netflix_analytics;

-- -----------------------------------------------------------------------------
-- 1. CREACIÓN DE TABLA EXTERNA SOBRE HDFS
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS netflix_external;

CREATE EXTERNAL TABLE IF NOT EXISTS netflix_external (
    show_id         STRING,
    type            STRING,
    title           STRING,
    director        STRING,
    cast            STRING,
    country         STRING,
    date_added      STRING,
    release_year    INT,
    rating          STRING,
    duration        STRING,
    listed_in       STRING,
    description     STRING,
    duration_min    INT,
    seasons_count   INT,
    year_added      INT,
    month_added     INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/data/netflix/raw/'
TBLPROPERTIES ("skip.header.line.count"="1");


-- -----------------------------------------------------------------------------
-- 2. CONSULTA HQL COMPLEJA 1:
-- Desarticulación de países con LATERAL VIEW explode y conteo por tipo de contenido.
-- -----------------------------------------------------------------------------
-- Explicación:
-- Una película producida por "United States, United Kingdom" se desagrega
-- para contabilizar el impacto real e independiente de cada territorio.
-- -----------------------------------------------------------------------------
SELECT 
    TRIM(c.single_country) AS pais,
    t.type AS tipo_contenido,
    COUNT(*) AS total_titulos,
    AVG(t.duration_min) AS duracion_promedio_min
FROM netflix_external t
LATERAL VIEW explode(split(t.country, ',')) c AS single_country
WHERE TRIM(c.single_country) != '' 
  AND TRIM(c.single_country) != 'Unknown Country'
GROUP BY TRIM(c.single_country), t.type
ORDER BY total_titulos DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 3. CONSULTA HQL COMPLEJA 2:
-- Análisis de co-ocurrencia: directores más prolíficos por país desarticulado.
-- -----------------------------------------------------------------------------
SELECT 
    TRIM(c.single_country) AS pais,
    t.director AS director,
    COUNT(*) AS titulos_dirigidos,
    MIN(t.release_year) AS primer_estreno,
    MAX(t.release_year) AS ultimo_estreno
FROM netflix_external t
LATERAL VIEW explode(split(t.country, ',')) c AS single_country
WHERE t.director != 'Unknown Director' 
  AND TRIM(c.single_country) != 'Unknown Country'
GROUP BY TRIM(c.single_country), t.director
HAVING COUNT(*) >= 5
ORDER BY titulos_dirigidos DESC
LIMIT 25;


-- -----------------------------------------------------------------------------
-- 4. CONSULTA HQL COMPLEJA 3 (REQUISITO PRINCIPAL):
-- Top 5 de géneros más populares por país de origen mediante
-- doble LATERAL VIEW explode y Función de Ventana ROW_NUMBER().
-- -----------------------------------------------------------------------------
-- Explicación técnica:
-- 1) Subconsulta CTE 'exploded_catalog': Aplica split y explode simultáneo a
--    'country' y 'listed_in' para normalizar la relación muchos a muchos.
-- 2) Subconsulta 'genre_rankings': Agrupa por país y género, calculando
--    la frecuencia y asignando una jerarquía particionada por país con
--    ROW_NUMBER() OVER (PARTITION BY pais ORDER BY total_titulos DESC).
-- 3) Filtro final: Selecciona estrictamente los registros con ranking <= 5.
-- -----------------------------------------------------------------------------
WITH exploded_catalog AS (
    SELECT 
        TRIM(c.single_country) AS pais,
        TRIM(g.single_genre) AS genero
    FROM netflix_external t
    LATERAL VIEW explode(split(t.country, ',')) c AS single_country
    LATERAL VIEW explode(split(t.listed_in, ',')) g AS single_genre
    WHERE TRIM(c.single_country) != '' 
      AND TRIM(c.single_country) != 'Unknown Country'
      AND TRIM(g.single_genre) != ''
),
genre_aggregated AS (
    SELECT 
        pais,
        genero,
        COUNT(*) AS total_titulos
    FROM exploded_catalog
    GROUP BY pais, genero
),
ranked_genres AS (
    SELECT 
        pais,
        genero,
        total_titulos,
        ROW_NUMBER() OVER (
            PARTITION BY pais 
            ORDER BY total_titulos DESC
        ) AS ranking_popularidad
    FROM genre_aggregated
)
SELECT 
    pais,
    ranking_popularidad,
    genero,
    total_titulos
FROM ranked_genres
WHERE ranking_popularidad <= 5
ORDER BY pais ASC, ranking_popularidad ASC;
