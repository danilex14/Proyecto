-- PREGUNTA 1: clusters de sismos M >= 6.0 someros (< 30 km) y densidad por celdas.
-- Ejecutar:  Get-Content queries\q1_clusters.sql | docker exec -i sismos-postgis psql -U sismos -d sismos
--
-- Corre sobre todo el histórico (catálogo mundial). Para limitarlo a Chile, agrega en los WHERE:
--     AND geom && ST_MakeEnvelope(-76, -56, -66, -17, 4326)
\timing on

-- (a) Densidad por celdas hexagonales de 0.5 grados.
-- Cada punto busca SU hexágono (ST_HexagonGrid acotado al punto), en vez de generar la grilla
-- completa sobre la extensión de los datos: a escala mundial eso serían ~10^5-10^6 celdas vacías.
WITH f AS (
    SELECT geom, magnitud FROM sismos
    WHERE magnitud >= 6.0 AND profundidad_km < 30 AND fuente <> 'sim'
)
SELECT h.i, h.j,
       count(*)                           AS n_sismos,
       round(avg(f.magnitud)::numeric, 2) AS mag_media,
       round(max(f.magnitud)::numeric, 1) AS mag_max,
       ST_AsText(ST_Centroid(h.geom))     AS centro_celda
FROM f
CROSS JOIN LATERAL ST_HexagonGrid(0.5, ST_SetSRID(f.geom, 4326)) h
WHERE ST_Intersects(h.geom, f.geom)
GROUP BY h.i, h.j, h.geom
ORDER BY n_sismos DESC
LIMIT 20;

-- (b) Clusters formales con DBSCAN espacial (eps = 0.5 grados, mínimo 5 eventos)
WITH f AS (
    SELECT id, geom, magnitud, lugar,
           ST_ClusterDBSCAN(geom, eps := 0.5, minpoints := 5) OVER () AS cluster_id
    FROM sismos
    WHERE magnitud >= 6.0 AND profundidad_km < 30 AND fuente <> 'sim'
)
SELECT cluster_id,
       count(*)                                  AS n_sismos,
       round(avg(magnitud)::numeric, 2)          AS mag_media,
       round(max(magnitud)::numeric, 1)          AS mag_max,
       ST_AsText(ST_Centroid(ST_Collect(geom)))  AS centroide,
       (array_agg(lugar ORDER BY magnitud DESC))[1] AS lugar_mayor
FROM f
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id
ORDER BY n_sismos DESC
LIMIT 25;
