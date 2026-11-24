-- Esquema geoespacial del proyecto (se ejecuta automáticamente al crear el contenedor)
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS sismos (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT        NOT NULL UNIQUE,
    ts              TIMESTAMPTZ NOT NULL,
    magnitud        REAL        NOT NULL,
    profundidad_km  REAL,
    lat             DOUBLE PRECISION NOT NULL,
    lon             DOUBLE PRECISION NOT NULL,
    lugar           TEXT,
    fuente          TEXT        DEFAULT 'csn',   -- 'csn' | 'usgs' | 'sim'
    -- Las siguientes 3 columnas las calcula el trigger de abajo:
    geom            geometry(Point, 4326),
    radio_km        DOUBLE PRECISION,
    buffer_geom     geometry(MultiPolygon, 4326)   -- 2 partes si cruza el antimeridiano
);

-- Zona de impacto: radio = longitud de ruptura subsuperficial (Wells & Coppersmith, 1994,
-- todos los tipos de falla):  log10(L_km) = -2.44 + 0.59 * M
-- Es un SUPUESTO DE MODELADO: justifícalo (y cita la fuente) en el informe.
-- El búfer se calcula sobre geography (metros reales), así no hay que elegir huso UTM
-- (Chile cruza los husos 18S y 19S).
--
-- Antimeridiano: si el círculo cruza los ±180° (Fiji, Tonga, Aleutianas...), el cast a geometry da
-- un polígono inválido que "da la vuelta al mundo" (xmin=-180, xmax=180) y contiene falsamente
-- puntos de toda esa franja de latitud. En ese caso se corre a longitudes 0..360, se corta en 180°
-- y la parte sobrante vuelve al lado negativo: un MultiPolygon válido de 2 partes.
CREATE OR REPLACE FUNCTION sismos_buffer(g geometry, r_km double precision) RETURNS geometry AS $$
DECLARE b geometry;
BEGIN
    b := ST_Buffer(g::geography, r_km * 1000, 16)::geometry;
    IF ST_XMax(b) - ST_XMin(b) > 180 THEN
        b := ST_WrapX(ST_CollectionExtract(ST_Split(ST_ShiftLongitude(b),
                 ST_SetSRID(ST_MakeLine(ST_MakePoint(180, -90), ST_MakePoint(180, 90)), 4326)), 3), 180, -360);
    END IF;
    RETURN ST_Multi(b);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION sismos_set_geoms() RETURNS trigger AS $$
BEGIN
    NEW.geom        := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
    NEW.radio_km    := POWER(10::double precision, -2.44 + 0.59 * NEW.magnitud::double precision);
    NEW.buffer_geom := sismos_buffer(NEW.geom, NEW.radio_km);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sismos_geoms ON sismos;
CREATE TRIGGER trg_sismos_geoms
    BEFORE INSERT ON sismos
    FOR EACH ROW EXECUTE FUNCTION sismos_set_geoms();

-- Índices espaciales (GiST = R-Tree) y auxiliares
CREATE INDEX IF NOT EXISTS idx_sismos_geom   ON sismos USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_sismos_buffer ON sismos USING GIST (buffer_geom);
-- Pregunta 3 (análogos a <= R km): ST_DWithin sobre geography usa este índice de expresión
-- (distancia geodésica en metros, correcta también cruzando el antimeridiano)
CREATE INDEX IF NOT EXISTS idx_sismos_geog   ON sismos USING GIST ((geom::geography));
CREATE INDEX IF NOT EXISTS idx_sismos_ts     ON sismos (ts);
CREATE INDEX IF NOT EXISTS idx_sismos_mag    ON sismos (magnitud);

-- Informes del cruce (pregunta 3): guarda lo que calcula cross_query.py
CREATE TABLE IF NOT EXISTS informes_mainshock (
    event_id    TEXT PRIMARY KEY,
    creado_en   TIMESTAMPTZ DEFAULT now(),
    latencia_ms DOUBLE PRECISION,
    informe     JSONB
);
