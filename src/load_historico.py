#!/usr/bin/env python3
"""Carga un catálogo histórico CSV (CSN o USGS) en PostGIS.

Reconoce nombres de columna típicos de USGS (time, latitude, longitude, depth, mag, place, id)
y de CSN (fecha/hora, latitud, longitud, profundidad, magnitud, referencia...).

  python load_historico.py ../data/usgs_chile.csv --fuente usgs
"""
import argparse
import hashlib
import io

import pandas as pd

from common import pg_conn

ALIASES = {
    "ts":    ["time", "fecha", "fecha (utc)", "fecha_utc", "fecha utc", "datetime", "date"],
    "lat":   ["latitude", "latitud", "lat"],
    "lon":   ["longitude", "longitud", "lon"],
    "depth": ["depth", "profundidad", "profundidad_km", "profundidad (km)", "depth_km"],
    "mag":   ["mag", "magnitud", "magnitude", "magnitud (ml)"],
    "place": ["place", "lugar", "referencia", "epicentro", "ubicacion", "ubicación"],
    "id":    ["id", "event_id", "eventid"],
}


def pick(df, key):
    for name in ALIASES[key]:
        if name in df.columns:
            return name
    return None


def normalize(path, fuente):
    df = pd.read_csv(path, low_memory=False)
    df.columns = [str(c).strip().lower() for c in df.columns]

    c_ts, c_lat, c_lon, c_mag = pick(df, "ts"), pick(df, "lat"), pick(df, "lon"), pick(df, "mag")
    missing = [n for n, c in (("fecha", c_ts), ("latitud", c_lat), ("longitud", c_lon), ("magnitud", c_mag)) if not c]
    if missing:
        raise SystemExit(f"No encuentro columnas para: {missing}. Columnas del archivo: {list(df.columns)}")
    c_dep, c_place, c_id = pick(df, "depth"), pick(df, "place"), pick(df, "id")

    ts_raw = df[c_ts].astype(str)
    if "hora" in df.columns and c_ts == "fecha":          # CSN con fecha y hora separadas
        ts_raw = ts_raw + " " + df["hora"].astype(str)
    out = pd.DataFrame({
        "ts": pd.to_datetime(ts_raw, utc=True, errors="coerce"),
        "magnitud": pd.to_numeric(df[c_mag], errors="coerce"),
        "profundidad_km": pd.to_numeric(df[c_dep], errors="coerce") if c_dep else None,
        "lat": pd.to_numeric(df[c_lat], errors="coerce"),
        "lon": pd.to_numeric(df[c_lon], errors="coerce"),
        "lugar": df[c_place] if c_place else None,
        "fuente": fuente,
    })
    n0 = len(out)
    out = out.dropna(subset=["ts", "magnitud", "lat", "lon"])
    out = out[out["lat"].between(-90, 90) & out["lon"].between(-180, 180)]

    if c_id:
        out["event_id"] = fuente + ":" + df.loc[out.index, c_id].astype(str)
    else:
        out["event_id"] = [hashlib.md5(f"{t}|{la}|{lo}|{m}".encode()).hexdigest()
                           for t, la, lo, m in zip(out["ts"], out["lat"], out["lon"], out["magnitud"])]
    out = out.drop_duplicates("event_id")
    print(f"Filas leídas: {n0} | válidas y sin duplicados: {len(out)}")
    return out[["event_id", "ts", "magnitud", "profundidad_km", "lat", "lon", "lugar", "fuente"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--fuente", default="csn", help="csn | usgs")
    a = ap.parse_args()

    df = normalize(a.csv, a.fuente)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    conn = pg_conn()
    with conn, conn.cursor() as cur:
        cur.execute("""CREATE TEMP TABLE stg (event_id text, ts timestamptz, magnitud real,
                       profundidad_km real, lat double precision, lon double precision,
                       lugar text, fuente text) ON COMMIT DROP""")
        cur.copy_expert("COPY stg FROM STDIN WITH (FORMAT csv)", buf)
        cur.execute("""INSERT INTO sismos (event_id, ts, magnitud, profundidad_km, lat, lon, lugar, fuente)
                       SELECT event_id, ts, magnitud, profundidad_km, lat, lon, lugar, fuente FROM stg
                       ON CONFLICT (event_id) DO NOTHING""")
        print(f"Insertadas {cur.rowcount} filas nuevas en sismos")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("ANALYZE sismos")
    conn.close()


if __name__ == "__main__":
    main()
