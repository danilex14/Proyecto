#!/usr/bin/env python3
"""Benchmark 3: el sistema (Redis + PostGIS) contra Pandas / GeoPandas / Python en memoria.

Parte A, consulta histórica (Pregunta 3): para los mismos epicentros, ¿qué sismos análogos hubo
(a <= R km, M +-dM, misma clase de profundidad) y cuántas réplicas M>=4 tuvo cada uno en 1 h?
  * PostGIS: la consulta de cross_query.py (ST_DWithin geography + GiST; réplicas con ST_Intersects)
  * GeoPandas: índice STRtree de epicentros + refinamiento haversine; réplicas con los buffers de PostGIS
  * NumPy: fuerza bruta, distancia haversine vectorizada contra todo el histórico
  Se reporta el tiempo de preparación (cargar datos en memoria) aparte del tiempo por consulta y
  cuánto coinciden los resultados con PostGIS.

Parte B, ventana deslizante en streaming (Pregunta 2): la misma secuencia de eventos (con
enjambres inyectados) por cuatro implementaciones de la regla ">5 eventos M>=4 en WINDOW_S (60 min) por celda":
  * Redis: el loop real de stream_processor.py (XREADGROUP de a 200 + handle_event con Sorted Sets)
    en la base 15 de Redis (no toca los datos del pipeline)
  * Pandas por micro-lotes de 200 (concat + groupby + searchsorted)
  * Pandas por evento (concat y filtro en cada llegada; sobre un subconjunto, es lento)
  * Python puro en memoria (dict de deques): cota inferior, sin estado compartido ni durabilidad
  Se reporta eventos/s y si las alertas coinciden con las de Redis.

  python bench_pandas.py --epicentros 200 --eventos 20000
"""
import argparse
import json
import time
from collections import defaultdict, deque

import numpy as np
import pandas as pd
import redis
import shapely

import stream_processor as sp
from bench_common import gen_stream, pct, print_table, sample_epicenters, save_csv
from common import (AFT_MIN_MAG, AFT_WINDOW_S, REDIS_HOST, REDIS_PORT, SWARM_MIN_MAG, SWARM_THRESHOLD,
                    WINDOW_S, cell_of, parse_ts, pg_conn)
from cross_query import depth_class, historical_analogs

R_EARTH = 6371.0088


def hav(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * R_EARTH * np.arcsin(np.sqrt(a))


# =============================================================== Parte A
def depth_class_arr(d):
    """Igual que cross_query.depth_class: 0 (<70 km), 1 (70-300), 2 (>300); -1 si no hay dato."""
    out = np.full(len(d), -1, int)
    ok = ~np.isnan(d)
    out[ok] = np.where(d[ok] < 70, 0, np.where(d[ok] < 300, 1, 2))
    return out


def load_hist(conn):
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("""SELECT extract(epoch FROM ts), magnitud, lat, lon, radio_km, profundidad_km,
                              ST_AsBinary(buffer_geom)
                       FROM sismos WHERE fuente <> 'sim' ORDER BY ts""")
        rows = cur.fetchall()
    conn.rollback()
    t_load = time.perf_counter() - t0
    H = {"ts": np.array([r[0] for r in rows], float), "mag": np.array([r[1] for r in rows], float),
         "lat": np.array([r[2] for r in rows], float), "lon": np.array([r[3] for r in rows], float),
         "radio": np.array([r[4] for r in rows], float),
         "dclass": depth_class_arr(np.array([np.nan if r[5] is None else r[5] for r in rows], float))}
    t1 = time.perf_counter()
    import geopandas as gpd
    buffers = gpd.GeoSeries(shapely.from_wkb([bytes(r[6]) for r in rows]), crs=4326)   # zonas de impacto
    points = gpd.GeoSeries(shapely.points(H["lon"], H["lat"]), crs=4326)
    pindex = points.sindex                   # STRtree (R-Tree empaquetado) de epicentros
    pindex.query(shapely.Point(0, 0))        # fuerza la construcción
    t_geo = time.perf_counter() - t1
    return H, buffers, pindex, t_load, t_geo


def aftershock_counts(H, idx, inside):
    """Réplicas M>=AFT_MIN_MAG en (ts, ts+1h] dentro de la zona de cada histórico idx.
    H está ordenado por ts, así que la ventana temporal es un rango contiguo (searchsorted)."""
    out = []
    for i in idx:
        lo = np.searchsorted(H["ts"], H["ts"][i], "right")
        hi = np.searchsorted(H["ts"], H["ts"][i] + AFT_WINDOW_S, "right")
        sl = np.arange(lo, hi)
        sl = sl[H["mag"][sl] >= AFT_MIN_MAG]
        out.append(int(inside(i, sl).sum()) if len(sl) else 0)
    return out


def analog_filter(H, idx, info, radius, dmag):
    """Filtro de análogos sobre candidatos idx (mismo criterio que cross_query.SQL_HIST)."""
    d = hav(info["lat"], info["lon"], H["lat"][idx], H["lon"][idx])
    ok = (d <= radius) & (np.abs(H["mag"][idx] - info["magnitud"]) <= dmag + 1e-6) & (H["ts"][idx] < info["ts_epoch"])
    dc = depth_class(info.get("profundidad_km"))
    if dc is not None:
        ok &= H["dclass"][idx] == dc
    idx = idx[ok]
    # mismo orden que el SQL: magnitud desc y, a igual magnitud (float4 en PostgreSQL), ts desc
    return idx[np.lexsort((-H["ts"][idx], -H["mag"][idx].astype(np.float32)))][:50]


def q_numpy(H, info, radius, dmag):
    idx = analog_filter(H, np.arange(len(H["ts"])), info, radius, dmag)     # fuerza bruta: 1 M distancias
    return aftershock_counts(H, idx, lambda i, sl: hav(H["lat"][i], H["lon"][i], H["lat"][sl], H["lon"][sl])
                             <= H["radio"][i])


def search_boxes(lat, lon, radius):
    """Cajas lon/lat que cubren el círculo (2 si cruza el antimeridiano)."""
    dlat = radius / 111.0
    dlon = min(180.0, radius / (111.0 * max(np.cos(np.radians(min(abs(lat) + dlat, 89.9))), 1e-6)))
    s, n = max(-90, lat - dlat), min(90, lat + dlat)
    boxes = [shapely.box(max(-180, lon - dlon), s, min(180, lon + dlon), n)]
    if lon - dlon < -180:
        boxes.append(shapely.box(lon - dlon + 360, s, 180, n))
    if lon + dlon > 180:
        boxes.append(shapely.box(-180, s, lon + dlon - 360, n))
    return boxes


def q_geopandas(H, buffers, pindex, info, radius, dmag):
    cand = np.unique(np.concatenate([pindex.query(b) for b in search_boxes(info["lat"], info["lon"], radius)]))
    idx = analog_filter(H, cand, info, radius, dmag)                       # refinamiento exacto
    geoms = buffers.values
    return aftershock_counts(H, idx, lambda i, sl: shapely.intersects_xy(geoms[i], H["lon"][sl], H["lat"][sl]))


def part_a(conn, n, crits):
    epis = sample_epicenters(conn, n)
    H, buffers, pindex, t_load, t_geo = load_hist(conn)
    print(f"[A] {len(H['ts'])} históricos en memoria: carga desde PostGIS {t_load*1000:.0f} ms, "
          f"buffers + STRtree {t_geo*1000:.0f} ms")
    rows = []
    for radius, dmag in crits:
        crit = f"{radius:.0f} km, M±{dmag}"
        res = {}
        for name, fn in (("postgis", lambda e: [r[5] for r in historical_analogs(conn, e, radius, dmag)[0]]),
                         ("geopandas", lambda e: q_geopandas(H, buffers, pindex, e, radius, dmag)),
                         ("numpy", lambda e: q_numpy(H, e, radius, dmag))):
            fn(epis[0])                                   # calentamiento
            times, outs = [], []
            for e in epis:
                t0 = time.perf_counter()
                outs.append(fn(e))
                times.append((time.perf_counter() - t0) * 1000)
            res[name] = (times, outs)
        ref = res["postgis"][1]
        for name, (times, outs) in res.items():
            same_n = sum(len(o) == len(r) for o, r in zip(outs, ref))
            same_rep = sum(sum(o) == sum(r) for o, r in zip(outs, ref))
            rows.append({"metodo": name, "criterio": crit, "n": len(epis), "ms_p50": pct(times, 50),
                         "ms_p95": pct(times, 95), "ms_media": sum(times) / len(times),
                         "analogos_media": sum(len(o) for o in outs) / len(outs),
                         "coincide_analogos_%": 100 * same_n / len(epis),
                         "coincide_replicas_%": 100 * same_rep / len(epis),
                         "prep_ms": 0.0 if name == "postgis" else (t_load + (t_geo if name == "geopandas" else 0)) * 1000})
    return rows


# =============================================================== Parte B
class Dedupe:
    """Misma regla anti-repetición que stream_processor: una alerta por celda cada WINDOW_S."""
    def __init__(self):
        self.last, self.alerts = {}, []

    def check(self, cell, ts, count):
        if count > SWARM_THRESHOLD:
            last = self.last.get(cell)
            if last is None or ts - last > WINDOW_S:
                self.last[cell] = ts
                self.alerts.append((cell, round(ts, 3)))


def stream_redis(events):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=15, decode_responses=True)
    r.flushdb()
    p = r.pipeline(transaction=False)
    for e in events:
        p.xadd(sp.STREAM_IN, {k: ("" if v is None else str(v)) for k, v in e.items()})
    p.execute()
    sp.ensure_group(r)

    alerts = []
    orig_alert = sp.alert

    def bench_alert(rc, channel, payload):            # igual que sp.alert pero sin imprimir ni Pub/Sub global
        rc.xadd(sp.STREAM_ALERTS, {"channel": channel, "payload": json.dumps(payload)})
        alerts.append((tuple(payload["cell"]), round(parse_ts(payload["ts"]), 3)))
    sp.alert = bench_alert
    try:
        active, done, clock = {}, 0, 0.0
        t0 = time.perf_counter()
        while done < len(events):
            resp = r.xreadgroup(sp.GROUP, "bench", {sp.STREAM_IN: ">"}, count=200, block=100)
            if not resp:
                continue
            ids = []
            for _s, msgs in resp:
                for mid, f in msgs:
                    clock = max(clock, sp.handle_event(r, f, active))
                    ids.append(mid)
            r.set(sp.KEY_CLOCK, clock)
            r.xack(sp.STREAM_IN, sp.GROUP, *ids)
            done += len(ids)
        dt = time.perf_counter() - t0
    finally:
        sp.alert = orig_alert
        r.flushdb()
    return dt, alerts


def to_frame(events):
    df = pd.DataFrame(events)
    df["t"] = [parse_ts(s) for s in df["ts"]]
    ij = [cell_of(a, b) for a, b in zip(df["lat"], df["lon"])]
    df["ci"], df["cj"] = [c[0] for c in ij], [c[1] for c in ij]
    return df


def stream_pandas_microbatch(df, size=200):
    win = df.iloc[0:0][["ci", "cj", "t"]]
    dd = Dedupe()
    t0 = time.perf_counter()
    for start in range(0, len(df), size):
        b = df.iloc[start:start + size]
        b = b.loc[b["magnitud"] >= SWARM_MIN_MAG, ["ci", "cj", "t"]]
        if b.empty:
            continue
        win = pd.concat([win, b], ignore_index=True)
        win = win[win["t"] > b["t"].iloc[0] - WINDOW_S]          # descarta lo que ya no puede contar
        groups = {k: g["t"].to_numpy() for k, g in win.groupby(["ci", "cj"], sort=False)}
        for ci, cj, t in b.itertuples(index=False):
            ts = groups[(ci, cj)]
            cnt = np.searchsorted(ts, t, "right") - np.searchsorted(ts, t - WINDOW_S, "right")
            dd.check((ci, cj), t, int(cnt))
    return time.perf_counter() - t0, dd.alerts


def stream_pandas_per_event(df):
    win = df.iloc[0:0][["ci", "cj", "t"]]
    dd = Dedupe()
    t0 = time.perf_counter()
    for row in df.itertuples(index=False):
        if row.magnitud < SWARM_MIN_MAG:
            continue
        win = pd.concat([win, pd.DataFrame([[row.ci, row.cj, row.t]], columns=["ci", "cj", "t"])],
                        ignore_index=True)
        win = win[win["t"] > row.t - WINDOW_S]
        cnt = int(((win["ci"] == row.ci) & (win["cj"] == row.cj)).sum())
        dd.check((row.ci, row.cj), row.t, cnt)
    return time.perf_counter() - t0, dd.alerts


def stream_python(events):
    win = defaultdict(deque)
    dd = Dedupe()
    t0 = time.perf_counter()
    for e in events:
        if float(e["magnitud"]) < SWARM_MIN_MAG:
            continue
        t = parse_ts(e["ts"])
        cell = cell_of(float(e["lat"]), float(e["lon"]))
        q = win[cell]
        q.append(t)
        while q and q[0] <= t - WINDOW_S:
            q.popleft()
        dd.check(cell, t, len(q))
    return time.perf_counter() - t0, dd.alerts


def part_b(n, n_per_event):
    events = gen_stream(n, seed=2)
    df = to_frame(events)                    # convertir a DataFrame no se cuenta (es preparación)
    print(f"[B] {n} eventos ({(df['magnitud'] >= SWARM_MIN_MAG).sum()} con M>=4), "
          f"{(df['t'].iloc[-1] - df['t'].iloc[0]) / 3600:.1f} h de tiempo de evento")
    out = {}
    print("[B] redis ...", flush=True)
    out["redis (stream_processor)"] = (n, *stream_redis(events))
    print("[B] pandas micro-lotes ...", flush=True)
    out["pandas micro-lotes 200"] = (n, *stream_pandas_microbatch(df))
    print(f"[B] pandas por evento ({n_per_event} eventos) ...", flush=True)
    out["pandas por evento"] = (n_per_event, *stream_pandas_per_event(df.iloc[:n_per_event]))
    print("[B] python en memoria ...", flush=True)
    out["python dict+deque"] = (n, *stream_python(events))

    ref_full = out["redis (stream_processor)"][2]
    t_cut = df["t"].iloc[n_per_event - 1]
    ref_cut = [a for a in ref_full if a[1] <= t_cut]
    rows = []
    for name, (m, dt, alerts) in out.items():
        ref = ref_cut if m < n else ref_full
        rows.append({"implementacion": name, "eventos": m, "segundos": dt, "eventos_por_s": m / dt,
                     "us_por_evento": dt * 1e6 / m, "alertas": len(alerts),
                     "coincide_con_redis": sorted(alerts) == sorted(ref)})
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--epicentros", type=int, default=200)
    ap.add_argument("--criterios", default="150:0.5,300:1.0", help="radio_km:dmag de los análogos")
    ap.add_argument("--eventos", type=int, default=20000)
    ap.add_argument("--eventos-por-evento", type=int, default=3000, help="subconjunto para Pandas por evento")
    ap.add_argument("--partes", default="A,B", help="A = consulta histórica, B = ventana en streaming")
    a = ap.parse_args()
    partes = a.partes.upper().split(",")

    if "B" in partes and "A" not in partes:
        rows_b = part_b(a.eventos, a.eventos_por_evento)
        print()
        print_table(rows_b)
        save_csv("pandas_streaming.csv", rows_b)
        return
    conn = pg_conn()
    rows_a = part_a(conn, a.epicentros, [(float(c.split(":")[0]), float(c.split(":")[1]))
                                         for c in a.criterios.split(",")])
    print()
    print_table(rows_a)
    save_csv("pandas_consulta.csv", rows_a)

    rows_b = part_b(a.eventos, a.eventos_por_evento)
    print()
    print_table(rows_b)
    save_csv("pandas_streaming.csv", rows_b)


if __name__ == "__main__":
    main()
