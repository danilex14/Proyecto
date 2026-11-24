"""Utilidades compartidas por los benchmarks (Paso 8)."""
import csv
import math
import os
import random
from datetime import datetime, timedelta, timezone

import simulator as sim
from common import radio_km

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "..", "resultados")
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def gen_stream(n, rate=1.0, seed=42, swarm_every_s=1800, swarm_events=12, swarm_span_s=300, max_mag=6.4):
    """Secuencia reproducible de n eventos ordenados por tiempo de evento.

    Fondo Gutenberg-Richter a `rate` ev/s (tiempo virtual) + un enjambre inyectado cada
    `swarm_every_s` (12 eventos M4.0-5.2 en 5 min). M < 6.5 para no disparar mainshocks.
    """
    random.seed(seed)
    evs, t, next_swarm = [], 0.0, swarm_every_s
    while len(evs) < n:
        t += random.expovariate(rate)
        if t >= next_swarm:
            lat0, lon0, _ = random.choice(sim.ZONES)
            lat0, lon0 = lat0 + random.uniform(-1, 1), lon0 + random.uniform(-0.5, 0.5)
            for k in range(swarm_events):
                ts = T0 + timedelta(seconds=next_swarm + k * swarm_span_s / swarm_events)
                evs.append(sim.make_event(ts, round(random.uniform(4.0, 5.2), 1),
                                          round(random.gauss(lat0, 0.05), 3), round(random.gauss(lon0, 0.05), 3),
                                          round(random.uniform(5, 25), 1), fuente="bench", lugar="enjambre bench"))
            next_swarm += swarm_every_s
        lat, lon = sim.background_location()
        evs.append(sim.make_event(T0 + timedelta(seconds=t), min(sim.gr_mag(), max_mag), lat, lon,
                                  sim.random_depth(), fuente="bench"))
    evs = sorted(evs[:n], key=lambda e: e["ts"])
    for i, e in enumerate(evs):                 # ids reproducibles
        e["event_id"] = f"bench-{i:07d}"
    return evs


def sample_epicenters(conn, n, seed=7, mag=7.0):
    """n epicentros reales (ubicación y profundidad de sismos USGS M>=4.5) tratados como un M`mag` nuevo, "ahora"."""
    with conn.cursor() as cur:
        cur.execute("""SELECT lat, lon, profundidad_km FROM sismos WHERE fuente = 'usgs' AND magnitud >= 4.5
                       ORDER BY md5(event_id || %s) LIMIT %s""", (str(seed), n))
        rows = cur.fetchall()
    conn.rollback()
    ts = datetime.now(timezone.utc).timestamp()
    return [{"event_id": f"bench-main-{i}", "ts_epoch": ts, "magnitud": mag, "lat": la, "lon": lo,
             "profundidad_km": d, "radio_km": radio_km(mag)} for i, (la, lo, d) in enumerate(rows)]


def pct(values, p):
    """Percentil p (0-100) con interpolación lineal."""
    v = sorted(values)
    if not v:
        return float("nan")
    k = (len(v) - 1) * p / 100
    f = math.floor(k)
    c = min(f + 1, len(v) - 1)
    return v[f] + (v[c] - v[f]) * (k - f)


def save_csv(name, rows):
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, name)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"-> {os.path.relpath(path, os.path.join(HERE, '..'))}")
    return path


def print_table(rows):
    cols = list(rows[0].keys())
    fmt = lambda v: f"{v:.2f}" if isinstance(v, float) else str(v)
    width = {c: max(len(c), *(len(fmt(r[c])) for r in rows)) for c in cols}
    print("  ".join(c.rjust(width[c]) for c in cols))
    for r in rows:
        print("  ".join(fmt(r[c]).rjust(width[c]) for c in cols))
