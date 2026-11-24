#!/usr/bin/env python3
"""Simulador de eventos sísmicos (reproducible).

Modos:
  normal     sismicidad de fondo continua (Gutenberg-Richter) a --rate eventos/s
  swarm      enjambre: varios eventos M>=4 concentrados en una zona (dispara la pregunta 2)
  mainshock  terremoto M>=6.5 seguido de réplicas con ley de Omori (dispara la pregunta 3); productividad
             calibrada con el catálogo real (calibrate_aftershocks.py)
  replay     re-emite eventos REALES del catálogo (PostGIS) en orden de tiempo, acelerados: flujo continuo
             con datos reales (escenarios iquique2014, illapel2015, maule2010 o rango/recuadro propio)

Destinos (--sink):  stdout | redis (XADD a sismos:in) | http (POST JSON a NiFi ListenHTTP)

Ejemplos:
  python simulator.py normal --rate 80 --sink redis
  python simulator.py swarm --sink redis
  python simulator.py mainshock --mag 7.2 --compress 60 --sink redis
  python simulator.py replay --escenario iquique2014 --compress 3600 --sink redis
"""
import argparse
import json
import math
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from common import (AFT_NB_K, AFT_PROD_A, AFT_PROD_ALPHA, OMORI_C7_S, OMORI_C_GAMMA, STREAM_IN,
                    radio_km)

# (lat, lon, sigma en grados): zonas sísmicas típicas de Chile
ZONES = [
    (-20.0, -70.0, 1.0), (-23.0, -70.5, 1.0), (-27.0, -70.8, 1.0),
    (-30.0, -71.5, 1.0), (-33.0, -72.0, 1.2), (-35.8, -72.7, 1.0),
    (-37.5, -73.5, 1.0), (-42.0, -74.0, 1.2),
]


M4_POR_DIA = 2.3      # sismos M>=4 por día en Chile (21.543 en el catálogo USGS 2000-2025)


def gr_mag(mmin=2.5, mmax=8.8, b=1.0):
    """Magnitud ~ Gutenberg-Richter truncada."""
    while True:
        m = mmin - math.log10(1.0 - random.random()) / b
        if m <= mmax:
            return round(m, 1)


def random_depth():
    return round(random.uniform(5, 30) if random.random() < 0.6 else random.uniform(30, 150), 1)


def background_location():
    lat, lon, sg = random.choice(ZONES)
    return round(random.gauss(lat, sg), 3), round(random.gauss(lon, sg), 3)


def make_event(ts: datetime, mag, lat, lon, depth, fuente="sim", lugar=None):
    return {
        "event_id": uuid.uuid4().hex,
        "ts": ts.isoformat(),
        "magnitud": mag,
        "profundidad_km": depth,
        "lat": lat,
        "lon": lon,
        "lugar": lugar or "simulado",
        "fuente": fuente,
    }


def omori_times(n, duration, c=300.0):
    """n tiempos (s) con densidad ~ 1/(t+c) en [0, duration] (Omori, p=1), ordenados."""
    return sorted(c * ((duration / c + 1.0) ** random.random() - 1.0) for _ in range(n))


def omori_c(m):
    return OMORI_C7_S * 10 ** (OMORI_C_GAMMA * (m - 7))


# Las réplicas se reparten con una gaussiana 2D de sigma = R/2 (R = radio de impacto): una fracción
# 1 - e^-2 = 86,5 % cae dentro de R, que es donde el pipeline (y la calibración) las cuenta.
INSIDE_FRAC = 1 - math.exp(-2)


def expected_aftershocks(m, mmin=3.0, duration=86400.0, b=1.0):
    """Número esperado de réplicas M>=mmin a generar en [0, duration], calibrado con el catálogo real.

    La calibración da las M>=4 en 24 h dentro de la zona de impacto; se pasa a M>=mmin con la
    Gutenberg-Richter truncada en la réplica máxima (Båth, M-1.2), a la duración con Omori y se
    compensa la fracción que cae fuera de la zona.
    """
    n24_m4 = 10 ** (AFT_PROD_A + AFT_PROD_ALPHA * (m - 7))
    mmax = m - 1.2
    span = 1 - 10 ** (-b * (mmax - mmin))
    m4 = 3.95                                     # gr_mag redondea a 0.1: desde 3.95 ya cuenta como M4.0
    frac_ge4 = max((10 ** (-b * (m4 - mmin)) - 10 ** (-b * (mmax - mmin))) / span, 1e-6) if mmax > m4 else 1e-6
    c = omori_c(m)
    t_frac = math.log1p(duration / c) / math.log1p(86400.0 / c)
    return n24_m4 / frac_ge4 * t_frac / INSIDE_FRAC


def poisson(lam):
    if lam < 30:                                  # Knuth
        L, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= random.random()
            if p <= L:
                return k
            k += 1
    return max(0, round(random.gauss(lam, math.sqrt(lam))))


def aftershock_plan(main_event, mmin=3.0, duration=86400.0, b=1.0, variability="media"):
    """Lista [(segundos_virtuales, evento)] de réplicas. Ley de Båth: réplica mayor ~ M-1.2.

    variability="media": genera exactamente el número esperado (demo reproducible, "secuencia típica").
    variability="real":  binomial negativa (gamma-Poisson) con la dispersión del catálogo: hay secuencias
                         muy pobres y otras muy productivas, como en la realidad.
    """
    m = main_event["magnitud"]
    mmax = m - 1.2
    mu = expected_aftershocks(m, mmin, duration, b)
    if variability == "real":
        n = poisson(mu * random.gammavariate(AFT_NB_K, 1 / AFT_NB_K))
    else:
        n = round(mu)
    t0 = datetime.fromisoformat(main_event["ts"])
    sigma_lat = 0.5 * radio_km(m) / 111.0
    sigma_lon = sigma_lat / max(math.cos(math.radians(main_event["lat"])), 0.05)   # grados de lon más cortos
    plan = []
    for t in omori_times(n, duration, c=omori_c(m)):
        lat = round(random.gauss(main_event["lat"], sigma_lat), 3)
        lon = round(random.gauss(main_event["lon"], sigma_lon), 3)
        ev = make_event(t0 + timedelta(seconds=t), gr_mag(mmin, mmax, b), lat, lon,
                        round(random.uniform(5, 35), 1), lugar="replica simulada")
        plan.append((t, ev))
    return plan


class Sink:
    """Destino de los eventos.

    En modo http los eventos se agrupan en un arreglo JSON por POST (hasta `http_batch` eventos o
    cada `http_flush_s` segundos, lo que ocurra primero). Motivo: cada POST a ListenHTTP cuesta
    ~50 ms casi sin importar su tamaño, así que de a 1 evento el techo es ~20 ev/s.
    El flujo de NiFi acepta tanto un objeto como un arreglo. Con --http-batch 1 se envía de a uno.
    """

    def __init__(self, kind, url=None, http_batch=50, http_flush_s=0.25):
        self.kind = kind
        self.r = self.s = None
        self.url = url
        self.http_batch = max(1, http_batch)
        self.buf = []
        self.posts = 0
        if kind == "redis":
            from common import redis_client
            self.r = redis_client()
        elif kind == "http":
            import threading
            import requests
            self.s = requests.Session()
            self.lock = threading.Lock()        # protege el buffer
            self.post_lock = threading.Lock()   # la Session no es thread-safe: un POST a la vez
            self.stop = threading.Event()
            if self.http_batch > 1:
                def flusher():
                    while not self.stop.wait(http_flush_s):
                        self.flush()
                threading.Thread(target=flusher, daemon=True).start()

    def send(self, ev):
        if self.kind == "stdout":
            print(json.dumps(ev, ensure_ascii=False), flush=True)
        elif self.kind == "redis":
            flat = {k: ("" if v is None else str(v)) for k, v in ev.items()}
            self.r.xadd(STREAM_IN, flat, maxlen=500000, approximate=True)
        elif self.kind == "http":
            if self.http_batch == 1:
                self.s.post(self.url, json=ev, timeout=5)
                self.posts += 1
                return
            with self.lock:
                self.buf.append(ev)
                full = len(self.buf) >= self.http_batch
            if full:
                self.flush()

    def flush(self):
        with self.lock:                    # el POST va fuera del lock: no frena al generador
            batch, self.buf = self.buf, []
        if batch:
            with self.post_lock:
                self.s.post(self.url, json=batch, timeout=10)
                self.posts += 1

    def close(self):
        if self.kind == "http":
            self.stop.set()
            self.flush()
            print(f"[http] {self.posts} POST a {self.url}", file=sys.stderr)


def now():
    return datetime.now(timezone.utc)


def run_normal(sink, rate, duration):
    end = time.time() + duration if duration else float("inf")
    n = 0
    # Carga de volumen: microsismicidad M2.5-3.9 a `rate` ev/s, y los M>=4 a la tasa real de Chile
    # (M4_POR_DIA, medida en el catálogo USGS 2000-2025). Si los M>=4 salieran de la misma
    # Gutenberg-Richter, a 80 ev/s habría ~2,4 por SEGUNDO (la realidad es ~2,3 por DÍA) y cada celda
    # dispararía alertas de enjambre falsas todo el tiempo.
    p_m4 = M4_POR_DIA / 86400.0 / rate
    while time.time() < end:
        lat, lon = background_location()
        mag = gr_mag(4.0, 6.4) if random.random() < p_m4 else gr_mag(2.5, 3.9)
        sink.send(make_event(now(), mag, lat, lon, random_depth()))
        n += 1
        time.sleep(random.expovariate(rate))
    print(f"[normal] {n} eventos enviados", file=sys.stderr)


def run_swarm(sink, duration, n_events, lat, lon):
    """n_events con M>=4.0 en una misma zona repartidos en `duration` segundos reales."""
    print(f"[swarm] {n_events} eventos M>=4 cerca de ({lat},{lon}) en {duration}s", file=sys.stderr)
    for _ in range(n_events):
        sink.send(make_event(now(), round(random.uniform(4.0, 5.2), 1),
                             round(random.gauss(lat, 0.05), 3), round(random.gauss(lon, 0.05), 3),
                             round(random.uniform(5, 25), 1), lugar="enjambre simulado"))
        time.sleep(duration / n_events)


def run_mainshock(sink, mag, lat, lon, compress, max_real_s, variability="media"):
    main_ev = make_event(now(), mag, lat, lon, 20.0, lugar="mainshock simulado")
    print(f"[mainshock] M{mag} en ({lat},{lon}) id={main_ev['event_id']}", file=sys.stderr)
    sink.send(main_ev)
    plan = aftershock_plan(main_ev, variability=variability)
    r = radio_km(mag)
    from common import haversine_km
    n1h = sum(1 for t, ev in plan if t <= 3600 and ev["magnitud"] >= 4.0
              and haversine_km(lat, lon, ev["lat"], ev["lon"]) <= r)
    print(f"[mainshock] réplicas M>=4 en la 1a hora dentro de la zona de impacto: {n1h} "
          f"(calibrado con el catálogo real; variabilidad={variability})", file=sys.stderr)
    print(f"[mainshock] {len(plan)} réplicas planificadas, compresión x{compress} "
          f"(1 h virtual = {3600 / compress:.0f} s reales)", file=sys.stderr)
    start = time.time()
    for t_virtual, ev in plan:
        due = start + t_virtual / compress
        if due - start > max_real_s:
            break
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)
        sink.send(ev)
    print("[mainshock] fin de la emisión", file=sys.stderr)


# --------------------------------------------------------------------------- replay del catálogo real
# Secuencias reales de Chile para la demo (recuadro lat_min, lat_max, lon_min, lon_max)
SCENARIOS = {
    "iquique2014": dict(desde="2014-03-01", hasta="2014-04-10", bbox=(-22.0, -18.0, -72.5, -69.0),
                        desc="precursores (16-mar M6.7), terremoto de Iquique M8.2 (1-abr) y réplica M7.7 (3-abr)"),
    "illapel2015": dict(desde="2015-09-01", hasta="2015-09-25", bbox=(-33.0, -29.5, -73.5, -70.5),
                        desc="terremoto de Illapel M8.3 (16-sep-2015) y sus réplicas"),
    "maule2010": dict(desde="2010-02-20", hasta="2010-03-15", bbox=(-39.0, -32.5, -75.0, -70.5),
                      desc="terremoto del Maule M8.8 (27-feb-2010) y sus réplicas"),
}


def load_replay(desde, hasta, bbox, minmag):
    """Eventos reales del catálogo en PostGIS, ordenados por tiempo, en el formato del stream.
    Conservan su event_id ('usgs:...'), así al volver a escribirse en PostGIS no se duplican."""
    from common import pg_conn
    la1, la2, lo1, lo2 = bbox
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT event_id, ts, magnitud, profundidad_km, lat, lon, lugar, fuente FROM sismos
                       WHERE fuente <> 'sim' AND ts >= %s AND ts < %s AND magnitud >= %s
                         AND lat BETWEEN %s AND %s AND lon BETWEEN %s AND %s
                       ORDER BY ts""", (desde, hasta, minmag, la1, la2, lo1, lo2))
        rows = cur.fetchall()
    return [{"event_id": r[0], "ts": r[1].isoformat(), "magnitud": round(float(r[2]), 2),
             "profundidad_km": None if r[3] is None else round(float(r[3]), 2), "lat": r[4], "lon": r[5],
             "lugar": r[6], "fuente": r[7]} for r in rows]


def run_replay(sink, events, compress, max_gap):
    """Emite eventos reales respetando sus intervalos (divididos por `compress`); ningún silencio dura más
    de `max_gap` segundos reales. Las ventanas del pipeline usan tiempo de evento, así que acortar los
    silencios no cambia ningún resultado: solo hace que 40 días quepan en una demo de minutos."""
    if not events:
        print("[replay] no hay eventos en ese rango", file=sys.stderr)
        return
    t_first = datetime.fromisoformat(events[0]["ts"])
    t_last = datetime.fromisoformat(events[-1]["ts"])
    print(f"[replay] {len(events)} eventos reales del {t_first:%Y-%m-%d} al {t_last:%Y-%m-%d} "
          f"(compresión x{compress:g}, silencios de máx. {max_gap:g} s)", file=sys.stderr)
    prev, t0 = None, time.time()
    for ev in events:
        ts = datetime.fromisoformat(ev["ts"])
        if prev is not None:
            time.sleep(min((ts - prev).total_seconds() / compress, max_gap))
        prev = ts
        sink.send(ev)
        if ev["magnitud"] >= 6.0:
            print(f"[replay] {ts:%Y-%m-%d %H:%M} M{ev['magnitud']}  {ev['lugar'] or ''}", file=sys.stderr)
    print(f"[replay] fin: {len(events)} eventos en {time.time() - t0:.0f} s reales", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["normal", "swarm", "mainshock", "replay"])
    ap.add_argument("--sink", choices=["stdout", "redis", "http"], default="stdout")
    ap.add_argument("--url", default="http://localhost:8081/sismos", help="URL de NiFi ListenHTTP (sink http)")
    ap.add_argument("--rate", type=float, default=80.0, help="eventos/s (modo normal)")
    ap.add_argument("--duration", type=float, default=60.0, help="segundos (normal: 0 = infinito; swarm: duración)")
    ap.add_argument("--events", type=int, default=12, help="eventos M>=4 del enjambre")
    ap.add_argument("--mag", type=float, default=7.0, help="magnitud del mainshock")
    ap.add_argument("--lat", type=float, default=-35.8)
    ap.add_argument("--lon", type=float, default=-72.7)
    ap.add_argument("--compress", type=float, default=60.0, help="mainshock/replay: factor de compresión del tiempo")
    ap.add_argument("--max-real", type=float, default=300.0, help="mainshock: máximo de segundos reales a emitir")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--variabilidad", choices=["media", "real"], default="media",
                    help="mainshock: 'media' = secuencia típica; 'real' = dispersión del catálogo (binomial negativa)")
    ap.add_argument("--escenario", choices=list(SCENARIOS), help="replay: secuencia real predefinida")
    ap.add_argument("--desde", help="replay: fecha inicial (ISO), si no se usa --escenario")
    ap.add_argument("--hasta", help="replay: fecha final (ISO, excluida)")
    ap.add_argument("--bbox", help="replay: lat_min,lat_max,lon_min,lon_max")
    ap.add_argument("--minmag", type=float, default=2.5, help="replay: magnitud mínima")
    ap.add_argument("--max-gap", type=float, default=0.5, help="replay: segundos reales máx. de silencio entre eventos")
    ap.add_argument("--http-batch", type=int, default=50, help="sink http: máx. eventos por POST (1 = de a uno)")
    ap.add_argument("--http-flush", type=float, default=0.25, help="sink http: segundos máx. que espera un evento")
    a = ap.parse_args()

    if a.seed is not None:
        random.seed(a.seed)
    sink = Sink(a.sink, a.url, a.http_batch, a.http_flush)
    try:
        if a.mode == "normal":
            run_normal(sink, a.rate, a.duration)
        elif a.mode == "swarm":
            run_swarm(sink, a.duration, a.events, a.lat, a.lon)
        elif a.mode == "mainshock":
            run_mainshock(sink, a.mag, a.lat, a.lon, a.compress, a.max_real, a.variabilidad)
        else:
            if a.escenario:
                sc = SCENARIOS[a.escenario]
                print(f"[replay] escenario {a.escenario}: {sc['desc']}", file=sys.stderr)
                desde, hasta, bbox = sc["desde"], sc["hasta"], sc["bbox"]
            elif a.desde and a.hasta and a.bbox:
                desde, hasta, bbox = a.desde, a.hasta, tuple(float(x) for x in a.bbox.split(","))
            else:
                ap.error("replay necesita --escenario o bien --desde, --hasta y --bbox")
            run_replay(sink, load_replay(desde, hasta, bbox, a.minmag), a.compress, a.max_gap)
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()


if __name__ == "__main__":
    main()
