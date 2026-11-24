#!/usr/bin/env python3
"""Procesador de streaming sobre Redis (familia Streaming/NoSQL).

Lee el Redis Stream `sismos:in` con un consumer group y:
  * Pregunta 2: mantiene un Sorted Set por celda (score = timestamp del evento) con los
    eventos M>=4.0 de los últimos 60 min (WINDOW_S) de TIEMPO DE EVENTO. Si hay más de 5 -> alerta de
    enjambre por Pub/Sub (canal alerts:swarm).
  * Pregunta 3 (parte Redis): si llega un evento M>=6.5 publica alerta en alerts:mainshock y
    empieza a contar sus réplicas (Sorted Set aft:<event_id>).
"""
import json
import socket
import time

from common import (AFT_MIN_MAG, AFT_TRACK_S, CH_MAIN, CH_SWARM, KEY_CLOCK, MAIN_MAG, STREAM_ALERTS,
                    STREAM_IN, SWARM_MIN_MAG, SWARM_THRESHOLD, WINDOW_S, cell_center, cell_of,
                    haversine_km, parse_ts, radio_km, redis_client)

GROUP = "proc"
CONSUMER = f"proc-{socket.gethostname()}"


def ensure_group(r):
    from redis.exceptions import ResponseError
    try:
        r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def load_active_mainshocks(r):
    active = {}
    for eid in r.smembers("mainshocks:active"):
        raw = r.get(f"main:{eid}")
        if raw:
            active[eid] = json.loads(raw)
    return active


def alert(r, channel, payload):
    msg = json.dumps(payload, ensure_ascii=False)
    r.publish(channel, msg)
    r.xadd(STREAM_ALERTS, {"channel": channel, "payload": msg}, maxlen=10000, approximate=True)
    print(f"[ALERTA {channel}] {msg}", flush=True)


def handle_event(r, f, active):
    eid = f["event_id"]
    ts = parse_ts(f["ts"])
    mag = float(f["magnitud"])
    lat, lon = float(f["lat"]), float(f["lon"])

    # --- Pregunta 2: ventana deslizante por celda ---
    if mag >= SWARM_MIN_MAG:
        ci, cj = cell_of(lat, lon)
        key = f"win:{ci}:{cj}"
        p = r.pipeline()
        p.zadd(key, {eid: ts})
        p.zremrangebyscore(key, "-inf", ts - WINDOW_S)   # expulsa lo que salió de la ventana
        # cuenta solo (ts - W, ts]: si al stream llegan eventos de otra época (p. ej. un replay de 2014
        # después de una simulación de hoy), los "del futuro" no se suman a la ventana
        p.zcount(key, f"({ts - WINDOW_S}", ts)
        p.expire(key, WINDOW_S * 3)
        count = p.execute()[2]
        if count > SWARM_THRESHOLD:
            last = r.get(f"swarm:last:{ci}:{cj}")
            if last is None or abs(ts - float(last)) > WINDOW_S:   # evita repetir la alerta (en cualquier época)
                r.set(f"swarm:last:{ci}:{cj}", ts, ex=WINDOW_S * 3)
                clat, clon = cell_center(ci, cj)
                alert(r, CH_SWARM, {"type": "swarm", "cell": [ci, cj], "center": [clat, clon],
                                    "count": count, "window_s": WINDOW_S, "min_mag": SWARM_MIN_MAG,
                                    "ts": f["ts"]})

    # --- Pregunta 3: seguimiento de réplicas de mainshocks activos ---
    if mag >= AFT_MIN_MAG:
        for mid, m in list(active.items()):
            if ts > m["ts_epoch"] + AFT_TRACK_S:
                active.pop(mid)
                r.srem("mainshocks:active", mid)
            elif ts > m["ts_epoch"] and eid != mid and \
                    haversine_km(lat, lon, m["lat"], m["lon"]) <= m["radio_km"]:
                r.zadd(f"aft:{mid}", {eid: ts})
                r.expire(f"aft:{mid}", AFT_TRACK_S * 2)

    # --- Pregunta 3: detección de mainshock ---
    if mag >= MAIN_MAG:
        info = {"event_id": eid, "ts": f["ts"], "ts_epoch": ts, "magnitud": mag,
                "lat": lat, "lon": lon, "radio_km": radio_km(mag),
                "profundidad_km": f.get("profundidad_km") or None}
        r.set(f"main:{eid}", json.dumps(info), ex=AFT_TRACK_S * 2)
        r.sadd("mainshocks:active", eid)
        active[eid] = info
        alert(r, CH_MAIN, {"type": "mainshock", **info})

    return ts


def main():
    r = redis_client()
    ensure_group(r)
    active = load_active_mainshocks(r)
    clock = 0.0
    total, t_report = 0, time.time()
    print(f"[processor] escuchando {STREAM_IN} como {GROUP}/{CONSUMER}", flush=True)
    while True:
        resp = r.xreadgroup(GROUP, CONSUMER, {STREAM_IN: ">"}, count=200, block=1000)
        if not resp:
            continue
        ids = []
        for _stream, msgs in resp:
            for mid, fields in msgs:
                clock = handle_event(r, fields, active)
                ids.append(mid)
        # Reloj = tiempo de evento del ÚLTIMO evento procesado (la posición actual del stream), no el máximo
        # histórico: si después de una simulación de hoy se hace un replay de 2014, el reloj vuelve a 2014 y
        # cross_query.py espera de verdad la hora de réplicas de cada mainshock del replay.
        r.set(KEY_CLOCK, clock)
        r.xack(STREAM_IN, GROUP, *ids)
        total += len(ids)
        if time.time() - t_report >= 5:
            print(f"[processor] {total} eventos procesados", flush=True)
            t_report = time.time()


if __name__ == "__main__":
    main()
