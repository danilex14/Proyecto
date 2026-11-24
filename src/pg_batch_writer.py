#!/usr/bin/env python3
"""Escritor por lotes Redis Stream -> PostGIS.

Implementa la mitigación de riesgo de la propuesta: Redis Streams como cola intermedia
(desacople asíncrono) + batching antes de escribir en PostGIS (pocas transacciones grandes).

Variables útiles:  BATCH_SIZE (default 500)   FLUSH_S (default 2.0)
Con --batch-size 1 puedes medir el caso "sin batching" para el benchmark.
"""
import argparse
import socket
import time

from psycopg2.extras import execute_values

from common import STREAM_IN, pg_conn, redis_client

GROUP = "pg"
CONSUMER = f"pg-{socket.gethostname()}"

SQL = """
INSERT INTO sismos (event_id, ts, magnitud, profundidad_km, lat, lon, lugar, fuente)
VALUES %s
ON CONFLICT (event_id) DO NOTHING
"""


def ensure_group(r):
    from redis.exceptions import ResponseError
    try:
        r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def to_row(f):
    depth = f.get("profundidad_km")
    return (f["event_id"], f["ts"], float(f["magnitud"]),
            float(depth) if depth not in (None, "") else None,
            float(f["lat"]), float(f["lon"]), f.get("lugar") or None, f.get("fuente") or "sim")


def flush(conn, rows, ids, r, stats):
    if not rows:
        return
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        execute_values(cur, SQL, rows, page_size=len(rows))
    conn.commit()
    r.xack(STREAM_IN, GROUP, *ids)          # ack SOLO después del commit (no se pierden eventos)
    dt = (time.perf_counter() - t0) * 1000
    stats["rows"] += len(rows)
    stats["batches"] += 1
    stats["ms"] += dt
    print(f"[pg] lote de {len(rows)} filas en {dt:.1f} ms "
          f"(total {stats['rows']} filas, {stats['batches']} lotes)", flush=True)
    rows.clear()
    ids.clear()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--flush-s", type=float, default=2.0)
    a = ap.parse_args()

    r = redis_client()
    ensure_group(r)
    conn = pg_conn()
    rows, ids = [], []
    stats = {"rows": 0, "batches": 0, "ms": 0.0}
    last_flush = time.time()
    print(f"[pg] batch_size={a.batch_size} flush_s={a.flush_s}", flush=True)
    while True:
        resp = r.xreadgroup(GROUP, CONSUMER, {STREAM_IN: ">"}, count=a.batch_size, block=500)
        for _stream, msgs in resp or []:
            for mid, fields in msgs:
                rows.append(to_row(fields))
                ids.append(mid)
        if len(rows) >= a.batch_size or (rows and time.time() - last_flush >= a.flush_s):
            flush(conn, rows, ids, r, stats)
            last_flush = time.time()


if __name__ == "__main__":
    main()
