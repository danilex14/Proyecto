#!/usr/bin/env python3
"""Benchmark 1: efecto del batching en la escritura Redis Stream -> PostGIS.

Para cada tamaño de lote carga los MISMOS n eventos en un stream aparte (`bench:in`) y los consume
con exactamente la lógica de pg_batch_writer.py (XREADGROUP count=lote -> INSERT multi-fila con el
trigger de geometrías -> COMMIT -> XACK). Mide eventos/s y ms por lote (mediana de --repeat corridas, con mín/máx). Las filas de prueba
(fuente='bench') se borran al final.

  python bench_batching.py --n 10000 --sizes 1,10,50,100,500,1000
"""
import argparse
import time

from psycopg2.extras import execute_values

from bench_common import gen_stream, pct, print_table, save_csv
from common import pg_conn, redis_client
from pg_batch_writer import SQL, to_row

STREAM, GROUP = "bench:in", "bench"


def clean(r, conn):
    r.delete(STREAM)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sismos WHERE fuente = 'bench'")
    conn.commit()


def run(size, events, r, conn):
    clean(r, conn)
    p = r.pipeline(transaction=False)
    for e in events:
        p.xadd(STREAM, {k: ("" if v is None else str(v)) for k, v in e.items()})
    p.execute()
    r.xgroup_create(STREAM, GROUP, id="0")

    n, done, lat = len(events), 0, []
    t0 = time.perf_counter()
    while done < n:
        resp = r.xreadgroup(GROUP, "c1", {STREAM: ">"}, count=size, block=100)
        if not resp:
            continue
        msgs = resp[0][1]
        tb = time.perf_counter()
        with conn.cursor() as cur:
            execute_values(cur, SQL, [to_row(f) for _, f in msgs], page_size=len(msgs))
        conn.commit()
        r.xack(STREAM, GROUP, *[m for m, _ in msgs])
        lat.append((time.perf_counter() - tb) * 1000)
        done += len(msgs)
    total = time.perf_counter() - t0

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM sismos WHERE fuente = 'bench'")
        stored = cur.fetchone()[0]
    conn.commit()
    return {"batch_size": size, "eventos": n, "guardados": stored, "segundos": total,
            "eventos_por_s": n / total, "lotes": len(lat), "ms_lote_media": sum(lat) / len(lat),
            "ms_lote_p95": pct(lat, 95), "ms_por_evento": total * 1000 / n}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10000)
    ap.add_argument("--sizes", default="1,10,50,100,500,1000")
    ap.add_argument("--repeat", type=int, default=3, help="corridas por tamaño; se reporta la mediana")
    a = ap.parse_args()

    events = gen_stream(a.n, seed=1)
    r, conn = redis_client(), pg_conn()
    rows = []
    try:
        for size in [int(s) for s in a.sizes.split(",")]:
            print(f"[batching] lote={size} ...", flush=True)
            runs = sorted((run(size, events, r, conn) for _ in range(a.repeat)), key=lambda x: x["segundos"])
            med = dict(runs[len(runs) // 2])
            med["eventos_por_s_min"] = min(x["eventos_por_s"] for x in runs)
            med["eventos_por_s_max"] = max(x["eventos_por_s"] for x in runs)
            med["corridas"] = len(runs)
            rows.append(med)
    finally:
        clean(r, conn)
    print()
    print_table(rows)
    base = rows[0]["eventos_por_s"]
    print("\nAceleración vs lote de", rows[0]["batch_size"], ":",
          ", ".join(f"{x['batch_size']}: x{x['eventos_por_s'] / base:.1f}" for x in rows))
    save_csv("batching.csv", rows)


if __name__ == "__main__":
    main()
