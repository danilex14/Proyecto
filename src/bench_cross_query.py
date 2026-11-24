#!/usr/bin/env python3
"""Benchmark 2: latencia de la consulta de cruce de la Pregunta 3 (PostGIS), con y sin índices.

Usa la misma consulta de cross_query.py (sismos análogos: a <= R km y M +-dM, más sus réplicas) sobre
n epicentros reales tratados como un M7.0 nuevo. Modo "gist": planificador normal (GiST sobre
geography para la distancia, GiST de buffer_geom para las réplicas y B-tree de ts).
Modo "seqscan": índices desactivados en la sesión (enable_indexscan/bitmapscan = off), como
referencia de qué aporta el R-Tree. Guarda también el EXPLAIN ANALYZE de un caso por modo.

  python bench_cross_query.py --n 200 --n-seq 20 --criterios 150:0.5,300:1.0
"""
import argparse
import os

from bench_common import RESULTS, pct, print_table, sample_epicenters, save_csv
from common import pg_conn
from cross_query import SQL_HIST, historical_analogs, query_params

NO_INDEX = ["SET enable_indexscan = off", "SET enable_bitmapscan = off", "SET enable_indexonlyscan = off"]


def explain(conn, info, radius, dmag, path):
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + SQL_HIST, query_params(info, radius, dmag))
        plan = "\n".join(r[0] for r in cur.fetchall())
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(plan + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200, help="epicentros con índices")
    ap.add_argument("--n-seq", type=int, default=30, help="epicentros sin índices (es lento)")
    ap.add_argument("--criterios", default="150:0.5,300:1.0", help="radio_km:dmag de los análogos")
    a = ap.parse_args()

    conn = pg_conn()
    conn.autocommit = True          # para que los SET de sesión no se deshagan con el rollback
    epis = sample_epicenters(conn, a.n)
    # epicentro de referencia para el EXPLAIN: el que más históricos cruza (zona del Maule)
    ref = {"event_id": "bench-ref", "ts_epoch": epis[0]["ts_epoch"], "magnitud": 7.0, "profundidad_km": 20.0,
           "lat": -35.8, "lon": -72.7, "radio_km": epis[0]["radio_km"]}
    crits = [(float(c.split(":")[0]), float(c.split(":")[1])) for c in a.criterios.split(",")]

    summary, raw = [], []
    for mode in ("gist", "seqscan"):
        with conn.cursor() as cur:
            for s in (NO_INDEX if mode == "seqscan" else [x.replace("off", "on") for x in NO_INDEX]):
                cur.execute(s)
        subset = epis if mode == "gist" else epis[:a.n_seq]
        for radius, dmag in crits:
            crit = f"{radius:.0f} km, M±{dmag}"
            print(f"[cross] modo={mode} criterio={crit} n={len(subset)} ...", flush=True)
            historical_analogs(conn, subset[0], radius, dmag)   # calentamiento (caché)
            times, overl, reps = [], [], []
            for info in subset:
                rows, ms = historical_analogs(conn, info, radius, dmag)
                times.append(ms)
                overl.append(len(rows))
                reps.append(sum(r[5] for r in rows))
                raw.append({"modo": mode, "criterio": crit, "lat": info["lat"], "lon": info["lon"],
                            "ms": ms, "historicos": len(rows), "replicas_total": reps[-1]})
            summary.append({"modo": mode, "criterio": crit, "n": len(subset),
                            "ms_p50": pct(times, 50), "ms_p95": pct(times, 95),
                            "ms_media": sum(times) / len(times), "ms_max": max(times),
                            "analogos_media": sum(overl) / len(overl),
                            "con_historicos": sum(1 for o in overl if o)})
            explain(conn, ref, radius, dmag, os.path.join(RESULTS, f"explain_{mode}_r{radius:.0f}.txt"))
    print()
    print_table(summary)
    save_csv("cross_query.csv", summary)
    save_csv("cross_query_raw.csv", raw)


if __name__ == "__main__":
    main()
