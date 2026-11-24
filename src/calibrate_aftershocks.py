#!/usr/bin/env python3
"""Calibra la productividad de réplicas del simulador con el catálogo real (PostGIS).

1. Mainshocks: sismos USGS superficiales (< 70 km) M >= 6.5 desde 2000 que NO tienen un sismo mayor en los
   3 días previos a <= 300 km (desagrupado simple: así no se toman réplicas como mainshocks).
2. Para cada uno cuenta réplicas M >= 4 dentro de su zona de impacto (buffer de Wells & Coppersmith) en la
   primera hora y en las primeras 24 h, igual que las cuenta el pipeline.
3. Ajusta por máxima verosimilitud:
     productividad   N24h(M>=4) = 10^(A + ALPHA * (M - 7))                     (Poisson)
     forma temporal  Omori p = 1 con c(M) = C7 * 10^(GAMMA * (M - 7))         (binomial: N1h | N24h)
     dispersión      binomial negativa, var = mu + mu^2 / K                    (máxima verosimilitud)
   La c resultante es grande (~1 h para M7) y crece con M: no es física, es la incompletitud de corto plazo
   del catálogo (las réplicas de las primeras horas se pierden en la coda del sismo grande). Como el pipeline
   compara contra ESE catálogo, el simulador debe imitar lo que el catálogo registra.

  python calibrate_aftershocks.py          # imprime los parámetros para common.py y guarda resultados/
"""
import json
import os

import numpy as np
import pandas as pd

from bench_common import RESULTS
from common import pg_conn

SQL = """
WITH ms AS (
    SELECT s.event_id, s.ts, s.magnitud AS m, s.buffer_geom
    FROM sismos s
    WHERE s.fuente = 'usgs' AND s.magnitud >= 6.5 AND s.profundidad_km < 70 AND s.ts >= '2000-01-01'
      AND NOT EXISTS (SELECT 1 FROM sismos b
                      WHERE b.magnitud > s.magnitud AND b.ts BETWEEN s.ts - interval '3 days' AND s.ts
                        AND ST_DWithin(b.geom::geography, s.geom::geography, 300000))
)
SELECT ms.event_id, ms.m,
       (SELECT count(*) FROM sismos e WHERE e.ts > ms.ts AND e.ts <= ms.ts + interval '1 hour'
           AND e.magnitud >= 4 AND ST_Intersects(ms.buffer_geom, e.geom)) AS n1h,
       (SELECT count(*) FROM sismos e WHERE e.ts > ms.ts AND e.ts <= ms.ts + interval '1 day'
           AND e.magnitud >= 4 AND ST_Intersects(ms.buffer_geom, e.geom)) AS n24h
FROM ms
"""


def omori_frac(c, t1=3600.0, t2=86400.0):
    """Fracción de las réplicas de [0, t2] que caen en [0, t1] (Omori p=1)."""
    return np.log1p(t1 / c) / np.log1p(t2 / c)


def fit(d):
    M, n1, n24 = d["m"].to_numpy(), d["n1h"].to_numpy(), d["n24h"].to_numpy()
    # productividad: grilla de máxima verosimilitud Poisson (vectorizada)
    A, AL = np.meshgrid(np.arange(0.5, 2.0, 0.005), np.arange(0.3, 1.6, 0.005), indexing="ij")
    mu = 10 ** (A[..., None] + AL[..., None] * (M - 7))
    ll = (n24 * np.log(mu) - mu).sum(-1)
    i, j = np.unravel_index(ll.argmax(), ll.shape)
    a, alpha = A[i, j], AL[i, j]
    # forma temporal: binomial N1h | N24h
    LC, G = np.meshgrid(np.arange(1.0, 5.0, 0.01), np.arange(-0.5, 2.0, 0.01), indexing="ij")
    f = np.clip(omori_frac(10 ** (LC[..., None] + G[..., None] * (M - 7))), 1e-9, 1 - 1e-9)
    ll = (n1 * np.log(f) + (n24 - n1) * np.log(1 - f)).sum(-1)
    i, j = np.unravel_index(ll.argmax(), ll.shape)
    c7, gamma = 10 ** LC[i, j], G[i, j]
    # dispersión: máxima verosimilitud de la binomial negativa con media mu(M), sobre una grilla de K
    from math import lgamma
    lg = np.vectorize(lgamma)
    mu = 10 ** (a + alpha * (M - 7))
    best = None
    for k in np.arange(0.05, 10.0, 0.01):
        ll = (lg(n24 + k) - lg(k) + k * np.log(k / (k + mu)) + n24 * np.log(mu / (k + mu))).sum()
        if best is None or ll > best[0]:
            best = (ll, k)
    k = best[1]
    return {"A": round(float(a), 3), "ALPHA": round(float(alpha), 3), "C7_S": round(float(c7)),
            "GAMMA": round(float(gamma), 2), "K": round(float(k), 2), "n_mainshocks": int(len(d))}


def main():
    conn = pg_conn()
    d = pd.read_sql(SQL, conn)
    conn.close()
    p = fit(d)
    print(f"{p['n_mainshocks']} mainshocks superficiales M>=6.5 (2000-2025, desagrupados)\n")
    print("Parámetros para common.py:")
    print(f"  AFT_PROD_A = {p['A']}\n  AFT_PROD_ALPHA = {p['ALPHA']}\n  OMORI_C7_S = {p['C7_S']}"
          f"\n  OMORI_C_GAMMA = {p['GAMMA']}\n  AFT_NB_K = {p['K']}\n")

    d["bin"] = pd.cut(d["m"], [6.45, 6.95, 7.45, 7.95, 9.5], labels=["6.5-6.9", "7.0-7.4", "7.5-7.9", "8.0+"])
    mu24 = 10 ** (p["A"] + p["ALPHA"] * (d["m"] - 7))
    d["mod24"] = mu24
    d["mod1h"] = mu24 * omori_frac(p["C7_S"] * 10 ** (p["GAMMA"] * (d["m"] - 7)))
    tab = d.groupby("bin", observed=True).agg(n=("m", "size"), obs_1h=("n1h", "mean"), modelo_1h=("mod1h", "mean"),
                                              mediana_1h=("n1h", "median"), obs_24h=("n24h", "mean"),
                                              modelo_24h=("mod24", "mean")).round(2)
    print(tab.to_string())
    os.makedirs(RESULTS, exist_ok=True)
    tab.to_csv(os.path.join(RESULTS, "calibracion_replicas.csv"))
    d.drop(columns=["bin"]).to_csv(os.path.join(RESULTS, "calibracion_replicas_eventos.csv"), index=False)
    with open(os.path.join(RESULTS, "calibracion_replicas.json"), "w", encoding="utf-8") as fh:
        json.dump(p, fh, indent=2)
    print("\n-> resultados/calibracion_replicas.{csv,json}")


if __name__ == "__main__":
    main()
