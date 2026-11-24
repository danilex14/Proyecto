#!/usr/bin/env python3
"""Pregunta 3: cruce Redis -> PostGIS.

Escucha alerts:mainshock (Pub/Sub). Al llegar un M>=6.5:
  1. PostGIS: busca sismos históricos ANÁLOGOS, es decir, con epicentro a <= CMP_RADIUS_KM del nuevo
     (ST_DWithin sobre geography, índice GiST idx_sismos_geog), magnitud dentro de +-CMP_DMAG y la
     misma clase de profundidad (superficial / intermedia / profunda: no se comparan réplicas de un
     sismo a 20 km con las de uno a 600 km, que casi no tiene).
     Para cada uno cuenta sus réplicas M>=AFT_MIN_MAG en la primera ventana de AFT_WINDOW_S
     segundos dentro de su propia zona de impacto (buffer_geom, índice GiST).
  2. Redis: espera a que el "reloj de eventos" avance AFT_WINDOW_S y lee cuántas réplicas lleva
     el evento actual (Sorted Set aft:<id>, contadas dentro de SU zona de impacto).
  3. Normaliza los conteos históricos a la magnitud actual (Gutenberg-Richter,
     N * 10^(b * (M_actual - M_hist))), compara con la tasa actual y guarda el informe
     (Redis report:<id> + tabla informes_mainshock).

Antes se usaba "históricos cuya zona de impacto contiene el epicentro": con el radio de Wells &
Coppersmith un M5.5 tiene una zona de ~6 km, así que casi solo cruzaban los megaterremotos y la
comparación quedaba con 1-2 casos. El criterio de análogos da una mediana de 6 por epicentro.
"""
import argparse
import json
import statistics
import time

from psycopg2.extras import Json

from common import (AFT_MIN_MAG, AFT_WINDOW_S, B_VALUE, CH_MAIN, CMP_DMAG, CMP_RADIUS_KM, KEY_CLOCK,
                    pg_conn, redis_client)

# columnas: 0 event_id, 1 ts, 2 magnitud, 3 profundidad, 4 radio_km, 5 replicas, 6 dist_km, 7 lugar
SQL_HIST = """
WITH p AS (SELECT ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography AS g)
SELECT s.event_id, s.ts, s.magnitud, s.profundidad_km,
       round(s.radio_km::numeric, 1) AS radio_km,
       (SELECT count(*) FROM sismos e
         WHERE e.ts > s.ts
           AND e.ts <= s.ts + make_interval(secs => %(w)s)
           AND e.magnitud >= %(amin)s
           AND e.event_id <> s.event_id
           AND ST_Intersects(s.buffer_geom, e.geom)) AS replicas,
       round((ST_Distance(s.geom::geography, p.g) / 1000)::numeric, 1) AS dist_km,
       s.lugar
FROM sismos s, p
WHERE ST_DWithin(s.geom::geography, p.g, %(r)s * 1000)       -- usa idx_sismos_geog
  AND s.magnitud BETWEEN %(mag)s - %(dm)s AND %(mag)s + %(dm)s
  -- misma clase de profundidad: superficial (<70 km), intermedia (70-300) o profunda (>300)
  AND (%(dclass)s::int IS NULL OR width_bucket(s.profundidad_km, ARRAY[70, 300]::real[]) = %(dclass)s::int)
  AND s.ts < to_timestamp(%(ts)s)
  AND s.event_id <> %(eid)s
  AND s.fuente <> 'sim'          -- el "histórico" excluye eventos simulados
ORDER BY s.magnitud DESC, s.ts DESC   -- desempate determinista: los más grandes y, a igual M, los más recientes
LIMIT 50
"""


def depth_class(depth):
    """0 superficial (<70 km), 1 intermedia (70-300), 2 profunda (>300); None si no se conoce."""
    if depth in (None, ""):
        return None
    d = float(depth)
    return 0 if d < 70 else (1 if d < 300 else 2)


def query_params(info, radius_km=CMP_RADIUS_KM, dmag=CMP_DMAG):
    return {"dclass": depth_class(info.get("profundidad_km")),"w": AFT_WINDOW_S, "amin": AFT_MIN_MAG, "lon": info["lon"], "lat": info["lat"],
            "r": radius_km, "mag": info["magnitud"], "dm": dmag, "ts": info["ts_epoch"],
            "eid": info["event_id"]}


def historical_analogs(conn, info, radius_km=CMP_RADIUS_KM, dmag=CMP_DMAG):
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(SQL_HIST, query_params(info, radius_km, dmag))
        rows = cur.fetchall()
    conn.rollback()
    return rows, (time.perf_counter() - t0) * 1000


def normalized(count, m_hist, m_now, b=B_VALUE):
    """Réplicas que habría tenido el histórico si hubiese sido de la magnitud actual."""
    return count * 10 ** (b * (m_now - m_hist))


def wait_for_window(r, info, timeout_s):
    """Espera (tiempo real) hasta que el reloj de eventos pase main_ts + ventana."""
    target = info["ts_epoch"] + AFT_WINDOW_S
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        clock = float(r.get(KEY_CLOCK) or 0)
        if clock >= target:
            return True
        time.sleep(1)
    return False


def handle(r, conn, info, radius_km, dmag, timeout_s):
    t0 = time.perf_counter()
    m_now = info["magnitud"]
    print(f"\n=== MAINSHOCK M{m_now} en ({info['lat']:.2f}, {info['lon']:.2f}) id={info['event_id']} ===",
          flush=True)

    rows, sql_ms = historical_analogs(conn, info, radius_km, dmag)
    adj = [normalized(row[5], row[2], m_now) for row in rows]
    print(f"[PostGIS] {len(rows)} sismos análogos (a <= {radius_km:.0f} km, M{m_now - dmag:.1f}-{m_now + dmag:.1f}) "
          f"en {sql_ms:.1f} ms")
    for row, a in list(zip(rows, adj))[:10]:
        print(f"   - {row[1]:%Y-%m-%d}  M{row[2]:.1f}  a {row[6]} km  -> {row[5]} réplicas M>={AFT_MIN_MAG} "
              f"en {AFT_WINDOW_S // 60} min (normalizado a M{m_now}: {a:.1f})  {row[7] or ''}")

    complete = wait_for_window(r, info, timeout_s)
    current = r.zcount(f"aft:{info['event_id']}", info["ts_epoch"], info["ts_epoch"] + AFT_WINDOW_S)

    report = {"mainshock": {k: info[k] for k in ("event_id", "ts", "magnitud", "lat", "lon", "radio_km")},
              "criterio": {"radio_km": radius_km, "dmag": dmag, "b_value": B_VALUE,
                           "clase_profundidad": depth_class(info.get("profundidad_km"))},
              "ventana_s": AFT_WINDOW_S, "mag_min_replicas": AFT_MIN_MAG,
              "ventana_completa": complete, "replicas_actual": current,
              "n_historicos": len(rows), "sql_ms": round(sql_ms, 2),
              "historicos": [{"event_id": row[0], "ts": row[1].isoformat(), "magnitud": row[2],
                              "dist_km": float(row[6]), "replicas": row[5], "replicas_normalizadas": round(a, 2)}
                             for row, a in zip(rows, adj)]}
    if rows:
        mean = statistics.mean(adj)
        report.update({"replicas_hist_media_bruta": round(statistics.mean(row[5] for row in rows), 2),
                       "replicas_hist_media_normalizada": round(mean, 2),
                       "replicas_hist_mediana_normalizada": round(statistics.median(adj), 2),
                       "replicas_hist_max_normalizada": round(max(adj), 2)})
        if mean == 0:
            verdict = "sin réplicas históricas de referencia (los análogos no tuvieron réplicas registradas)"
        else:
            ratio = current / mean
            report["razon_actual_vs_historica"] = round(ratio, 2)
            if ratio > 1.5:
                verdict = "ACTIVIDAD SUPERIOR a la histórica"
            elif ratio < 0.67:
                verdict = "actividad inferior a la histórica"
            else:
                verdict = "actividad similar a la histórica"
        report["veredicto"] = verdict + " (umbrales heurísticos 1.5x / 0.67x)"
    else:
        report["veredicto"] = f"no hay sismos análogos a <= {radius_km:.0f} km"

    print(f"[Redis] réplicas M>={AFT_MIN_MAG} del evento actual en la ventana: {current}"
          f"{'' if complete else '  (ventana incompleta: timeout)'}")
    if rows:
        print(f"[Histórico] media normalizada a M{m_now}: {report['replicas_hist_media_normalizada']} "
              f"(bruta {report['replicas_hist_media_bruta']})")
    print(f"[Resultado] {report['veredicto']}")

    report["latencia_total_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    r.set(f"report:{info['event_id']}", json.dumps(report, default=str), ex=86400)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO informes_mainshock (event_id, latencia_ms, informe) VALUES (%s,%s,%s) "
                    "ON CONFLICT (event_id) DO UPDATE SET informe = EXCLUDED.informe, "
                    "latencia_ms = EXCLUDED.latencia_ms",
                    (info["event_id"], sql_ms, Json(report, dumps=lambda o: json.dumps(o, default=str))))
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--radio-km", type=float, default=CMP_RADIUS_KM, help="distancia máx. de los análogos")
    ap.add_argument("--dmag", type=float, default=CMP_DMAG, help="diferencia máx. de magnitud de los análogos")
    ap.add_argument("--timeout", type=float, default=300, help="segundos reales máx. esperando la ventana")
    a = ap.parse_args()

    r = redis_client()
    conn = pg_conn()
    ps = r.pubsub()
    ps.subscribe(CH_MAIN)
    print(f"[cross] suscrito a {CH_MAIN}; esperando terremotos M>=6.5...", flush=True)
    for msg in ps.listen():
        if msg["type"] == "message":
            handle(r, conn, json.loads(msg["data"]), a.radio_km, a.dmag, a.timeout)


if __name__ == "__main__":
    main()
