#!/usr/bin/env python3
"""Descarga el catálogo histórico de USGS (CSV), de Chile o mundial.

La API de USGS entrega máx. 20.000 eventos por consulta: se baja por año y, si un rango llega al
tope, se parte en dos recursivamente (año -> meses -> días...). Cada año se guarda en una parte
(`<out>.parts/<año>.csv`), así una descarga larga que se corta se reanuda sin repetir lo ya bajado.
Al final se unen las partes en `--out`.

  python download_usgs.py --region chile  --start 2000 --end 2025 --minmag 3.0 --out ../data/usgs_chile.csv
  python download_usgs.py --region global --start 1973 --end 2025 --minmag 2.5 --out ../data/usgs_global.csv
"""
import argparse
import csv
import io
import os
import time
from datetime import datetime, timedelta

import requests

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
LIMIT = 20000
REGIONS = {
    # recuadro aproximado de Chile (incluye algo de Argentina/Perú/Bolivia)
    "chile": dict(minlatitude=-56, maxlatitude=-17, minlongitude=-76, maxlongitude=-66),
    "global": {},
}
S = requests.Session()
S.headers["User-Agent"] = "proyecto-sismos-usm/1.0 (curso)"


def fetch(start: datetime, end: datetime, minmag, region):
    params = dict(format="csv", eventtype="earthquake", orderby="time-asc", limit=LIMIT,
                  starttime=start.isoformat(), endtime=end.isoformat(), minmagnitude=minmag, **REGIONS[region])
    for attempt in range(6):
        try:
            resp = S.get(URL, params=params, timeout=180)
            if resp.status_code == 200:
                return list(csv.reader(io.StringIO(resp.text)))
            if resp.status_code == 204:
                return []
            msg = f"HTTP {resp.status_code}"
        except requests.RequestException as e:
            msg = str(e)[:120]
        wait = 5 * (attempt + 1)
        print(f"   reintento {attempt + 1} ({msg}) en {wait}s", flush=True)
        time.sleep(wait)
    raise SystemExit(f"USGS no responde para {start}..{end}")


def fetch_range(start, end, minmag, region):
    """Devuelve (header, filas) de [start, end); parte el rango en dos si llega al tope."""
    rows = fetch(start, end, minmag, region)
    if not rows:
        return None, []
    if len(rows) - 1 < LIMIT:
        return rows[0], rows[1:]
    mid = start + (end - start) / 2
    h1, r1 = fetch_range(start, mid, minmag, region)
    h2, r2 = fetch_range(mid, end, minmag, region)
    return h1 or h2, r1 + r2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", choices=list(REGIONS), default="chile")
    ap.add_argument("--start", type=int, default=2000)
    ap.add_argument("--end", type=int, default=2025)
    ap.add_argument("--minmag", type=float, default=3.0)
    ap.add_argument("--out", default="../data/usgs_chile.csv")
    a = ap.parse_args()

    parts = a.out + ".parts"
    os.makedirs(parts, exist_ok=True)
    total, t0 = 0, time.time()
    for year in range(a.start, a.end + 1):
        part = os.path.join(parts, f"{year}.csv")
        if os.path.exists(part):
            with open(part, encoding="utf-8") as fh:
                n = sum(1 for _ in fh) - 1
            total += max(n, 0)
            print(f"{year}: {n} eventos (ya descargado)", flush=True)
            continue
        header, rows = fetch_range(datetime(year, 1, 1), datetime(year + 1, 1, 1), a.minmag, a.region)
        tmp = part + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if header:
                w.writerow(header)
                w.writerows(rows)
        os.replace(tmp, part)                 # la parte solo existe si se completó
        total += len(rows)
        print(f"{year}: {len(rows)} eventos (acumulado {total}, {time.time() - t0:.0f}s)", flush=True)

    header_written, seen, dup = False, set(), 0
    with open(a.out, "w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        for year in range(a.start, a.end + 1):
            with open(os.path.join(parts, f"{year}.csv"), encoding="utf-8") as fh:
                r = csv.reader(fh)
                header = next(r, None)
                if header is None:
                    continue
                if not header_written:
                    w.writerow(header)
                    header_written = True
                    id_col = header.index("id")
                for row in r:
                    if row[id_col] in seen:   # un evento justo en el borde de dos rangos
                        dup += 1
                        continue
                    seen.add(row[id_col])
                    w.writerow(row)
    size = os.path.getsize(a.out) / 1e6
    print(f"Listo: {len(seen)} eventos ({dup} duplicados de borde omitidos), {size:.0f} MB en {a.out}")


if __name__ == "__main__":
    main()
