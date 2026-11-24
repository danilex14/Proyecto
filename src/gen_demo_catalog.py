#!/usr/bin/env python3
"""Genera un catálogo SINTÉTICO con formato tipo USGS para probar PostGIS sin descargar nada.

  python gen_demo_catalog.py --n 30000 --out ../data/demo_catalog.csv
Luego:  python load_historico.py ../data/demo_catalog.csv --fuente demo
(OJO: los datos son inventados; para el informe usa el catálogo real de CSN/USGS.)
"""
import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from simulator import background_location, gr_mag, random_depth


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=30000)
    ap.add_argument("--out", default="../data/demo_catalog.csv")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    random.seed(a.seed)

    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    t0 = datetime(2000, 1, 1, tzinfo=timezone.utc)
    span = (datetime(2025, 12, 31, tzinfo=timezone.utc) - t0).total_seconds()
    with open(a.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["time", "latitude", "longitude", "depth", "mag", "place", "id"])
        for _ in range(a.n):
            lat, lon = background_location()
            ts = t0 + timedelta(seconds=random.random() * span)
            w.writerow([ts.isoformat(), lat, lon, random_depth(), gr_mag(3.0), "demo", uuid.uuid4().hex])
    print(f"{a.n} eventos sintéticos en {a.out}")


if __name__ == "__main__":
    main()
