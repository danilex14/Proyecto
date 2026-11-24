#!/usr/bin/env python3
"""Gráficos de los benchmarks del Paso 8 para el informe (PNG, fondo claro, listos para imprimir).

Lee los CSV de resultados/ y escribe resultados/graficos/*.png.

  python plot_benchmarks.py
"""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "..", "resultados")
OUT = os.path.join(RES, "graficos")

# Paleta de referencia (modo claro): 3 primeras ranuras categóricas, validadas entre todos los pares
SURFACE, INK, INK2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#8a8984", "#e6e5e1"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10, "text.color": INK,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.titlepad": 22, "axes.spines.top": False, "axes.spines.right": False,
    "xtick.color": INK2, "ytick.color": INK2, "xtick.major.size": 0, "ytick.major.size": 0,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "legend.frameon": False, "legend.fontsize": 9,
})


def miles(x, _=None):
    """Formato numérico chileno: 1.234 / 0,5"""
    if x >= 1:
        return f"{x:,.0f}".replace(",", ".")
    return f"{x:g}".replace(".", ",")


def num(x, dec=1):
    s = f"{x:,.{dec}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def subtitle(ax, text):
    ax.text(0, 1.02, text, transform=ax.transAxes, color=INK2, fontsize=9, va="bottom")


def save(fig, name):
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=200, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    print("->", os.path.relpath(path, os.path.join(HERE, "..")))


# ------------------------------------------------------------------ 1. batching
def plot_batching():
    df = pd.read_csv(os.path.join(RES, "batching.csv"))
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    x, y = df["batch_size"], df["eventos_por_s"]
    ax.axvspan(180, 800, color=GRID, alpha=0.6, lw=0)
    ax.text(380, 1.6e2, "atasco de red de\nDocker Desktop\n(SQL de 20-80 KB)", ha="center", va="bottom",
            color=INK2, fontsize=8)
    ax.fill_between(x, df["eventos_por_s_min"], df["eventos_por_s_max"], color=S1, alpha=0.15, lw=0)
    ax.plot(x, y, color=S1, lw=2, marker="o", ms=6, mec=SURFACE, mew=2, zorder=3)
    ax.axhline(80, color=MUTED, lw=1, ls=(0, (4, 3)))
    ax.text(1.05, 88, "carga real del proyecto: 80 ev/s", color=INK2, fontsize=8, va="bottom")
    for xi, yi in zip(x, y):
        if xi in (1, 100, 500, x.max()):
            ax.annotate(miles(yi), (xi, yi), textcoords="offset points", xytext=(0, 9), ha="center",
                        fontsize=8.5, color=INK)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_ylim(50, max(20000, y.max() * 2.5))
    ax.set_xticks(x)
    ax.xaxis.set_major_formatter(FuncFormatter(miles))
    ax.yaxis.set_major_formatter(FuncFormatter(miles))
    ax.minorticks_off()
    ax.set_xlabel("eventos por lote (escala log)")
    ax.set_ylabel("eventos escritos por segundo (log)")
    ax.set_title(f"El batching multiplica por ~{y.max() / y.iloc[0]:.0f} la escritura en PostGIS")
    subtitle(ax, "Redis Stream -> PostGIS, 10.000 eventos; mediana de 3 corridas, banda = mín-máx")
    save(fig, "1_batching.png")


# ------------------------------------------------------------------ 2. cruce con / sin índices
def plot_cross():
    df = pd.read_csv(os.path.join(RES, "cross_query.csv"))
    fig, ax = plt.subplots(figsize=(7.5, 3.6))
    hmins = list(dict.fromkeys(df["criterio"]))          # orden del CSV
    h = 0.36
    for k, (modo, color, nombre) in enumerate((("gist", S1, "con índice GiST"), ("seqscan", S2, "sin índices"))):
        sub = df[df["modo"] == modo].set_index("criterio").loc[hmins]
        yy = np.arange(len(hmins)) + (k - 0.5) * (h + 0.04)
        ax.barh(yy, sub["ms_p50"], height=h, color=color, label=nombre, edgecolor=SURFACE, lw=2)
        ax.errorbar(sub["ms_p95"], yy, fmt="|", color=INK2, ms=10, mew=1.5)
        for yi, p50, p95 in zip(yy, sub["ms_p50"], sub["ms_p95"]):
            ax.text(p95 * 1.15, yi, f"{num(p50)} ms  (p95 {num(p95)})", va="center", fontsize=8.5, color=INK)
    ax.set_yticks(np.arange(len(hmins)))
    ax.set_yticklabels([("análogos a ≤ " + c.split(",")[0] + "\n" + c.split(",")[1].strip()).replace(".", ",")
                        for c in hmins])
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.set_xlim(0.5, df["ms_p95"].max() * 12)
    ax.xaxis.set_major_formatter(FuncFormatter(miles))
    ax.minorticks_off()
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("ms por consulta (barra = mediana, | = p95; escala log)")
    g, sq = df[df["modo"] == "gist"]["ms_p50"].mean(), df[df["modo"] == "seqscan"]["ms_p50"].mean()
    ax.set_title(f"El R-Tree (GiST) acelera ~{sq / g:.0f}x la consulta de cruce")
    subtitle(ax, "Pregunta 3: sismos análogos a un nuevo M7.0 (distancia, magnitud, profundidad) y sus réplicas en 1 h")
    ax.legend(loc="upper right", ncol=2, handlelength=1.2)
    save(fig, "2_cruce_indices.png")


# ------------------------------------------------------------------ 3. consulta: costo acumulado
def n_hist():
    try:
        from common import pg_conn
        with pg_conn() as c, c.cursor() as cur:
            cur.execute("SELECT count(*) FROM sismos WHERE fuente <> 'sim'")
            return cur.fetchone()[0]
    except Exception:
        return 0


N_HIST = 0


def plot_query_cost():
    df = pd.read_csv(os.path.join(RES, "pandas_consulta.csv"))
    df = df[df["criterio"] == df["criterio"].iloc[0]].set_index("metodo")
    pg0, gp0 = df.loc["postgis"], df.loc["geopandas"]
    cross0 = gp0["prep_ms"] / (pg0["ms_media"] - gp0["ms_media"])
    n_max = int(np.ceil(cross0 * 1.6 / 1000) * 1000)
    n = np.arange(0, n_max + 1)
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    series = (("postgis", S1, "PostGIS"), ("geopandas", S2, "GeoPandas (STRtree)"), ("numpy", S3, "NumPy (haversine)"))
    ends = {}
    for key, color, name in series:
        cost = (df.loc[key, "prep_ms"] + n * df.loc[key, "ms_media"]) / 1000
        ax.plot(n, cost, color=color, lw=2, label=name)
        ends[key] = cost[-1]
    # etiquetas al final de cada línea, separadas al menos 9 % del eje para que no se pisen
    names = {k: nm for k, _, nm in series}
    gap, prev = max(ends.values()) * 0.09, None
    for key in sorted(ends, key=ends.get):
        y = ends[key] if prev is None else max(ends[key], prev + gap)
        prev = y
        ax.text(n_max * 1.015, y, f"{names[key]}\n{num(df.loc[key, 'ms_p50'], 2)} ms/consulta", color=INK,
                fontsize=8, va="center")
    pg, gp = df.loc["postgis"], df.loc["geopandas"]
    cross = gp["prep_ms"] / (pg["ms_media"] - gp["ms_media"])
    ax.plot([cross], [(pg["ms_media"] * cross) / 1000], "o", color=INK, ms=6, mec=SURFACE, mew=2, zorder=4)
    ax.annotate(f"GeoPandas recupera su carga\ninicial tras ~{miles(round(cross, -2 if cross > 1000 else 0))} consultas", (cross, pg["ms_media"] * cross / 1000),
                textcoords="offset points", xytext=(-150, 30), fontsize=8.5, color=INK2,
                arrowprops=dict(arrowstyle="-", color=MUTED, lw=1))
    ax.set_xlim(0, n_max)
    ax.set_ylim(0, None)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: num(v, 0) if v >= 10 else num(v, 1)))
    ax.xaxis.set_major_formatter(FuncFormatter(miles))
    ax.set_xlabel("número de consultas de cruce")
    ax.set_ylabel("tiempo acumulado (s)")
    if df.loc["numpy", "ms_media"] > df.loc["postgis", "ms_media"]:
        ax.set_title("En memoria solo gana con índice espacial y muchas consultas")
    else:
        ax.set_title("En memoria es más rápido, pero hay que cargar los datos")
    agree = min(df.loc["geopandas", "coincide_replicas_%"], df.loc["numpy", "coincide_replicas_%"])
    subtitle(ax, f"Costo = preparación (cargar {miles(N_HIST)} eventos + índice) + consultas; "
                 f"resultados iguales a PostGIS en el {num(agree, 1)} % de los casos")
    ax.legend(loc="upper left")
    save(fig, "3_consulta_costo.png")


# ------------------------------------------------------------------ 4. streaming
def plot_streaming():
    df = pd.read_csv(os.path.join(RES, "pandas_streaming.csv"))
    names = {"redis (stream_processor)": "Redis (stream_processor.py)",
             "pandas micro-lotes 200": "Pandas, micro-lotes de 200",
             "pandas por evento": "Pandas, evento a evento",
             "python dict+deque": "Python puro en memoria"}
    df = df.sort_values("eventos_por_s")
    fig, ax = plt.subplots(figsize=(7.5, 3.4))
    yy = np.arange(len(df))
    colors = [S1 if "redis" in i else "#9ec5f4" for i in df["implementacion"]]
    ax.barh(yy, df["eventos_por_s"], height=0.55, color=colors, edgecolor=SURFACE, lw=2)
    for yi, v in zip(yy, df["eventos_por_s"]):
        ax.text(v * 1.2, yi, f"{miles(v)} ev/s", va="center", fontsize=8.5, color=INK)
    ax.axvline(80, color=MUTED, lw=1, ls=(0, (4, 3)))
    ax.set_ylim(-0.95, len(df) - 0.5)
    ax.text(88, -0.68, "carga real: 80 ev/s", color=INK2, fontsize=8, va="center")
    ax.set_yticks(yy)
    ax.set_yticklabels([names[i] for i in df["implementacion"]])
    ax.set_xscale("log")
    ax.set_xlim(30, 5e7)
    ax.xaxis.set_major_formatter(FuncFormatter(miles))
    ax.minorticks_off()
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("eventos procesados por segundo (escala log)")
    ax.set_title(f"Todas las variantes superan por >{int(df['eventos_por_s'].min() / 80 // 50 * 50)}x la carga real")
    from common import WINDOW_S
    subtitle(ax, f"Pregunta 2: ventana deslizante de {WINDOW_S // 60} min por celda; las cuatro dan las mismas alertas")
    save(fig, "4_streaming.png")


if __name__ == "__main__":
    N_HIST = n_hist()
    plot_batching()
    plot_cross()
    plot_query_cost()
    plot_streaming()
