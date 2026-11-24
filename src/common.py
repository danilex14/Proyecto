"""Configuración y utilidades compartidas por todos los scripts."""
import math
import os
from datetime import datetime

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PG_DSN = os.getenv("PG_DSN", "host=localhost port=5432 dbname=sismos user=sismos password=sismos")

# Nombres de estructuras en Redis
STREAM_IN = "sismos:in"          # cola de entrada (Redis Stream)
STREAM_ALERTS = "alerts:log"     # bitácora de alertas
CH_SWARM = "alerts:swarm"        # canal Pub/Sub de enjambres
CH_MAIN = "alerts:mainshock"     # canal Pub/Sub de terremotos mayores
KEY_CLOCK = "clock:last_ts"      # reloj de "tiempo de evento" (max ts visto)

# Parámetros del análisis (los de la propuesta)
CELL_DEG = 0.5            # tamaño de la celda regional
WINDOW_S = 3600           # ventana deslizante: 60 minutos. La propuesta decía 10 min, pero con datos
                          # reales (Chile 2000-2025) esa regla no alertó NUNCA, ni en Iquique 2014, Illapel
                          # 2015 ni Maule 2010; con 60 min detecta las tres (~1 alerta/año). Ver README.
SWARM_MIN_MAG = 4.0       # sólo cuentan eventos M >= 4.0
SWARM_THRESHOLD = 5       # enjambre: más de 5 eventos en la ventana
MAIN_MAG = 6.5            # terremoto mayor
AFT_MIN_MAG = 4.0         # magnitud mínima para contar réplicas
AFT_WINDOW_S = 3600       # ventana de réplicas a comparar (1 hora de tiempo de evento)
AFT_TRACK_S = 86400       # cuánto tiempo se siguen las réplicas de un mainshock

# Pregunta 3: qué sismos históricos son "análogos" del nuevo mainshock
CMP_RADIUS_KM = 150.0     # epicentro histórico a <= 150 km del nuevo (distancia geodésica)
CMP_DMAG = 0.5            # magnitud dentro de +-0.5 de la del nuevo
B_VALUE = 1.0             # Gutenberg-Richter: normaliza réplicas por magnitud, N * 10^(b * (M_nuevo - M_hist))

# Réplicas del simulador, calibradas con el catálogo real (calibrate_aftershocks.py, 805 mainshocks
# superficiales M>=6.5 desde 2000). N24h(M>=4) = 10^(A + ALPHA*(M-7)); Omori p=1 con c(M) = C7*10^(GAMMA*(M-7));
# variabilidad binomial negativa con dispersión K (var = mu + mu^2/K).
AFT_PROD_A = 1.22
AFT_PROD_ALPHA = 0.775
OMORI_C7_S = 3890
OMORI_C_GAMMA = 0.65
AFT_NB_K = 0.65


def redis_client():
    import redis
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def pg_conn():
    import psycopg2
    return psycopg2.connect(PG_DSN)


def cell_of(lat: float, lon: float):
    return math.floor(lat / CELL_DEG), math.floor(lon / CELL_DEG)


def cell_center(i: int, j: int):
    return (i + 0.5) * CELL_DEG, (j + 0.5) * CELL_DEG


def radio_km(mag: float) -> float:
    """Radio de la zona de impacto (Wells & Coppersmith 1994). Igual que en el trigger SQL."""
    return 10 ** (-2.44 + 0.59 * mag)


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def parse_ts(s: str) -> float:
    """ISO-8601 -> epoch en segundos."""
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
