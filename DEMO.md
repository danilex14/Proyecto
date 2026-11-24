# Demo en vivo (~12 minutos)

Guía para mostrar el proyecto funcionando: las tres preguntas, NiFi y el replay real de Iquique 2014.
Todos los comandos son de PowerShell y se corren desde la carpeta del proyecto.

## El día antes

1. Tener el histórico cargado (README, Pasos 1 a 3). Comprobar:
   ```powershell
   docker exec sismos-postgis psql -U sismos -d sismos -c "select count(*) from sismos"
   ```
   Debe decir unos **1.002.173** (más algunos simulados).
2. Hacer un ensayo completo de esta guía. La primera consulta tras reiniciar el PC tarda más (caché fría).

## 15 minutos antes

1. Levantar todo, NiFi incluido (tarda ~1-2 min en responder):
   ```powershell
   docker compose --profile nifi up -d
   ```
2. Crear el flujo de NiFi (si ya existe, `--reset` lo recrea):
   ```powershell
   .venv\Scripts\Activate.ps1; cd src; python nifi_setup.py --reset
   ```
3. Dejar Redis limpio para que las alertas en pantalla sean solo las de la demo:
   ```powershell
   docker exec sismos-redis redis-cli flushdb
   ```
4. "Calentar" PostGIS corriendo una vez la Pregunta 1 (paso 1 de la demo), para que en vivo tarde milisegundos.
5. Abrir en el navegador: la UI de NiFi (http://localhost:8080/nifi, grupo **sismos**) y el gráfico
   `resultados/graficos/2_cruce_indices.png`.
6. Abrir **4 terminales** en `src`, con el entorno activado (`..\.venv\Scripts\Activate.ps1`), letra grande:

| Terminal | Qué corre | Qué muestra |
|---|---|---|
| T1 | `python stream_processor.py` | las alertas `[ALERTA alerts:swarm]` y `[ALERTA alerts:mainshock]` |
| T2 | `python cross_query.py` | el análisis de cada terremoto contra sus análogos |
| T3 | `python pg_batch_writer.py` | los lotes que escribe a PostGIS (en la demo con NiFi no hace falta, pero no estorba) |
| T4 | los comandos de la demo | |

Arrancar T1, T2 y T3 antes de empezar a hablar.

## La demo, paso a paso

### 0. Contexto (1 min)
Mostrar el diagrama de arquitectura (en el informe o la presentación). Mensaje: *"Redis vigila el flujo en
tiempo real, PostGIS guarda 52 años de historia (1 millón de sismos reales), y la Pregunta 3 cruza los dos."*

### 1. Pregunta 1: clusters (1 min) · T4
```powershell
Get-Content ..\queries\q1_clusters.sql | docker exec -i sismos-postgis psql -U sismos -d sismos
```
**Qué mostrar:** la tabla del DBSCAN. Arriba están Loyalty, Vanuatu, Kermadec; más abajo Tohoku 2011 y
Maule 2010 (22 sismos). Señalar el `Time:` de unos 25-80 ms sobre un millón de filas.

### 2. Pregunta 2: un enjambre entrando por NiFi (2 min) · T4
```powershell
python simulator.py swarm --sink http
```
**Qué mostrar:**
- En la UI de NiFi, los contadores de `recibir-eventos` → `validar-esquema` → las dos ramas (Redis y PostGIS).
- En **T1**, a los pocos segundos: `[ALERTA alerts:swarm] ... "count": 6`.
- **Qué decir:** *"Más de 5 sismos M ≥ 4 en una celda de 0,5° dentro de 60 minutos. La propuesta decía 10 minutos,
  pero con datos reales esa regla no alertó nunca en 26 años; con 60 detecta Iquique, Illapel y Maule."*

### 3. Pregunta 3: un terremoto simulado (2 min) · T4
```powershell
python simulator.py mainshock --mag 7.0 --compress 60 --sink http --variabilidad real --seed 15
```
Una hora de réplicas pasa en un minuto. **Qué mostrar:**
- **T1:** `[ALERTA alerts:mainshock]` al instante.
- **T2:** de inmediato, los 6 análogos reales (Constitución 2012 M7,1, Tomé 2011 M6,9...) con sus réplicas
  normalizadas. Al minuto: `réplicas ... del evento actual: 13` y **"ACTIVIDAD SUPERIOR a la histórica"**.
- **Qué decir:** *"Las réplicas del simulador están calibradas con 805 terremotos reales; con esta semilla salió
  una secuencia productiva."* Si sobra tiempo, `--seed 12` da "similar" (4 réplicas) y `--seed 5` "inferior" (0).

### 4. Replay real de Iquique 2014 (2 min) · T4
```powershell
python simulator.py replay --escenario iquique2014 --compress 3600 --max-gap 0.2 --sink redis
```
40 días de sismos reales en ~70 segundos. **Qué mostrar:**
- T4 imprime cada sismo M ≥ 6 a medida que pasa: el M6,7 del 16 de marzo, el M8,2 del 1 de abril, el M7,7 del 3.
- **T1:** la primera alerta de enjambre es del **2014-03-16 21:35**: *"16 días antes del terremoto M8,2"*.
- **T2:** el análisis de los 5 terremotos M ≥ 6,5 de la secuencia (M6,9 y M7,7 "similar", M6,5 "inferior",
  y el M8,2 sin análogos de su tamaño desde 1973).

### 5. Lo que quedó guardado (1 min) · T4
```powershell
docker exec sismos-postgis psql -U sismos -d sismos -c "select event_id, round(latencia_ms) ms, informe->>'replicas_actual' actual, informe->>'replicas_hist_media_normalizada' historico, informe->>'veredicto' veredicto from informes_mainshock order by creado_en desc limit 6"
```
Cada informe queda en PostGIS y en Redis (`report:<id>`). Para mostrar el estado vivo en Redis:
```powershell
docker exec sismos-redis redis-cli xlen sismos:in
docker exec sismos-redis redis-cli keys "win:*"
```

### 6. Cierre con números (1 min)
Mostrar `2_cruce_indices.png`: con índices GiST la consulta de cruce tarda ~7 ms; sin ellos, 1 a 6 segundos.
Mensaje final: *"En memoria, Pandas y GeoPandas a veces son más rápidos, pero no dan estado compartido entre
procesos, durabilidad ni un histórico que crece sin reconstruir índices."*

## Si algo falla

| Problema | Qué hacer |
|---|---|
| NiFi no responde o va lento | Usar `--sink redis` en los pasos 2 y 3: el resultado es el mismo, solo se salta NiFi. |
| T1 no muestra alertas | Revisar que T1 esté corriendo; `docker exec sismos-redis redis-cli xlen sismos:in` debe subir al enviar eventos. |
| T2 se queda esperando | Espera que pase 1 hora en tiempo de evento: dejar terminar el comando del simulador (no cortarlo). |
| Salieron alertas de ensayos anteriores | `docker exec sismos-redis redis-cli flushdb` y reiniciar T1 y T2. |
| Un contenedor cayó | `docker compose --profile nifi up -d` y repetir `python nifi_setup.py --reset`. |
| Nada funciona | Mostrar las tablas y gráficos del informe y de `resultados/`: son de corridas reales. |

## Después de la demo
```powershell
docker compose --profile nifi stop
```
`stop` conserva los datos; `down -v` los borraría.
