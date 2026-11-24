#!/usr/bin/env python3
"""Arma el flujo de NiFi del proyecto por la API REST (reproducible, sin clics en la UI).

Crea el grupo de procesos "sismos" en el lienzo raíz:

  ListenHTTP :8081/sismos ──► ValidateRecord (esquema Avro) ──┬─ rama A: ExecuteGroovyScript ──► XADD Redis sismos:in
                                   │ invalid/failure          └─ rama B: MergeRecord (500-1000, 2 s) ──► PutDatabaseRecord (PostGIS)
                                   ▼
                              LogAttribute "invalidos"

  python nifi_setup.py            # crea el flujo y lo arranca
  python nifi_setup.py --reset    # borra el grupo "sismos" si existe y lo vuelve a crear
  python nifi_setup.py --delete   # solo lo borra

Con NiFi la rama B reemplaza a pg_batch_writer.py (si corres ambos no hay duplicados: los dos
insertan con ON CONFLICT (event_id) DO NOTHING).
"""
import argparse
import json
import os
import sys
import time

import requests

API = os.getenv("NIFI_API", "http://localhost:8080/nifi-api")
GROUP_NAME = "sismos"
HERE = os.path.dirname(os.path.abspath(__file__))
GROOVY = os.path.join(HERE, "..", "nifi", "scripts", "redis_xadd.groovy")

# Dentro de la red de docker compose los servicios se llaman por su nombre
# stringtype=unspecified: los strings se envían sin tipo y PostgreSQL los convierte según la columna
# (así 'ts' en ISO-8601 entra directo a timestamptz; si no, falla con "ts is of type timestamptz
# but expression is of type character varying")
JDBC_URL = "jdbc:postgresql://postgis:5432/sismos?stringtype=unspecified"
JDBC_JAR = "/opt/nifi/drivers/postgresql-42.7.4.jar"

SCHEMA = json.dumps({
    "type": "record", "name": "sismo",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "ts", "type": "string"},          # ISO-8601; PostgreSQL lo convierte a timestamptz
        {"name": "magnitud", "type": "double"},
        {"name": "profundidad_km", "type": ["null", "double"], "default": None},
        {"name": "lat", "type": "double"},
        {"name": "lon", "type": "double"},
        {"name": "lugar", "type": ["null", "string"], "default": None},
        {"name": "fuente", "type": ["null", "string"], "default": None},
    ],
})

S = requests.Session()


def call(method, path, **kw):
    r = S.request(method, API + path, timeout=30, **kw)
    if r.status_code >= 400:
        raise SystemExit(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
    return r.json() if r.text and r.headers.get("content-type", "").startswith("application/json") else None


def wait(pred, what, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return
        time.sleep(1)
    raise SystemExit(f"Timeout esperando: {what}")


# ---------------------------------------------------------------- borrar grupo existente
def find_group(root):
    flow = call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]
    for pg in flow["processGroups"]:
        if pg["component"]["name"] == GROUP_NAME:
            return pg["id"]
    return None


def delete_group(pg):
    call("PUT", f"/flow/process-groups/{pg}", json={"id": pg, "state": "STOPPED"})
    wait(lambda: call("GET", f"/flow/process-groups/{pg}/status")
         ["processGroupStatus"]["aggregateSnapshot"]["activeThreadCount"] == 0, "procesadores detenidos")
    call("PUT", f"/flow/process-groups/{pg}/controller-services", json={"id": pg, "state": "DISABLED"})
    wait(lambda: all(cs["component"]["state"] == "DISABLED" for cs in
                     call("GET", f"/flow/process-groups/{pg}/controller-services")["controllerServices"]
                     if cs["component"]["parentGroupId"] == pg), "servicios deshabilitados")
    req = call("POST", f"/process-groups/{pg}/empty-all-connections-requests")["dropRequest"]
    wait(lambda: call("GET", f"/process-groups/{pg}/empty-all-connections-requests/{req['id']}")
         ["dropRequest"]["finished"], "colas vaciadas")
    call("DELETE", f"/process-groups/{pg}/empty-all-connections-requests/{req['id']}")
    ver = call("GET", f"/process-groups/{pg}")["revision"]["version"]
    call("DELETE", f"/process-groups/{pg}", params={"version": ver})
    print(f"[nifi] grupo '{GROUP_NAME}' borrado")


# ---------------------------------------------------------------- creación
def service(pg, typ, props, name):
    cs = call("POST", f"/process-groups/{pg}/controller-services", json={
        "revision": {"version": 0}, "component": {"type": typ, "name": name, "properties": props}})
    return cs["id"]


def enable_services(pg):
    call("PUT", f"/flow/process-groups/{pg}/controller-services", json={"id": pg, "state": "ENABLED"})

    def ok():
        css = call("GET", f"/flow/process-groups/{pg}/controller-services")["controllerServices"]
        bad = [c for c in css if c["component"]["state"] != "ENABLED"]
        for c in bad:
            if c["component"].get("validationErrors"):
                raise SystemExit(f"Servicio inválido {c['component']['name']}: {c['component']['validationErrors']}")
        return not bad
    wait(ok, "servicios habilitados")


def processor(pg, typ, name, x, y, props=None, auto_terminate=(), schedule=None, concurrent=1):
    cfg = {"properties": props or {}, "autoTerminatedRelationships": list(auto_terminate),
           "concurrentlySchedulableTaskCount": concurrent}
    if schedule:
        cfg["schedulingPeriod"] = schedule
    p = call("POST", f"/process-groups/{pg}/processors", json={
        "revision": {"version": 0},
        "component": {"type": typ, "name": name, "position": {"x": x, "y": y}, "config": cfg}})
    return p["id"]


def connect(pg, src, dst, rels, name=None):
    call("POST", f"/process-groups/{pg}/connections", json={
        "revision": {"version": 0},
        "component": {"name": name or ",".join(rels),
                      "source": {"id": src, "groupId": pg, "type": "PROCESSOR"},
                      "destination": {"id": dst, "groupId": pg, "type": "PROCESSOR"},
                      "selectedRelationships": rels,
                      "backPressureObjectThreshold": 20000,
                      "backPressureDataSizeThreshold": "1 GB"}})


def build(root):
    pg = call("POST", f"/process-groups/{root}/process-groups", json={
        "revision": {"version": 0},
        "component": {"name": GROUP_NAME, "position": {"x": 0, "y": 0}}})["id"]

    # --- servicios de controlador
    reader = service(pg, "org.apache.nifi.json.JsonTreeReader",
                     {"schema-access-strategy": "schema-text-property", "schema-text": SCHEMA}, "lector-json-sismo")
    writer = service(pg, "org.apache.nifi.json.JsonRecordSetWriter",
                     {"schema-access-strategy": "inherit-record-schema", "output-grouping": "output-oneline"},
                     "escritor-json")
    dbcp = service(pg, "org.apache.nifi.dbcp.DBCPConnectionPool", {
        "Database Connection URL": JDBC_URL, "Database Driver Class Name": "org.postgresql.Driver",
        "database-driver-locations": JDBC_JAR, "Database User": "sismos", "Password": "sismos"}, "postgis")
    enable_services(pg)

    # --- procesadores
    listen = processor(pg, "org.apache.nifi.processors.standard.ListenHTTP", "recibir-eventos", 0, 0,
                       {"Listening Port": "8081", "Base Path": "sismos"})
    validate = processor(pg, "org.apache.nifi.processors.standard.ValidateRecord", "validar-esquema", 0, 220,
                         {"record-reader": reader, "record-writer": writer, "allow-extra-fields": "false",
                          "strict-type-checking": "true",
                          "validation-details-attribute-name": "validacion.detalle"})
    with open(GROOVY, encoding="utf-8") as fh:
        groovy_body = fh.read()
    xadd = processor(pg, "org.apache.nifi.processors.groovyx.ExecuteGroovyScript", "redis-xadd", -420, 440,
                     {"groovyx-script-body": groovy_body, "groovyx-failure-strategy": "rollback"},
                     auto_terminate=("success", "failure"), schedule="0 sec")
    merge = processor(pg, "org.apache.nifi.processors.standard.MergeRecord", "lotes-500", 420, 440,
                      {"record-reader": reader, "record-writer": writer, "min-records": "500",
                       "max-records": "1000", "max-bin-age": "2 sec"},
                      auto_terminate=("original",))
    putdb = processor(pg, "org.apache.nifi.processors.standard.PutDatabaseRecord", "postgis-sismos", 420, 660,
                      {"put-db-record-record-reader": reader, "db-type": "PostgreSQL",
                       "put-db-record-statement-type": "INSERT_IGNORE", "put-db-record-dcbp-service": dbcp,
                       "put-db-record-table-name": "sismos", "put-db-record-update-keys": "event_id",
                       "put-db-record-unmatched-column-behavior": "Ignore Unmatched Columns"},
                      auto_terminate=("success",))
    invalid = processor(pg, "org.apache.nifi.processors.standard.LogAttribute", "invalidos", -420, 220,
                        {"Log Payload": "true", "Log Level": "warn"}, auto_terminate=("success",))

    # --- conexiones
    connect(pg, listen, validate, ["success"])
    connect(pg, validate, xadd, ["valid"], "valid -> redis")
    connect(pg, validate, merge, ["valid"], "valid -> postgis")
    connect(pg, validate, invalid, ["invalid", "failure"])
    connect(pg, merge, putdb, ["merged"])
    connect(pg, merge, invalid, ["failure"])
    connect(pg, putdb, invalid, ["failure"])
    connect(pg, putdb, putdb, ["retry"])

    # Esperar a que termine la validación (ExecuteGroovyScript compila el script y tarda unos
    # segundos; si se arranca el grupo mientras está VALIDATING, NiFi lo deja detenido).
    def procs():
        return call("GET", f"/process-groups/{pg}/processors")["processors"]

    def validated():
        ps = procs()
        errs = {p["component"]["name"]: p["component"].get("validationErrors") for p in ps
                if p["component"].get("validationStatus") == "INVALID"}
        if errs:
            raise SystemExit(f"Procesadores inválidos: {json.dumps(errs, indent=1, ensure_ascii=False)}")
        return all(p["component"].get("validationStatus") == "VALID" for p in ps)
    wait(validated, "validación de procesadores")
    call("PUT", f"/flow/process-groups/{pg}", json={"id": pg, "state": "RUNNING"})
    wait(lambda: all(p["component"]["state"] == "RUNNING" for p in procs()), "procesadores corriendo")
    print(f"[nifi] flujo '{GROUP_NAME}' creado y corriendo. UI: http://localhost:8080/nifi")
    print("[nifi] prueba:  python simulator.py normal --rate 50 --duration 10 --sink http")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="borra el grupo existente y lo vuelve a crear")
    ap.add_argument("--delete", action="store_true", help="solo borra el grupo")
    a = ap.parse_args()

    try:
        root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
    except requests.ConnectionError:
        sys.exit("NiFi no responde en localhost:8080 (docker compose --profile nifi up -d; tarda ~1-2 min)")
    existing = find_group(root)
    if existing and (a.reset or a.delete):
        delete_group(existing)
    elif existing:
        sys.exit(f"Ya existe el grupo '{GROUP_NAME}'. Usa --reset para recrearlo.")
    if not a.delete:
        build(root)


if __name__ == "__main__":
    main()
