"""
TP 2B — Pipeline complet : Open-Meteo → MinIO (raw) → transformation → PostgreSQL
===================================================================================

Architecture du DAG
-------------------
fetch_weather
    └── save_to_minio
            └── transform_weather
                    └── load_weather
                            └── write_ingestion_log

Paramétrage — zéro hardcode
-----------------------------
Toute la configuration passe par Airflow, sans valeur figée dans le code :

  Variables Airflow (Admin > Variables) — paramètres métier et techniques :
    meteo_cities          : JSON array des villes
    meteo_fields          : champs API Open-Meteo
    meteo_api_base_url    : URL de base de l'API
    meteo_api_timeout     : timeout HTTP en secondes
    meteo_raw_bucket      : bucket MinIO pour le raw
    meteo_processed_bucket: bucket MinIO pour le processed
    meteo_schema          : schéma PostgreSQL
    meteo_target_table    : table cible
    meteo_tracking_table  : table de suivi

  Connections Airflow (Admin > Connections) — credentials :
    postgres_meteo : connexion PostgreSQL (host, port, login, password, schema)
    minio_meteo    : connexion MinIO
                     - Conn Type : Amazon Web Services
                     - Login     : access key
                     - Password  : secret key
                     - Extra     : {"endpoint_url": "http://minio:9000"}
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from botocore.client import Config

log = logging.getLogger(__name__)

default_args = {
    "owner": "lucas",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

CONN_POSTGRES = "postgres_meteo"
CONN_MINIO    = "minio_meteo"

CITY_COORDS: dict[str, dict] = {
    "Paris":            {"latitude": 48.8566, "longitude": 2.3522},
    "Lyon":             {"latitude": 45.7640, "longitude": 4.8357},
    "Marseille":        {"latitude": 43.2965, "longitude": 5.3698},
    "Bordeaux":         {"latitude": 44.8378, "longitude": -0.5792},
    "Lille":            {"latitude": 50.6292, "longitude": 3.0573},
    "Clermont-Ferrand": {"latitude": 45.7772, "longitude": 3.0870},
    "Nantes":           {"latitude": 47.2184, "longitude": -1.5536},
    "Strasbourg":       {"latitude": 48.5734, "longitude": 7.7521},
}


def _get_minio_client() -> boto3.client:
    """
    Construit un client boto3 à partir de la Connection Airflow 'minio_meteo'.
    Les credentials ne sont jamais dans le code — ils viennent d'Airflow.

    Connection attendue (Admin > Connections) :
      Conn Type : Amazon Web Services
      Login     : access key MinIO
      Password  : secret key MinIO
      Extra     : {"endpoint_url": "http://minio:9000"}
    """
    conn = BaseHook.get_connection(CONN_MINIO)
    extra = json.loads(conn.extra) if conn.extra else {}
    endpoint_url = extra.get("endpoint_url", "http://minio:9000")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@dag(
    dag_id="tp2b_meteo_pipeline",
    description="TP 2B — Open-Meteo → MinIO (raw) → transformation → PostgreSQL",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["tp2b", "meteo", "etl", "minio"],
    doc_md=__doc__,
)
def tp2b_meteo_pipeline():

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 1 — Extraction : appel Open-Meteo
    # ─────────────────────────────────────────────────────────────────────────
    @task(task_id="fetch_weather")
    def fetch_weather(**context) -> dict:
        """
        Appelle l'API Open-Meteo pour chaque ville.
        URL et timeout viennent des Variables Airflow.
        """
        cities: list[str] = json.loads(
            Variable.get("meteo_cities", default_var='["Paris", "Lyon", "Marseille", "Bordeaux", "Lille"]')
        )
        fields: str = Variable.get(
            "meteo_fields",
            default_var="temperature_2m,precipitation,windspeed_10m,weathercode",
        )
        base_url: str = Variable.get(
            "meteo_api_base_url",
            default_var="https://api.open-meteo.com/v1/forecast",
        )
        timeout: int = int(Variable.get("meteo_api_timeout", default_var="30"))

        log.info("Villes : %s | Champs : %s", cities, fields)

        raw_results: dict[str, dict] = {}
        rows_received = 0

        for city in cities:
            if city not in CITY_COORDS:
                log.warning("Ville inconnue ignorée : %s", city)
                continue

            coords = CITY_COORDS[city]
            params = {
                "latitude": coords["latitude"],
                "longitude": coords["longitude"],
                "hourly": fields,
                "timezone": "Europe/Paris",
                "forecast_days": 1,
            }

            resp = requests.get(base_url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            raw_results[city] = data
            nb = len(data.get("hourly", {}).get("time", []))
            rows_received += nb
            log.info("  ✓ %s — %d mesures reçues", city, nb)

        log.info("Extraction terminée — %d mesures brutes", rows_received)

        return {
            "raw": raw_results,
            "rows_received": rows_received,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 2 — Sauvegarde du brut dans MinIO
    # ─────────────────────────────────────────────────────────────────────────
    @task(task_id="save_to_minio")
    def save_to_minio(fetch_result: dict, **context) -> dict:
        """
        Écrit le JSON brut dans MinIO : raw-meteo/{run_id}/{city}.json
        Le client MinIO est construit depuis la Connection Airflow 'minio_meteo'.
        """
        raw_results: dict = fetch_result["raw"]
        run_id: str = context["run_id"]
        bucket: str = Variable.get("meteo_raw_bucket", default_var="raw-meteo")

        s3 = _get_minio_client()
        saved_keys: list[str] = []

        for city, data in raw_results.items():
            key = f"{run_id}/{city}.json"
            body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=BytesIO(body),
                ContentType="application/json",
            )
            saved_keys.append(key)
            log.info("  ✓ MinIO : s3://%s/%s (%d octets)", bucket, key, len(body))

        log.info("Sauvegarde MinIO terminée — %d fichiers dans s3://%s/%s/",
                 len(saved_keys), bucket, run_id)

        return {
            "bucket": bucket,
            "run_id": run_id,
            "saved_keys": saved_keys,
            "rows_received": fetch_result["rows_received"],
        }

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 3 — Transformation : lecture MinIO → structuration
    # ─────────────────────────────────────────────────────────────────────────
    @task(task_id="transform_weather")
    def transform_weather(minio_result: dict, **context) -> dict:
        """
        Lit les JSON bruts depuis MinIO et les transforme en lignes
        prêtes pour l'insertion PostgreSQL.
        """
        bucket: str = minio_result["bucket"]
        saved_keys: list[str] = minio_result["saved_keys"]
        airflow_run_id: str = context["run_id"]

        STAT_MAP = {
            "temperature_2m": ("temperature_c",    lambda v: v),
            "precipitation":  ("precipitation_mm", lambda v: v),
            "windspeed_10m":  ("windspeed_kmh",    lambda v: v),
            "weathercode":    ("weathercode",      lambda v: int(v) if v is not None else None),
        }

        s3 = _get_minio_client()
        rows: list[dict] = []

        for key in saved_keys:
            city = key.split("/")[-1].replace(".json", "")

            obj = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read().decode("utf-8"))

            hourly = data.get("hourly", {})
            times  = hourly.get("time", [])

            for i, time_str in enumerate(times):
                dt = datetime.fromisoformat(time_str)
                row: dict = {
                    "city": city,
                    "measure_date": dt.strftime("%Y-%m-%d"),
                    "measure_hour": dt.hour,
                    "airflow_run_id": airflow_run_id,
                }

                all_none = True
                for api_field, (col_name, cast) in STAT_MAP.items():
                    values = hourly.get(api_field, [])
                    raw_val = values[i] if i < len(values) else None
                    row[col_name] = cast(raw_val) if raw_val is not None else None
                    if raw_val is not None:
                        all_none = False

                if not all_none:
                    rows.append(row)

            log.info("  ✓ %s — %d lignes transformées", city, len(times))

        log.info("Transformation terminée — %d lignes prêtes", len(rows))

        return {
            "rows": rows,
            "rows_received": minio_result["rows_received"],
        }

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 4 — Chargement dans PostgreSQL
    # ─────────────────────────────────────────────────────────────────────────
    @task(task_id="load_weather")
    def load_weather(transform_result: dict, **context) -> dict:
        """
        INSERT dans weather_facts via la Connection Airflow 'postgres_meteo'.
        ON CONFLICT DO NOTHING : idempotent, safe pour les rejeux.
        """
        rows: list[dict] = transform_result["rows"]
        schema: str = Variable.get("meteo_schema", default_var="public")
        table: str  = Variable.get("meteo_target_table", default_var="weather_facts")

        hook   = PostgresHook(postgres_conn_id=CONN_POSTGRES)
        conn   = hook.get_conn()
        cursor = conn.cursor()

        insert_sql = f"""
            INSERT INTO {schema}.{table}
                (city, measure_date, measure_hour,
                 temperature_c, precipitation_mm, windspeed_kmh, weathercode,
                 airflow_run_id)
            VALUES
                (%(city)s, %(measure_date)s, %(measure_hour)s,
                 %(temperature_c)s, %(precipitation_mm)s, %(windspeed_kmh)s,
                 %(weathercode)s, %(airflow_run_id)s)
            ON CONFLICT (city, measure_date, measure_hour)
            DO NOTHING
        """

        rows_inserted = 0
        rows_skipped  = 0

        for row in rows:
            cursor.execute(insert_sql, row)
            if cursor.rowcount == 1:
                rows_inserted += 1
            else:
                rows_skipped += 1

        conn.commit()
        cursor.close()
        conn.close()

        log.info("Chargement terminé — %d insérées, %d ignorées (doublons)",
                 rows_inserted, rows_skipped)

        return {
            "rows_received": transform_result["rows_received"],
            "rows_inserted": rows_inserted,
            "rows_skipped":  rows_skipped,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 5 — Traçabilité
    # ─────────────────────────────────────────────────────────────────────────
    @task(task_id="write_ingestion_log")
    def write_ingestion_log(load_result: dict, minio_result: dict, **context) -> None:
        """
        Insère une ligne de suivi dans ingestion_log.
        Inclut le chemin MinIO raw pour pouvoir auditer la donnée source.
        """
        cities: list[str]   = json.loads(
            Variable.get("meteo_cities", default_var='["Paris", "Lyon", "Marseille", "Bordeaux", "Lille"]')
        )
        schema: str         = Variable.get("meteo_schema", default_var="public")
        tracking_table: str = Variable.get("meteo_tracking_table", default_var="ingestion_log")

        dag_id              = context["dag"].dag_id
        run_id              = context["run_id"]
        data_interval_start = context.get("data_interval_start")
        data_interval_end   = context.get("data_interval_end")
        raw_path            = f"s3://{minio_result['bucket']}/{minio_result['run_id']}/"

        hook   = PostgresHook(postgres_conn_id=CONN_POSTGRES)
        conn   = hook.get_conn()
        cursor = conn.cursor()

        insert_sql = f"""
            INSERT INTO {schema}.{tracking_table}
                (dag_id, run_id,
                 data_interval_start, data_interval_end,
                 source, cities_requested,
                 rows_received, rows_inserted, rows_skipped,
                 status, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """

        cursor.execute(insert_sql, (
            dag_id,
            run_id,
            data_interval_start,
            data_interval_end,
            f"open-meteo | raw={raw_path}",
            json.dumps(cities),
            load_result["rows_received"],
            load_result["rows_inserted"],
            load_result["rows_skipped"],
            "success",
        ))

        conn.commit()
        cursor.close()
        conn.close()

        log.info(
            "Log d'ingestion écrit — run=%s | raw=%s | insérées=%d | ignorées=%d",
            run_id, raw_path, load_result["rows_inserted"], load_result["rows_skipped"],
        )

    # ─── Chaînage ────────────────────────────────────────────────────────────
    fetched     = fetch_weather()
    minio_saved = save_to_minio(fetched)
    transformed = transform_weather(minio_saved)
    loaded      = load_weather(transformed)
    write_ingestion_log(loaded, minio_saved)


tp2b_meteo_pipeline()
