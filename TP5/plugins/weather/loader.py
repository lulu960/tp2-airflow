"""
plugins/weather/loader.py
Chargement dans PostgreSQL avec UPSERT idempotent.
"""

import json
import logging
from datetime import datetime

import psycopg2
from airflow.hooks.postgres_hook import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "weather_postgres"


def _get_conn():
    """Retourne une connexion psycopg2 via le Hook Airflow."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_conn()


def load_to_postgres(records: list[dict]) -> tuple[int, int]:
    """
    Charge les enregistrements dans weather_observations via UPSERT.

    La clé de conflit est (city, observation_time) — garantit l'idempotence :
    une relance du DAG sur la même date ne crée PAS de doublons.

    Returns:
        (inserted_count, skipped_count)
    """
    conn = _get_conn()
    cur  = conn.cursor()

    upsert_sql = """
        INSERT INTO weather_observations (
            city, observation_time, execution_date,
            latitude, longitude,
            temperature_c, apparent_temp_c,
            precipitation_mm, windspeed_kmh,
            humidity_pct, weather_code, weather_label,
            loaded_at
        ) VALUES (
            %(city)s, %(observation_time)s, %(execution_date)s,
            %(latitude)s, %(longitude)s,
            %(temperature_c)s, %(apparent_temp_c)s,
            %(precipitation_mm)s, %(windspeed_kmh)s,
            %(humidity_pct)s, %(weather_code)s, %(weather_label)s,
            NOW()
        )
        ON CONFLICT (city, observation_time)
        DO UPDATE SET
            temperature_c     = EXCLUDED.temperature_c,
            apparent_temp_c   = EXCLUDED.apparent_temp_c,
            precipitation_mm  = EXCLUDED.precipitation_mm,
            windspeed_kmh     = EXCLUDED.windspeed_kmh,
            humidity_pct      = EXCLUDED.humidity_pct,
            weather_code      = EXCLUDED.weather_code,
            weather_label     = EXCLUDED.weather_label,
            loaded_at         = NOW()
        RETURNING (xmax = 0) AS inserted
    """

    inserted = 0
    skipped  = 0

    try:
        for rec in records:
            cur.execute(upsert_sql, rec)
            row = cur.fetchone()
            if row and row[0]:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        logger.info(f"[LOADER] ✅ {inserted} insérés, {skipped} mis à jour / ignorés")

    except Exception as e:
        conn.rollback()
        logger.error(f"[LOADER] ❌ Erreur lors du chargement : {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return inserted, skipped


def log_ingestion(
    dag_id: str,
    run_id: str,
    execution_date: str,
    status: str,
    rows_inserted: int,
    rows_skipped: int,
    quality_report: str,
) -> None:
    """Insère une ligne de traçabilité dans ingestion_log."""
    conn = _get_conn()
    cur  = conn.cursor()

    sql = """
        INSERT INTO ingestion_log (
            dag_id, run_id, execution_date,
            status, rows_inserted, rows_skipped,
            quality_report, logged_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """
    try:
        cur.execute(sql, (
            dag_id, run_id, execution_date,
            status, rows_inserted, rows_skipped,
            quality_report,
        ))
        conn.commit()
        logger.info(f"[LOADER] 📋 Ingestion loguée (status={status})")
    except Exception as e:
        conn.rollback()
        logger.error(f"[LOADER] ❌ Erreur log_ingestion : {e}")
        raise
    finally:
        cur.close()
        conn.close()


def log_anomaly(
    dag_id: str,
    run_id: str,
    execution_date: str,
    anomaly_report: str,
) -> None:
    """Insère une ligne d'anomalie qualité dans quality_anomaly_log."""
    conn = _get_conn()
    cur  = conn.cursor()

    sql = """
        INSERT INTO quality_anomaly_log (
            dag_id, run_id, execution_date,
            anomaly_report, logged_at
        ) VALUES (%s, %s, %s, %s, NOW())
    """
    try:
        cur.execute(sql, (dag_id, run_id, execution_date, anomaly_report))
        conn.commit()
        logger.warning(f"[LOADER] ⚠️ Anomalie loguée pour {execution_date}")
    except Exception as e:
        conn.rollback()
        logger.error(f"[LOADER] ❌ Erreur log_anomaly : {e}")
        raise
    finally:
        cur.close()
        conn.close()
