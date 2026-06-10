"""
DAG : weather_pipeline
Auteur : Lucas
Description : Pipeline météo Open-Meteo industrialisé
              Extract → Archive MinIO → Transform → QC → Branch → Load PG / Skip
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from include.weather.extractor  import fetch_weather
from include.weather.archiver   import archive_to_minio
from include.weather.transformer import transform_data
from include.weather.quality    import check_data_quality
from include.weather.loader     import load_to_postgres, log_anomaly, log_ingestion

logger = logging.getLogger(__name__)

DEFAULT_CITIES = [
    {"name": "Paris",           "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Clermont-Ferrand","latitude": 45.7772, "longitude": 3.0870},
    {"name": "Lyon",            "latitude": 45.7640, "longitude": 4.8357},
]

default_args = {
    "owner": "lucas",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=int(Variable.get("weather_task_timeout_minutes", default_var=10))),
}

# ─── Wrappers des tâches ──────────────────────────────────────────────────────

def _fetch(**context):
    cities = Variable.get("weather_cities", default_var=None, deserialize_json=True) or DEFAULT_CITIES
    logger.info(f"[FETCH] {len(cities)} ville(s) : {[c['name'] for c in cities]}")
    result = fetch_weather(cities, context["ds"])
    context["task_instance"].xcom_push(key="raw_data", value=result)
    logger.info(f"[FETCH] {len(result)} ville(s) extraite(s)")


def _archive(**context):
    raw_data = context["task_instance"].xcom_pull(task_ids="fetch_weather", key="raw_data")
    # ── Archive dans MinIO, récupère les clés S3 ──────────────────────────────
    s3_keys = archive_to_minio(raw_data, context["ds"])
    # ── On pousse UNIQUEMENT les clés (léger) dans XCom, pas les données ──────
    context["task_instance"].xcom_push(key="s3_keys", value=s3_keys)
    logger.info(f"[ARCHIVE] {len(s3_keys)} clé(s) S3 : {s3_keys}")


def _transform(**context):
    # ── Lit les clés S3 depuis XCom (pas les données brutes) ──────────────────
    s3_keys = context["task_instance"].xcom_pull(task_ids="archive_raw_data", key="s3_keys")
    logger.info(f"[TRANSFORM] Lecture depuis MinIO : {s3_keys}")
    # ── Le transformer télécharge lui-même depuis MinIO ───────────────────────
    transformed = transform_data(s3_keys, context["ds"])
    context["task_instance"].xcom_push(key="transformed_data", value=transformed)
    logger.info(f"[TRANSFORM] {len(transformed)} enregistrements")


def _check_quality(**context):
    transformed    = context["task_instance"].xcom_pull(task_ids="transform_data", key="transformed_data")
    force_anomaly  = Variable.get("weather_force_anomaly", default_var="false").lower() == "true"
    is_valid, report = check_data_quality(transformed, force_anomaly=force_anomaly)
    context["task_instance"].xcom_push(key="quality_valid",  value=is_valid)
    context["task_instance"].xcom_push(key="quality_report", value=report)
    if is_valid:
        logger.info(f"[QUALITY] ✅ Valide — {report}")
    else:
        logger.warning(f"[QUALITY] ❌ Anomalie — {report}")


def _branch(**context):
    is_valid = context["task_instance"].xcom_pull(task_ids="check_quality", key="quality_valid")
    logger.info(f"[BRANCH] → {'load_to_postgres' if is_valid else 'skip_load'}")
    return "load_to_postgres" if is_valid else "skip_load"


def _load(**context):
    transformed = context["task_instance"].xcom_pull(task_ids="transform_data", key="transformed_data")
    inserted, skipped = load_to_postgres(transformed)
    context["task_instance"].xcom_push(key="load_inserted", value=inserted)
    context["task_instance"].xcom_push(key="load_skipped",  value=skipped)
    logger.info(f"[LOAD] ✅ {inserted} insérés, {skipped} mis à jour")


def _log_ingestion(**context):
    inserted = context["task_instance"].xcom_pull(task_ids="load_to_postgres", key="load_inserted") or 0
    skipped  = context["task_instance"].xcom_pull(task_ids="load_to_postgres", key="load_skipped")  or 0
    report   = context["task_instance"].xcom_pull(task_ids="check_quality",    key="quality_report") or {}
    log_ingestion(
        dag_id=context["dag"].dag_id, run_id=context["run_id"],
        execution_date=context["ds"], status="success",
        rows_inserted=inserted, rows_skipped=skipped,
        quality_report=str(report),
    )


def _skip_load(**context):
    report = context["task_instance"].xcom_pull(task_ids="check_quality", key="quality_report") or {}
    logger.warning(f"[SKIP] Chargement annulé — {report}")


def _log_anomaly(**context):
    report = context["task_instance"].xcom_pull(task_ids="check_quality", key="quality_report") or {}
    log_anomaly(
        dag_id=context["dag"].dag_id, run_id=context["run_id"],
        execution_date=context["ds"], anomaly_report=str(report),
    )
    logger.warning(f"[LOG_ANOMALY] Tracée : {report}")


# ─── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="weather_pipeline",
    description="Météo Open-Meteo : extract → MinIO → transform → QC → PostgreSQL",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "open-meteo", "minio", "tp5"],
) as dag:

    t_fetch    = PythonOperator(task_id="fetch_weather",    python_callable=_fetch)
    t_archive  = PythonOperator(task_id="archive_raw_data", python_callable=_archive)
    t_transform= PythonOperator(task_id="transform_data",   python_callable=_transform)
    t_quality  = PythonOperator(task_id="check_quality",    python_callable=_check_quality)

    t_branch   = BranchPythonOperator(task_id="quality_branch", python_callable=_branch)

    t_load         = PythonOperator(task_id="load_to_postgres", python_callable=_load)
    t_log_ingestion= PythonOperator(task_id="log_ingestion",    python_callable=_log_ingestion)

    t_skip         = PythonOperator(task_id="skip_load",    python_callable=_skip_load)
    t_log_anomaly  = PythonOperator(task_id="log_anomaly",  python_callable=_log_anomaly)

    t_end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ─── Dépendances ─────────────────────────────────────────────────────────
    t_fetch >> t_archive >> t_transform >> t_quality >> t_branch
    t_branch >> t_load >> t_log_ingestion >> t_end
    t_branch >> t_skip >> t_log_anomaly   >> t_end
