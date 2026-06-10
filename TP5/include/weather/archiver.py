"""
include/weather/archiver.py
Archivage des données brutes dans MinIO (object store S3-compatible).

Configuration lue depuis les Variables Airflow :
    weather_minio_endpoint : URL de l'API MinIO  (ex: http://minio:9000)
    weather_minio_bucket   : Nom du bucket       (ex: weather-raw)
    weather_minio_access   : Access key
    weather_minio_secret   : Secret key
"""

import json
import logging

import boto3
from botocore.client import Config
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ─── Valeurs par défaut ───────────────────────────────────────────────────────
_DEFAULT_ENDPOINT = "http://minio:9000"
_DEFAULT_BUCKET   = "weather-raw"
_DEFAULT_ACCESS   = "minioadmin"
_DEFAULT_SECRET   = "minioadmin"


def _get_s3_client():
    """Retourne un client boto3 configuré depuis les Variables Airflow."""
    endpoint = Variable.get("weather_minio_endpoint", default_var=_DEFAULT_ENDPOINT)
    access   = Variable.get("weather_minio_access",   default_var=_DEFAULT_ACCESS)
    secret   = Variable.get("weather_minio_secret",   default_var=_DEFAULT_SECRET)

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _get_bucket() -> str:
    return Variable.get("weather_minio_bucket", default_var=_DEFAULT_BUCKET)


def archive_to_minio(raw_data: list[dict], execution_date: str) -> list[str]:
    """
    Upload chaque réponse brute en JSON dans MinIO.

    Clé S3 : <execution_date>/<city_slug>.json
    Clé déterministe → relance = écrasement du même objet → idempotence.

    Returns:
        Liste des clés S3 uploadées
    """
    s3     = _get_s3_client()
    bucket = _get_bucket()
    s3_keys = []

    for record in raw_data:
        city_slug = record["city"].lower().replace(" ", "_").replace("-", "_")
        s3_key    = f"{execution_date}/{city_slug}.json"
        payload   = json.dumps(record, ensure_ascii=False, indent=2).encode("utf-8")

        try:
            s3.put_object(Bucket=bucket, Key=s3_key, Body=payload, ContentType="application/json")
            s3_keys.append(s3_key)
            logger.info(f"[ARCHIVER] ☁️  Uploadé : s3://{bucket}/{s3_key} ({len(payload)} bytes)")
        except Exception as e:
            logger.error(f"[ARCHIVER] ❌ Échec upload {s3_key} : {e}")
            raise

    logger.info(f"[ARCHIVER] {len(s3_keys)} fichier(s) archivé(s) dans MinIO")
    return s3_keys


def download_from_minio(s3_keys: list[str]) -> list[dict]:
    """
    Télécharge et désérialise des objets JSON depuis MinIO.

    Args:
        s3_keys : liste de clés S3

    Returns:
        Liste de dicts désérialisés
    """
    s3     = _get_s3_client()
    bucket = _get_bucket()
    records = []

    for s3_key in s3_keys:
        try:
            response = s3.get_object(Bucket=bucket, Key=s3_key)
            content  = response["Body"].read().decode("utf-8")
            records.append(json.loads(content))
            logger.info(f"[ARCHIVER] 📥 Téléchargé : s3://{bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"[ARCHIVER] ❌ Échec download {s3_key} : {e}")
            raise

    return records
