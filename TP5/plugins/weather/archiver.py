"""
plugins/weather/archiver.py
Archivage des données brutes dans MinIO (object store S3-compatible).

Structure des clés S3 :
    weather-raw/
        <execution_date>/
            <city_slug>.json      ← une clé par ville, même nom à chaque run
                                    → écrasement idempotent sur relance
"""

import json
import logging
import os

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)

# ─── Config MinIO lue depuis les variables d'environnement ───────────────────
# Injectées par docker-compose, ou surchargées via Airflow Variables si besoin
MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS    = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET    = os.environ.get("MINIO_BUCKET",     "weather-raw")


def _get_s3_client():
    """
    Retourne un client boto3 configuré pour MinIO.

    Pourquoi signature_version='s3' ?
    MinIO utilise la signature AWS Signature V4 mais certaines opérations
    nécessitent de forcer le path-style addressing (pas de virtual-host).
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # obligatoire même si MinIO ignore la région
    )


def archive_to_minio(raw_data: list[dict], execution_date: str) -> list[str]:
    """
    Upload chaque réponse brute en JSON dans MinIO.

    Clé S3 : <execution_date>/<city_slug>.json
    → La clé est DÉTERMINISTE : même ville + même date = même clé.
    → Une relance écrase le même objet : idempotence garantie.

    Args:
        raw_data        : liste de dicts produits par extractor.fetch_weather
        execution_date  : YYYY-MM-DD (logical date Airflow)

    Returns:
        Liste des clés S3 uploadées (ex: ["2025-06-10/paris.json", ...])
    """
    s3 = _get_s3_client()
    s3_keys = []

    for record in raw_data:
        city_slug = (
            record["city"]
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
        s3_key = f"{execution_date}/{city_slug}.json"

        payload = json.dumps(record, ensure_ascii=False, indent=2).encode("utf-8")

        try:
            s3.put_object(
                Bucket=MINIO_BUCKET,
                Key=s3_key,
                Body=payload,
                ContentType="application/json",
            )
            s3_keys.append(s3_key)
            logger.info(
                f"[ARCHIVER] ☁️  Uploadé : s3://{MINIO_BUCKET}/{s3_key} "
                f"({len(payload)} bytes)"
            )
        except Exception as e:
            logger.error(f"[ARCHIVER] ❌ Échec upload {s3_key} : {e}")
            raise

    logger.info(f"[ARCHIVER] {len(s3_keys)} fichier(s) archivé(s) dans MinIO")
    return s3_keys


def download_from_minio(s3_keys: list[str]) -> list[dict]:
    """
    Télécharge et désérialise des objets JSON depuis MinIO.

    Utilisé par le transformer pour lire les données brutes archivées
    plutôt que de les récupérer depuis XCom — le pipeline lit donc
    bien depuis l'archive, pas depuis la mémoire.

    Args:
        s3_keys: liste de clés S3 (ex: ["2025-06-10/paris.json"])

    Returns:
        Liste de dicts (données brutes désérialisées)
    """
    s3 = _get_s3_client()
    records = []

    for s3_key in s3_keys:
        try:
            response = s3.get_object(Bucket=MINIO_BUCKET, Key=s3_key)
            content  = response["Body"].read().decode("utf-8")
            record   = json.loads(content)
            records.append(record)
            logger.info(f"[ARCHIVER] 📥 Téléchargé : s3://{MINIO_BUCKET}/{s3_key}")
        except Exception as e:
            logger.error(f"[ARCHIVER] ❌ Échec download {s3_key} : {e}")
            raise

    return records
