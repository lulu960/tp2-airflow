"""
plugins/weather/transformer.py
Transformation et normalisation des données météo brutes.

Source des données : MinIO (via archiver.download_from_minio)
→ Le transformer ne consomme PAS le XCom de fetch_weather,
  il lit les fichiers archivés → pipeline vraiment linéaire.
"""

import logging
from typing import Any

from include.weather.archiver import download_from_minio

logger = logging.getLogger(__name__)

# Codes météo WMO → libellé humain (subset)
WMO_CODES = {
    0:  "Ciel dégagé",
    1:  "Principalement dégagé",
    2:  "Partiellement nuageux",
    3:  "Couvert",
    45: "Brouillard",
    48: "Brouillard givrant",
    51: "Bruine légère",
    61: "Pluie faible",
    63: "Pluie modérée",
    65: "Pluie forte",
    71: "Neige légère",
    80: "Averses légères",
    95: "Orage",
}


def transform_data(s3_keys: list[str], execution_date: str) -> list[dict]:
    """
    Télécharge les JSONs bruts depuis MinIO puis les transforme
    en enregistrements plats prêts pour PostgreSQL.

    Pourquoi lire depuis MinIO et pas depuis XCom ?
    - XCom est limité en taille (~48 KB par défaut dans la DB Airflow)
    - MinIO est la source de vérité : une relance partielle (depuis
      transform_data) relit les mêmes fichiers → comportement reproductible
    - Le pipeline est vraiment linéaire : fetch → archive → transform

    Args:
        s3_keys        : liste de clés S3 poussées par archive_to_minio
        execution_date : YYYY-MM-DD

    Returns:
        Liste de dicts normalisés (un par heure par ville)
    """
    # ── 1. Téléchargement depuis MinIO ────────────────────────────────────────
    logger.info(f"[TRANSFORMER] Lecture de {len(s3_keys)} fichier(s) depuis MinIO")
    raw_data = download_from_minio(s3_keys)

    # ── 2. Transformation ─────────────────────────────────────────────────────
    records = []

    for city_data in raw_data:
        city      = city_data["city"]
        latitude  = city_data["latitude"]
        longitude = city_data["longitude"]
        hourly    = city_data["raw_response"].get("hourly", {})

        times          = hourly.get("time", [])
        temperatures   = hourly.get("temperature_2m", [])
        precipitations = hourly.get("precipitation", [])
        windspeeds     = hourly.get("windspeed_10m", [])
        humidities     = hourly.get("relativehumidity_2m", [])
        apparent_temps = hourly.get("apparent_temperature", [])
        weathercodes   = hourly.get("weathercode", [])

        logger.info(f"[TRANSFORMER] {city} — {len(times)} entrées horaires")

        for i, ts in enumerate(times):
            temp     = _safe_get(temperatures, i)
            precip   = _safe_get(precipitations, i)
            wind     = _safe_get(windspeeds, i)
            humidity = _safe_get(humidities, i)
            apparent = _safe_get(apparent_temps, i)
            wcode    = _safe_get(weathercodes, i)

            records.append({
                # Clé composite pour UPSERT idempotent
                "city":             city,
                "observation_time": ts,
                "execution_date":   execution_date,
                # Coordonnées
                "latitude":         latitude,
                "longitude":        longitude,
                # Métriques
                "temperature_c":    temp,
                "apparent_temp_c":  apparent,
                "precipitation_mm": precip,
                "windspeed_kmh":    wind,
                "humidity_pct":     humidity,
                "weather_code":     wcode,
                "weather_label":    WMO_CODES.get(wcode, f"Code {wcode}") if wcode is not None else None,
                # Flag qualité pré-calculé
                "has_nulls":        any(v is None for v in [temp, precip, wind, humidity]),
            })

    logger.info(f"[TRANSFORMER] ✅ {len(records)} enregistrements produits")
    return records


def _safe_get(lst: list, idx: int) -> Any:
    """Retourne lst[idx] ou None si hors bornes."""
    try:
        return lst[idx]
    except IndexError:
        return None
