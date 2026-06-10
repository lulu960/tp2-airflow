"""
include/weather/quality.py
Contrôles qualité sur les données météo transformées.

Seuils configurables via Variables Airflow :
    weather_qc_temp_min      : température minimale acceptée (°C)
    weather_qc_temp_max      : température maximale acceptée (°C)
    weather_qc_wind_max      : vitesse du vent maximale acceptée (km/h)
    weather_qc_humidity_min  : humidité minimale acceptée (%)
    weather_qc_humidity_max  : humidité maximale acceptée (%)
    weather_qc_null_rate_max : taux de nulls maximal accepté (0.0 → 1.0)
"""

import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ─── Valeurs par défaut ───────────────────────────────────────────────────────
_DEFAULT_TEMP_MIN      = -50.0
_DEFAULT_TEMP_MAX      =  55.0
_DEFAULT_WIND_MAX      = 300.0
_DEFAULT_HUMIDITY_MIN  =   0.0
_DEFAULT_HUMIDITY_MAX  = 100.0
_DEFAULT_NULL_RATE_MAX =   0.10


def _get_thresholds() -> dict:
    """Charge les seuils qualité depuis les Variables Airflow."""
    return {
        "temp_min":      float(Variable.get("weather_qc_temp_min",      default_var=_DEFAULT_TEMP_MIN)),
        "temp_max":      float(Variable.get("weather_qc_temp_max",      default_var=_DEFAULT_TEMP_MAX)),
        "wind_max":      float(Variable.get("weather_qc_wind_max",      default_var=_DEFAULT_WIND_MAX)),
        "humidity_min":  float(Variable.get("weather_qc_humidity_min",  default_var=_DEFAULT_HUMIDITY_MIN)),
        "humidity_max":  float(Variable.get("weather_qc_humidity_max",  default_var=_DEFAULT_HUMIDITY_MAX)),
        "null_rate_max": float(Variable.get("weather_qc_null_rate_max", default_var=_DEFAULT_NULL_RATE_MAX)),
    }


def check_data_quality(records: list[dict], force_anomaly: bool = False) -> tuple[bool, dict]:
    """
    Contrôle la qualité des données transformées.

    Règles :
        1. Dataset non vide
        2. Taux de nulls < null_rate_max
        3. Température dans [temp_min, temp_max]
        4. Vitesse du vent dans [0, wind_max]
        5. Humidité dans [humidity_min, humidity_max]

    Args:
        records       : liste de dicts produits par transformer
        force_anomaly : injecte une anomalie artificielle si True (tests)

    Returns:
        (is_valid, report)
    """
    t = _get_thresholds()
    logger.info(f"[QUALITY] Seuils actifs : {t}")

    report = {"total_records": len(records), "errors": [], "warnings": [], "null_rate": 0.0}

    if not records:
        report["errors"].append("Dataset vide")
        return False, report

    if force_anomaly:
        records[0]["temperature_c"] = 999.0
        logger.warning("[QUALITY] ⚠️ Anomalie artificielle injectée (temperature_c=999)")

    total      = len(records)
    null_count = 0
    temp_errors = []
    wind_errors = []
    hum_errors  = []

    for rec in records:
        city = rec.get("city", "?")
        ts   = rec.get("observation_time", "?")

        if rec.get("has_nulls"):
            null_count += 1

        temp = rec.get("temperature_c")
        if temp is not None and (temp < t["temp_min"] or temp > t["temp_max"]):
            temp_errors.append(f"{city}@{ts}: {temp}°C hors [{t['temp_min']}, {t['temp_max']}]")

        wind = rec.get("windspeed_kmh")
        if wind is not None and (wind < 0 or wind > t["wind_max"]):
            wind_errors.append(f"{city}@{ts}: {wind} km/h hors [0, {t['wind_max']}]")

        hum = rec.get("humidity_pct")
        if hum is not None and (hum < t["humidity_min"] or hum > t["humidity_max"]):
            hum_errors.append(f"{city}@{ts}: {hum}% hors [{t['humidity_min']}, {t['humidity_max']}]")

    null_rate = null_count / total
    report["null_rate"] = round(null_rate, 4)

    if null_rate > t["null_rate_max"]:
        report["errors"].append(f"Taux de nulls {null_rate:.1%} > {t['null_rate_max']:.0%}")
    if temp_errors:
        report["errors"].append(f"{len(temp_errors)} erreur(s) température : {temp_errors[:3]}")
    if wind_errors:
        report["errors"].append(f"{len(wind_errors)} erreur(s) vent : {wind_errors[:3]}")
    if hum_errors:
        report["errors"].append(f"{len(hum_errors)} erreur(s) humidité : {hum_errors[:3]}")

    is_valid = len(report["errors"]) == 0

    if is_valid:
        logger.info(f"[QUALITY] ✅ {total} enregistrements valides (null_rate={null_rate:.1%})")
    else:
        logger.error(f"[QUALITY] ❌ {len(report['errors'])} erreur(s) : {report['errors']}")

    return is_valid, report
