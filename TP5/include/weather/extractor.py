"""
plugins/weather/extractor.py
Extraction des données météo depuis l'API Open-Meteo.

Toute la configuration est lue depuis les Variables Airflow :
    weather_api_url       : URL de base de l'API
    weather_hourly_vars   : champs horaires demandés (liste JSON)
    weather_past_days     : nb de jours passés à récupérer
    weather_forecast_days : nb de jours de prévision
    weather_api_timeout   : timeout HTTP en secondes
"""

import logging
import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ─── Valeurs par défaut (utilisées si la Variable Airflow n'existe pas) ───────
_DEFAULT_API_URL       = "https://api.open-meteo.com/v1/forecast"
_DEFAULT_HOURLY_VARS   = [
    "temperature_2m", "precipitation", "windspeed_10m",
    "relativehumidity_2m", "apparent_temperature", "weathercode",
]
_DEFAULT_PAST_DAYS      = 1
_DEFAULT_FORECAST_DAYS  = 1
_DEFAULT_TIMEOUT        = 15


def fetch_weather(cities: list[dict], execution_date: str) -> list[dict]:
    """
    Récupère les données météo pour chaque ville via Open-Meteo.

    Args:
        cities         : liste de dicts {name, latitude, longitude}
        execution_date : YYYY-MM-DD

    Returns:
        Liste de dicts bruts {city, latitude, longitude, raw_response}
    """
    # ── Lecture de la config depuis Airflow Variables ─────────────────────────
    api_url       = Variable.get("weather_api_url",       default_var=_DEFAULT_API_URL)
    hourly_vars   = Variable.get("weather_hourly_vars",   default_var=None, deserialize_json=True) or _DEFAULT_HOURLY_VARS
    past_days     = int(Variable.get("weather_past_days",     default_var=_DEFAULT_PAST_DAYS))
    forecast_days = int(Variable.get("weather_forecast_days", default_var=_DEFAULT_FORECAST_DAYS))
    timeout       = int(Variable.get("weather_api_timeout",   default_var=_DEFAULT_TIMEOUT))

    logger.info(f"[EXTRACTOR] API={api_url} | past_days={past_days} | forecast_days={forecast_days} | timeout={timeout}s")
    logger.info(f"[EXTRACTOR] Champs demandés : {hourly_vars}")

    results = []

    for city in cities:
        name = city["name"]
        lat  = city["latitude"]
        lon  = city["longitude"]

        params = {
            "latitude":      lat,
            "longitude":     lon,
            "hourly":        ",".join(hourly_vars),
            "past_days":     past_days,
            "forecast_days": forecast_days,
            "timezone":      "Europe/Paris",
        }

        logger.info(f"[EXTRACTOR] Requête pour {name} ({lat}, {lon})")

        try:
            response = requests.get(api_url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            results.append({
                "city":           name,
                "latitude":       lat,
                "longitude":      lon,
                "execution_date": execution_date,
                "raw_response":   data,
            })
            logger.info(f"[EXTRACTOR] ✅ {name} — {len(data.get('hourly', {}).get('time', []))} heures reçues")

        except requests.exceptions.Timeout:
            logger.error(f"[EXTRACTOR] ⏱️ Timeout ({timeout}s) pour {name}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"[EXTRACTOR] ❌ HTTP {e.response.status_code} pour {name}")
            raise
        except Exception as e:
            logger.error(f"[EXTRACTOR] ❌ Erreur inattendue pour {name} : {e}")
            raise

    return results
