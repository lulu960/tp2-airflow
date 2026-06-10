"""
plugins/weather/extractor.py
Extraction des données météo depuis l'API Open-Meteo.
"""

import logging
import requests

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Variables météo demandées à l'API
HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "windspeed_10m",
    "relativehumidity_2m",
    "apparent_temperature",
    "weathercode",
]


def fetch_weather(cities: list[dict], execution_date: str) -> list[dict]:
    """
    Récupère les données météo pour chaque ville via Open-Meteo.

    Args:
        cities: liste de dicts avec keys 'name', 'latitude', 'longitude'
        execution_date: date d'exécution au format YYYY-MM-DD (pour past_days)

    Returns:
        Liste de dicts bruts {city, latitude, longitude, raw_response}
    """
    results = []

    for city in cities:
        name = city["name"]
        lat  = city["latitude"]
        lon  = city["longitude"]

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(HOURLY_VARS),
            "past_days": 1,       # données J-1 pour idempotence sur date passée
            "forecast_days": 1,
            "timezone": "Europe/Paris",
        }

        logger.info(f"[EXTRACTOR] Requête Open-Meteo pour {name} ({lat}, {lon})")

        try:
            response = requests.get(OPEN_METEO_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            results.append({
                "city": name,
                "latitude": lat,
                "longitude": lon,
                "execution_date": execution_date,
                "raw_response": data,
            })
            logger.info(f"[EXTRACTOR] ✅ {name} — {len(data.get('hourly', {}).get('time', []))} heures reçues")

        except requests.exceptions.Timeout:
            logger.error(f"[EXTRACTOR] ⏱️ Timeout pour {name}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"[EXTRACTOR] ❌ HTTP {e.response.status_code} pour {name}")
            raise
        except Exception as e:
            logger.error(f"[EXTRACTOR] ❌ Erreur inattendue pour {name} : {e}")
            raise

    return results
