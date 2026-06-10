#!/bin/bash
# scripts/init_variables.sh
# Initialise toutes les Variables Airflow au démarrage
# Exécuté par airflow-init dans le docker-compose

set -e

echo ">>> Initialisation des Variables Airflow..."

# ── Villes ────────────────────────────────────────────────────────────────────
airflow variables set weather_cities \
  '[{"name":"Paris","latitude":48.8566,"longitude":2.3522},{"name":"Clermont-Ferrand","latitude":45.7772,"longitude":3.0870},{"name":"Lyon","latitude":45.764,"longitude":4.8357}]'

# ── API Open-Meteo ────────────────────────────────────────────────────────────
airflow variables set weather_api_url        "https://api.open-meteo.com/v1/forecast"
airflow variables set weather_hourly_vars    '["temperature_2m","precipitation","windspeed_10m","relativehumidity_2m","apparent_temperature","weathercode"]'
airflow variables set weather_past_days      "1"
airflow variables set weather_forecast_days  "1"
airflow variables set weather_api_timeout    "15"

# ── MinIO ─────────────────────────────────────────────────────────────────────
airflow variables set weather_minio_endpoint "http://minio:9000"
airflow variables set weather_minio_bucket   "weather-raw"
airflow variables set weather_minio_access   "minioadmin"
airflow variables set weather_minio_secret   "minioadmin"

# ── Seuils qualité ────────────────────────────────────────────────────────────
airflow variables set weather_qc_temp_min      "-50"
airflow variables set weather_qc_temp_max      "55"
airflow variables set weather_qc_wind_max      "300"
airflow variables set weather_qc_humidity_min  "0"
airflow variables set weather_qc_humidity_max  "100"
airflow variables set weather_qc_null_rate_max "0.10"

# ── Robustesse / Tests ────────────────────────────────────────────────────────
airflow variables set weather_task_timeout_minutes "10"
airflow variables set weather_force_anomaly        "false"

echo ">>> Variables initialisées ✅"
