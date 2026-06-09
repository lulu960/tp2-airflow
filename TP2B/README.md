# TP 2B — Pipeline Airflow : Open-Meteo → MinIO → PostgreSQL

## Architecture

```
fetch_weather → save_to_minio → transform_weather → load_weather → write_ingestion_log
```

| Tâche | Rôle |
|---|---|
| `fetch_weather` | Appelle l'API Open-Meteo pour chaque ville |
| `save_to_minio` | Écrit le JSON brut dans MinIO (`raw-meteo/{run_id}/{city}.json`) |
| `transform_weather` | Lit depuis MinIO, structure les données |
| `load_weather` | INSERT dans `weather_facts` (ON CONFLICT DO NOTHING) |
| `write_ingestion_log` | Traçabilité dans `ingestion_log` (avec le chemin MinIO raw) |

## Pourquoi MinIO entre fetch et transform ?

- **Couche raw** : le JSON brut de l'API est conservé tel quel. Si la transformation a un bug, on rejoue uniquement `transform_weather` sans re-appeler l'API.
- **Auditabilité** : on peut inspecter ce qu'on a reçu de la source à n'importe quel moment.
- **Pattern production** : en prod, le raw landing en object storage (S3, GCS, Azure Blob) est standard. MinIO est l'équivalent self-hosted.

## Lancement

```bash
mkdir -p logs plugins
docker compose up -d
# Attendre ~90s (MinIO + Airflow init), puis :
#   http://localhost:8080  →  Airflow  (admin / admin)
#   http://localhost:9001  →  MinIO UI (minio_access_key / minio_secret_key)
```

## Buckets MinIO

| Bucket | Rôle |
|---|---|
| `raw-meteo` | JSON bruts par run : `{run_id}/{city}.json` |
| `processed-meteo` | Disponible pour staging / exports futurs |

## Variables Airflow (Admin > Variables)

| Variable | Défaut | Rôle |
|---|---|---|
| `meteo_cities` | `["Paris","Lyon",...]` | Villes cibles |
| `meteo_fields` | `temperature_2m,...` | Champs API |
| `meteo_raw_bucket` | `raw-meteo` | Bucket MinIO raw |
| `meteo_processed_bucket` | `processed-meteo` | Bucket MinIO processed |
| `meteo_schema` | `public` | Schéma PostgreSQL |
| `meteo_target_table` | `weather_facts` | Table cible |
| `meteo_tracking_table` | `ingestion_log` | Table de suivi |

## Vérifier les données

```bash
# PostgreSQL
psql -h localhost -p 5433 -U meteo_user -d meteo_db
SELECT city, measure_date, measure_hour, temperature_c FROM public.weather_facts LIMIT 20;
SELECT * FROM public.ingestion_log ORDER BY started_at DESC;

# MinIO — lister les fichiers raw d'un run
docker compose exec minio-init \
  mc ls local/raw-meteo/ --recursive
```

## Arrêt

```bash
docker compose down       # arrête
docker compose down -v    # arrête + supprime les volumes
```
