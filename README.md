# TP Airflow — Vue d'ensemble

Ce dépôt contient trois projets Airflow indépendants, chacun avec sa propre stack Docker.

```
tp/
├── TP_1_2/   ← TP2 + TP2A — PokéAPI (Airflow + PostgreSQL)
├── TP2B/     ← TP2B — Open-Meteo + MinIO + PostgreSQL (pipeline basique)
└── TP5/      ← TP5 — Open-Meteo industrialisé : QC, branchement, traçabilité
```

---

## TP_1_2 — Pokémon (TP2 + TP2A)

**Source :** [PokeAPI](https://pokeapi.co)  
**Stack :** Airflow + PostgreSQL

### Deux DAGs dans ce projet

| DAG | Description |
|---|---|
| `tp2_pokemon_pipeline` | Pipeline de base — extraction, transformation, rapport dans les logs |
| `tp2a_pokemon_ingestion` | Pipeline structuré — extraction par génération, transformation, chargement PostgreSQL |

### Pipeline TP2

```
extract_pokemon → transform_pokemon → report_pokemon
```

Calcule un score global (`hp + attack + defense + speed`) sur les 20 premiers Pokémon et affiche le classement dans les logs. Les données transitent via XCom.

### Pipeline TP2A

```
extract_pokemon → transform_pokemon → report_pokemon → load_pokemon
```

3 générations de Pokémon jouent le rôle de 3 sources distinctes. Les données sont chargées dans une table `pokemon_stats` avec déduplication (`ON CONFLICT`).

### Lancer

```bash
cd TP_1_2
docker compose up -d
# http://localhost:8080  →  admin / admin
```

---

## TP2B — Météo basique (Open-Meteo → MinIO → PostgreSQL)

**Source :** [Open-Meteo](https://open-meteo.com) (API publique, sans clé)  
**Stack :** Airflow + PostgreSQL + MinIO

### Pipeline

```
fetch_weather → save_to_minio → transform_weather → load_weather → write_ingestion_log
```

| Tâche | Rôle |
|---|---|
| `fetch_weather` | Appelle Open-Meteo pour chaque ville (Paris, Lyon, Marseille…) |
| `save_to_minio` | Stocke le JSON brut dans MinIO (`raw-meteo/{run_id}/{ville}.json`) |
| `transform_weather` | Lit depuis MinIO, structure les données |
| `load_weather` | INSERT dans `weather_facts` avec déduplication |
| `write_ingestion_log` | Écrit une ligne de traçabilité dans `ingestion_log` |

MinIO sert de **couche raw** entre l'extraction et la transformation : si la transformation a un bug, on la rejoue sans re-appeler l'API.

### Lancer

```bash
cd TP2B
mkdir -p logs plugins
docker compose up -d
# http://localhost:8080  →  Airflow   (admin / admin)
# http://localhost:9001  →  MinIO UI  (minioadmin / minioadmin)
# psql -h localhost -p 5433 -U meteo_user -d meteo_db
```

### Paramétrage (Variables Airflow — Admin > Variables)

| Variable | Défaut | Rôle |
|---|---|---|
| `meteo_cities` | `["Paris","Lyon","Marseille","Bordeaux","Lille"]` | Villes à ingérer |
| `meteo_fields` | `temperature_2m,precipitation,...` | Champs API |
| `meteo_raw_bucket` | `raw-meteo` | Bucket MinIO brut |
| `meteo_target_table` | `weather_facts` | Table cible |
| `meteo_tracking_table` | `ingestion_log` | Table de suivi |

---

## TP5 — Météo industrialisée (Open-Meteo → MinIO → QC → PostgreSQL)

**Source :** [Open-Meteo](https://open-meteo.com) (API publique, sans clé)  
**Stack :** Airflow + PostgreSQL + MinIO

### Description

Pipeline d'ingestion météo **industrialisé** : robustesse, contrôle qualité, branchement
conditionnel, traçabilité complète et idempotence garantie à chaque étape.

### Pipeline

```
fetch_weather
    │
    ▼
archive_raw_data  ──→  MinIO : weather-raw/<date>/<ville>.json
    │  (XCom : clés S3 uniquement)
    ▼
transform_data  ←── lit depuis MinIO
    │
    ▼
check_quality
    │
    ▼
quality_branch ──────────────────────────────┐
    │ (valid)                                  │ (invalid)
    ▼                                          ▼
load_to_postgres                           skip_load
    │                                          │
    ▼                                          ▼
log_ingestion                            log_anomaly
    └──────────────────┬────────────────────────┘
                       ▼
                      end
```

### Tâches du DAG

| Task ID | Opérateur | Rôle |
|---|---|---|
| `fetch_weather` | PythonOperator | Appelle Open-Meteo pour chaque ville, retries=3, timeout=10min |
| `archive_raw_data` | PythonOperator | Upload JSON brut dans MinIO (clé déterministe → idempotence) |
| `transform_data` | PythonOperator | Télécharge depuis MinIO, normalise en enregistrements horaires |
| `check_quality` | PythonOperator | 5 règles : nulls, température, vent, humidité, dataset non vide |
| `quality_branch` | BranchPythonOperator | Route vers load ou skip selon le résultat qualité |
| `load_to_postgres` | PythonOperator | UPSERT idempotent sur `weather_observations` |
| `log_ingestion` | PythonOperator | Trace le succès dans `ingestion_log` |
| `skip_load` | PythonOperator | Log le motif de skip |
| `log_anomaly` | PythonOperator | Trace l'anomalie dans `quality_anomaly_log` |
| `end` | EmptyOperator | Convergence des deux branches |

### Variables Airflow utilisées

| Variable | Type | Description | Défaut |
|---|---|---|---|
| `weather_cities` | JSON | Liste des villes `[{name, latitude, longitude}]` | Paris, Clermont-Ferrand, Lyon |
| `weather_force_anomaly` | String | `"true"` pour simuler une anomalie qualité | `"false"` |

### Connexions Airflow utilisées

| Conn ID | Type | Description |
|---|---|---|
| `weather_postgres` | PostgreSQL | Base météo (host: postgres, db: weather, user: weather) |
| `weather_minio` | Amazon S3 | MinIO endpoint http://minio:9000 (minioadmin / minioadmin) |

### Contrôles qualité

| Règle | Seuil | Action si violation |
|---|---|---|
| Dataset non vide | > 0 enregistrements | Erreur → skip |
| Taux de nulls | ≤ 10 % | Erreur → skip |
| Température | [-50 °C, +55 °C] | Erreur → skip |
| Vitesse du vent | [0, 300 km/h] | Erreur → skip |
| Humidité | [0 %, 100 %] | Erreur → skip |

### Stratégie d'idempotence

- **MinIO** : clé S3 déterministe `<date>/<ville>.json` — relance = écrasement du même objet
- **PostgreSQL** : `ON CONFLICT (city, observation_time) DO UPDATE` — pas de doublon
- Le transformer lit toujours depuis MinIO : une relance partielle depuis `transform_data` relit les mêmes fichiers archivés

### Tables PostgreSQL

| Table | Rôle |
|---|---|
| `weather_observations` | Données horaires par ville — UNIQUE `(city, observation_time)` |
| `ingestion_log` | Trace chaque run réussi (rows_inserted, rows_skipped, quality_report) |
| `quality_anomaly_log` | Trace les runs bloqués par une anomalie qualité |

### Lancer

```bash
cd TP5
docker compose up -d
# http://localhost:8080  →  Airflow   (admin / admin)
# http://localhost:9001  →  MinIO UI  (minioadmin / minioadmin)

# Créer la connexion PostgreSQL
docker compose exec airflow-webserver airflow connections add weather_postgres \
  --conn-type postgres --conn-host postgres --conn-schema weather \
  --conn-login weather --conn-password weather --conn-port 5432

# Déclencher le DAG (cas nominal)
docker compose exec airflow-webserver airflow dags trigger weather_pipeline

# Simuler une anomalie qualité
docker compose exec airflow-webserver airflow variables set weather_force_anomaly true
docker compose exec airflow-webserver airflow dags trigger weather_pipeline

# Tester l'idempotence (deuxième run sur même date)
docker compose exec airflow-webserver airflow dags trigger weather_pipeline
```

### Cas à démontrer

- **Nominal** : graph view tout en vert, données dans `weather_observations`
- **Anomalie** : `skip_load` + `log_anomaly` en vert, `load_to_postgres` skippé, ligne dans `quality_anomaly_log`
- **Idempotence** : deux runs successifs → même nombre de lignes, `rows_skipped > 0` dans `ingestion_log`

---

## Comparatif des trois projets

| | TP_1_2 (Pokémon) | TP2B (Météo basique) | TP5 (Météo industrialisée) |
|---|---|---|---|
| Source | PokéAPI | Open-Meteo | Open-Meteo |
| Stockage intermédiaire | XCom | MinIO | MinIO (clé déterministe) |
| Base cible | `pokemon_stats` | `weather_facts` | `weather_observations` |
| Contrôle qualité | — | — | ✅ 5 règles + branchement |
| Branchement conditionnel | — | — | ✅ BranchPythonOperator |
| Traçabilité | — | `ingestion_log` | `ingestion_log` + `quality_anomaly_log` |
| Idempotence | ON CONFLICT DO NOTHING | ON CONFLICT DO NOTHING | ON CONFLICT DO UPDATE + clé S3 fixe |
| Retries / timeout | — | — | ✅ retries=3, timeout=10min |
| Services Docker | Airflow + PG | Airflow + PG + MinIO | Airflow + PG + MinIO |

---

## Identifiants des services

| Projet | Service | URL | Login | Mot de passe |
|---|---|---|---|---|
| TP_1_2 | Airflow UI | http://localhost:8080 | `admin` | `admin` |
| TP2B | Airflow UI | http://localhost:8080 | `admin` | `admin` |
| TP2B | MinIO UI | http://localhost:9001 | `minioadmin` | `minioadmin` |
| TP2B | PostgreSQL | localhost:5433 | `meteo_user` | *(voir compose)* |
| TP5 | Airflow UI | http://localhost:8080 | `admin` | `admin` |
| TP5 | MinIO UI | http://localhost:9001 | `minioadmin` | `minioadmin` |
| TP5 | MinIO API S3 | http://localhost:9000 | `minioadmin` | `minioadmin` |
| TP5 | PostgreSQL | localhost:5432 | `weather` | `weather` |

> ⚠️ Les projets TP_1_2, TP2B et TP5 utilisent tous le port 8080 pour Airflow — ne lance qu'un seul projet à la fois, ou change les ports dans les `docker-compose.yml` respectifs.

---

## Variables Airflow — TP5 (référence complète)

Toutes les variables ont une valeur par défaut dans le code — le pipeline fonctionne sans rien configurer. Les Variables Airflow permettent de tout changer sans toucher au code.

### Créer les variables (Admin > Variables dans l'UI, ou CLI)

```powershell
# Villes à ingérer
docker compose exec airflow-webserver airflow variables set weather_cities '[{"name":"Paris","latitude":48.8566,"longitude":2.3522},{"name":"Lyon","latitude":45.764,"longitude":4.8357}]'

# API Open-Meteo
docker compose exec airflow-webserver airflow variables set weather_api_url "https://api.open-meteo.com/v1/forecast"
docker compose exec airflow-webserver airflow variables set weather_hourly_vars '["temperature_2m","precipitation","windspeed_10m","relativehumidity_2m","apparent_temperature","weathercode"]'
docker compose exec airflow-webserver airflow variables set weather_past_days 1
docker compose exec airflow-webserver airflow variables set weather_forecast_days 1
docker compose exec airflow-webserver airflow variables set weather_api_timeout 15

# MinIO
docker compose exec airflow-webserver airflow variables set weather_minio_endpoint "http://minio:9000"
docker compose exec airflow-webserver airflow variables set weather_minio_bucket "weather-raw"
docker compose exec airflow-webserver airflow variables set weather_minio_access "minioadmin"
docker compose exec airflow-webserver airflow variables set weather_minio_secret "minioadmin"

# Seuils qualité
docker compose exec airflow-webserver airflow variables set weather_qc_temp_min -50
docker compose exec airflow-webserver airflow variables set weather_qc_temp_max 55
docker compose exec airflow-webserver airflow variables set weather_qc_wind_max 300
docker compose exec airflow-webserver airflow variables set weather_qc_humidity_min 0
docker compose exec airflow-webserver airflow variables set weather_qc_humidity_max 100
docker compose exec airflow-webserver airflow variables set weather_qc_null_rate_max 0.10

# Robustesse
docker compose exec airflow-webserver airflow variables set weather_task_timeout_minutes 10

# Tests
docker compose exec airflow-webserver airflow variables set weather_force_anomaly false
```

### Tableau récapitulatif

| Variable | Défaut | Description |
|---|---|---|
| `weather_cities` | Paris, Clermont-Ferrand, Lyon | Villes à ingérer (JSON) |
| `weather_api_url` | `https://api.open-meteo.com/v1/forecast` | URL de l'API météo |
| `weather_hourly_vars` | 6 champs météo | Champs horaires demandés à l'API |
| `weather_past_days` | `1` | Jours passés à récupérer |
| `weather_forecast_days` | `1` | Jours de prévision |
| `weather_api_timeout` | `15` | Timeout HTTP en secondes |
| `weather_minio_endpoint` | `http://minio:9000` | URL de l'API MinIO |
| `weather_minio_bucket` | `weather-raw` | Bucket de stockage brut |
| `weather_minio_access` | `minioadmin` | Access key MinIO |
| `weather_minio_secret` | `minioadmin` | Secret key MinIO |
| `weather_qc_temp_min` | `-50` | Température minimale acceptée (°C) |
| `weather_qc_temp_max` | `55` | Température maximale acceptée (°C) |
| `weather_qc_wind_max` | `300` | Vitesse du vent maximale acceptée (km/h) |
| `weather_qc_humidity_min` | `0` | Humidité minimale acceptée (%) |
| `weather_qc_humidity_max` | `100` | Humidité maximale acceptée (%) |
| `weather_qc_null_rate_max` | `0.10` | Taux de nulls maximal toléré |
| `weather_task_timeout_minutes` | `10` | Timeout par tâche Airflow (minutes) |
| `weather_force_anomaly` | `false` | `true` pour simuler une anomalie qualité |
