# TP5 — Pipeline Airflow Open-Meteo

Pipeline d'ingestion météo industrialisé avec Apache Airflow et l'API Open-Meteo.

---

## Description du pipeline

Le pipeline récupère quotidiennement les données météo horaires pour plusieurs villes configurables via l'API **Open-Meteo** (gratuite, sans clé API), les archive en JSON brut, les transforme en enregistrements plats, contrôle leur qualité, et les charge dans **PostgreSQL** si les données sont valides. En cas d'anomalie qualité, le chargement est bloqué et l'anomalie est tracée.

---

## Schéma du workflow

```
fetch_weather
    │
    ▼
archive_raw_data
    │
    ▼
transform_data
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
    │                                          │
    └──────────────────┬────────────────────────┘
                       ▼
                      end
```

---

## Variables Airflow utilisées

| Variable              | Type    | Description                                              | Valeur par défaut                      |
|-----------------------|---------|----------------------------------------------------------|----------------------------------------|
| `weather_cities`      | JSON    | Liste des villes à traiter (name, latitude, longitude)   | Paris, Clermont-Ferrand, Lyon          |
| `weather_force_anomaly` | String | `"true"` pour simuler une anomalie qualité             | `"false"`                              |

**Création via CLI :**
```bash
# Villes personnalisées
airflow variables set weather_cities '[{"name":"Bordeaux","latitude":44.8378,"longitude":-0.5792}]'

# Forcer une anomalie (pour test)
airflow variables set weather_force_anomaly true
```

---

## Connexions Airflow utilisées

| Conn ID            | Type       | Description                        |
|--------------------|------------|------------------------------------|
| `weather_postgres` | PostgreSQL | Base de données météo (host: postgres, db: weather, user: weather) |

**Création via CLI :**
```bash
airflow connections add weather_postgres \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema weather \
  --conn-login weather \
  --conn-password weather \
  --conn-port 5432
```

---

## Description des tâches du DAG

| Task ID              | Opérateur          | Rôle                                                                 |
|----------------------|--------------------|----------------------------------------------------------------------|
| `fetch_weather`      | PythonOperator     | Appelle l'API Open-Meteo pour chaque ville, retries=3, timeout=10min |
| `archive_raw_data`   | PythonOperator     | Sauvegarde les JSON bruts dans `/data/raw/<date>/`                   |
| `transform_data`     | PythonOperator     | Normalise en enregistrements plats (un par heure par ville)          |
| `check_quality`      | PythonOperator     | Vérifie températures, nulls, plages de valeurs                       |
| `quality_branch`     | BranchPythonOperator | Route vers `load_to_postgres` ou `skip_load`                      |
| `load_to_postgres`   | PythonOperator     | UPSERT idempotent sur `weather_observations`                         |
| `log_ingestion`      | PythonOperator     | Trace le succès dans `ingestion_log`                                 |
| `skip_load`          | PythonOperator     | Log le motif de skip                                                 |
| `log_anomaly`        | PythonOperator     | Trace l'anomalie dans `quality_anomaly_log`                          |
| `end`                | EmptyOperator      | Convergence des deux branches (trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS) |

---

## Stratégie de robustesse

- **retries=3** sur toutes les tâches, avec `retry_delay=2min`
- **execution_timeout=10min** par tâche
- **timeout=15s** sur les appels HTTP Open-Meteo
- **max_active_runs=1** — pas d'exécutions concurrentes
- **catchup=False** — pas de backfill automatique
- Toutes les exceptions sont catchées et loguées avant re-raise

---

## Stratégie d'idempotence

L'idempotence est garantie à deux niveaux :

1. **Archive** : les fichiers JSON sont écrits avec `open(..., "w")` — écrasement si relance sur même date
2. **PostgreSQL** : UPSERT avec `ON CONFLICT (city, observation_time) DO UPDATE` — une relance met à jour les valeurs sans créer de doublon

Une relance complète du DAG sur la même `execution_date` produit exactement le même état final en base.

---

## Contrôles qualité mis en place

| Règle                  | Seuil                        | Action si violation |
|------------------------|------------------------------|---------------------|
| Dataset non vide       | > 0 enregistrements          | Erreur → skip       |
| Taux de nulls          | ≤ 10%                        | Erreur → skip       |
| Température            | [-50°C, +55°C]               | Erreur → skip       |
| Vitesse du vent        | [0, 300 km/h]                | Erreur → skip       |
| Humidité               | [0%, 100%]                   | Erreur → skip       |

Pour **simuler une anomalie** : `airflow variables set weather_force_anomaly true`
Cela injecte `temperature_c = 999` dans le premier enregistrement.

---

## Règle de branchement conditionnel

Le `BranchPythonOperator` `quality_branch` lit le XCom `quality_valid` poussé par `check_quality` :

- Si `True` → branche `load_to_postgres` → `log_ingestion` → `end`
- Si `False` → branche `skip_load` → `log_anomaly` → `end`

Le `EmptyOperator` `end` utilise `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` pour se déclencher quelle que soit la branche active.

---

## Description des logs produits

Chaque tâche utilise `logging.getLogger(__name__)` avec des préfixes standardisés :

| Préfixe          | Niveau  | Signification                              |
|------------------|---------|--------------------------------------------|
| `[EXTRACTOR]`    | INFO    | Requêtes API, nombre d'heures reçues       |
| `[ARCHIVER]`     | INFO    | Chemin des fichiers archivés               |
| `[TRANSFORMER]`  | INFO    | Nombre d'enregistrements transformés       |
| `[QUALITY]`      | WARNING | Anomalies détectées (avec détails)         |
| `[BRANCH]`       | INFO    | Décision de routage                        |
| `[LOADER]`       | INFO    | Nombre de lignes insérées / mises à jour   |
| `[SKIP]`         | WARNING | Motif du skip avec rapport qualité         |

---

## Description des tables PostgreSQL

### `weather_observations`
Données météo horaires par ville.

| Colonne             | Type          | Description                                |
|---------------------|---------------|--------------------------------------------|
| id                  | SERIAL PK     | Identifiant auto                           |
| city                | VARCHAR(100)  | Nom de la ville                            |
| observation_time    | TIMESTAMP     | Heure de l'observation (clé d'unicité)     |
| execution_date      | DATE          | Date d'exécution du DAG                    |
| temperature_c       | NUMERIC(6,2)  | Température en °C                          |
| apparent_temp_c     | NUMERIC(6,2)  | Température ressentie en °C                |
| precipitation_mm    | NUMERIC(8,2)  | Précipitations en mm                       |
| windspeed_kmh       | NUMERIC(7,2)  | Vitesse du vent en km/h                    |
| humidity_pct        | NUMERIC(5,2)  | Humidité relative en %                     |
| weather_code        | INTEGER       | Code météo WMO                             |
| weather_label       | VARCHAR(100)  | Libellé humain du code WMO                 |
| loaded_at           | TIMESTAMP     | Horodatage du chargement                   |

**Contrainte UNIQUE** : `(city, observation_time)` — idempotence UPSERT.

### `ingestion_log`
Trace chaque exécution réussie du pipeline.

| Colonne         | Type         | Description                          |
|-----------------|--------------|--------------------------------------|
| dag_id          | VARCHAR(200) | Identifiant du DAG                   |
| run_id          | VARCHAR(500) | Identifiant de l'exécution           |
| execution_date  | DATE         | Date logique d'exécution             |
| status          | VARCHAR(50)  | `success`                            |
| rows_inserted   | INTEGER      | Nouvelles lignes insérées            |
| rows_skipped    | INTEGER      | Lignes mises à jour (déjà présentes) |
| quality_report  | TEXT         | Rapport qualité sérialisé            |

### `quality_anomaly_log`
Trace les exécutions bloquées par une anomalie qualité.

| Colonne         | Type         | Description                     |
|-----------------|--------------|---------------------------------|
| dag_id          | VARCHAR(200) | Identifiant du DAG              |
| run_id          | VARCHAR(500) | Identifiant de l'exécution      |
| execution_date  | DATE         | Date logique d'exécution        |
| anomaly_report  | TEXT         | Détail de l'anomalie détectée   |

---

## Démarrage rapide

```bash
# 1. Lancer la stack
docker compose up -d

# 2. Attendre que l'init soit terminé
docker compose logs airflow-init

# 3. Accéder à l'UI
open http://localhost:8080   # admin / admin

# 4. Créer la connexion PostgreSQL
docker compose exec airflow-webserver airflow connections add weather_postgres \
  --conn-type postgres --conn-host postgres --conn-schema weather \
  --conn-login weather --conn-password weather --conn-port 5432

# 5. Déclencher le DAG
docker compose exec airflow-webserver airflow dags trigger weather_pipeline

# 6. Simuler une anomalie
docker compose exec airflow-webserver airflow variables set weather_force_anomaly true
docker compose exec airflow-webserver airflow dags trigger weather_pipeline

# 7. Tester l'idempotence (relance)
docker compose exec airflow-webserver airflow dags trigger weather_pipeline --conf '{"ds":"2025-06-10"}'
```

---

## Preuves d'exécution attendues

- **Cas nominal** : screenshot du graph view avec toutes les tâches en vert, contenu de `weather_observations`
- **Anomalie qualité** : screenshot avec `skip_load` et `log_anomaly` en vert, `load_to_postgres` skippé, contenu de `quality_anomaly_log`
- **Idempotence** : deux runs successifs → même nombre de lignes en base, `rows_skipped` > 0 dans `ingestion_log`
- **Logs** : extrait des logs de `check_quality` montrant les messages `[QUALITY]`

---

## Limites éventuelles

- Les données Open-Meteo peuvent légèrement varier entre deux appels (mise à jour des prévisions) — l'UPSERT est une mise à jour, pas un doublon
- Le `archive_raw_data` écrase les fichiers existants sur relance (comportement intentionnel)
- Pas d'alerting email configuré (nécessite un SMTP dans Airflow)
- La base `weather` est initialisée via un script shell Docker — en production, utiliser des migrations (Alembic, Flyway)
