# TP2 — Apache Airflow

Ce dépôt contient deux projets indépendants, chacun avec sa propre stack Docker.

```
tp2/
├── TP_1_2/   ← TP2 + TP2A — PokéAPI (stack légère, pas de MinIO)
└── TP2B/     ← TP2B — Open-Meteo + MinIO + PostgreSQL
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

## TP2B — Météo (Open-Meteo → MinIO → PostgreSQL)

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
# http://localhost:9001  →  MinIO UI  (minio_access_key / minio_secret_key)
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

## Comparatif des deux projets

| | TP_1_2 (Pokémon) | TP2B (Météo) |
|---|---|---|
| Source | PokéAPI | Open-Meteo |
| Stockage intermédiaire | XCom Airflow | MinIO (object storage) |
| Base cible | `pokemon_stats` | `weather_facts` |
| Traçabilité | — | `ingestion_log` |
| Services Docker | Airflow + PostgreSQL | Airflow + PostgreSQL + MinIO |
| Stratégie chargement | ON CONFLICT DO NOTHING | ON CONFLICT DO NOTHING |
