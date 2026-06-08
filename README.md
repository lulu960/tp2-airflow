# TP2 — Pipeline Airflow

DAG Airflow qui extrait des données depuis l'API PokeAPI, les transforme et génère un rapport.

## Stack

- Apache Airflow 2.9.2
- PostgreSQL 15
- Docker

## Lancer le projet

```bash
docker compose up -d
```

Puis ouvrir http://localhost:8080 — `admin` / `admin`

## Pipeline

`extract_pokemon` → `transform_pokemon` → `report_pokemon`

1. **extract_pokemon** — récupère les 20 premiers Pokémon depuis l'API PokeAPI avec leurs stats (HP, attaque, défense, vitesse)
2. **transform_pokemon** — calcule un score global, filtre les Pokémon avec un score > 200, trie par score décroissant
3. **report_pokemon** — affiche le classement final dans les logs

Les données transitent entre les tâches via XCom et sont consultables dans la table `xcom` de la base PostgreSQL.

## Résultat

Les données extraites et transformées sont visibles dans :
- Les logs de chaque tâche (UI Airflow → Grid → clic sur une tâche → Logs)
- La table `xcom` en base : `SELECT * FROM xcom;`