# TP2 / TP2A — Pipeline Airflow PokeAPI

## Stack

- Apache Airflow 2.9.2
- PostgreSQL 15
- Docker

## Lancer le projet

```bash
docker compose up -d
```

Interface : http://localhost:8080 — `admin` / `admin`

---

## TP2 — Pipeline de base

**DAG** : `tp2_pokemon_pipeline`

### Pipeline

`extract_pokemon` → `transform_pokemon` → `report_pokemon`

1. **extract_pokemon** — récupère les 20 premiers Pokémon depuis PokeAPI avec leurs stats brutes
2. **transform_pokemon** — calcule un score global (`hp + attack + defense + speed`), filtre les Pokémon avec score > 200, trie par score décroissant
3. **report_pokemon** — affiche le classement final dans les logs

Les données transitent entre les tâches via XCom :

```sql
SELECT task_id, key FROM xcom;
```

---

## TP2A — Ingestion API (structure exploitable)

**DAG** : `tp2a_pokemon_ingestion`

### Sujet

Préparer une ingestion propre depuis une API externe pour plusieurs sources distinctes, avec séparation stricte entre récupération brute et transformation en structure exploitable.

3 générations de Pokémon jouent le rôle des 3 villes du sujet météo original.

### Pipeline

`extract_pokemon` → `transform_pokemon` → `report_pokemon` → `load_pokemon`

1. **extract_pokemon** — appelle PokeAPI pour chaque génération, stocke le JSON complet sans logique métier
2. **transform_pokemon** — identifie les champs utiles, restructure en vue de la table cible, calcule le score
3. **report_pokemon** — affiche un aperçu des données préparées par génération dans les logs
4. **load_pokemon** — crée la table `pokemon_stats` si absente, insère les données (`ON CONFLICT` pour éviter les doublons)

### Champs retenus et justification

| Champ | Source | Pourquoi |
|---|---|---|
| `id` | API | Identifiant unique, clé primaire naturelle |
| `name` | API | Nom lisible, exploitable dans tous les rapports |
| `groupe` | Pipeline | Génération d'origine, permet de filtrer par source |
| `hp` | API `stats` | Jauge de survie, indicateur de robustesse |
| `attack` | API `stats` | Stat offensive principale |
| `defense` | API `stats` | Stat défensive principale |
| `speed` | API `stats` | Détermine l'ordre d'action, critère de classement |
| `score` | Calculé | `hp + attack + defense + speed`, résumé de puissance globale |
| `types` | API | Type(s) du Pokémon, utile pour analyses croisées |
| `extracted_at` | Pipeline | Timestamp d'ingestion, trace la fraîcheur des données |

### Champs écartés

| Champ | Raison |
|---|---|
| `sprites` | URLs d'images, inutile pour une table analytique |
| `moves` | 100+ moves par Pokémon, hors scope |
| `abilities` | Non pertinent pour un scoring de puissance |
| `base_experience` | Donnée de progression, pas une stat de combat |

### Résultat en base

Après exécution du DAG :

```sql
SELECT * FROM pokemon_stats ORDER BY score DESC;
```
