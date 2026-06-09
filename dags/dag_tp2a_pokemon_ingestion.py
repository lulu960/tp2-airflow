"""
TP 2A — DAG Airflow : Ingestion API PokeAPI
============================================
Sujet original : ingestion API météo Open-Meteo pour plusieurs villes.
Adapté ici sur PokeAPI pour plusieurs "groupes" de Pokémon (comme on ferait
plusieurs villes), avec la même logique : appel API -> réponse JSON brute ->
identification des champs utiles -> transformation en structure exploitable.

Séparation stricte :
  - extract  : ce qui vient directement de l'API, aucune logique métier
  - transform : ce qui est préparé pour le pipeline / la table cible
  - report   : aperçu des données préparées
  - load     : insertion en base PostgreSQL
"""

import logging
import requests
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# Connexion à la même BDD PostgreSQL qu'Airflow
DB_CONN = {
    "host"    : "tp2-postgres",
    "port"    : 5432,
    "dbname"  : "airflow",
    "user"    : "airflow",
    "password": "airflow",
}

# ─────────────────────────────────────────────────────────────
# Groupes de Pokémon (équivalent des "villes" du sujet météo)
# On interroge 3 générations distinctes comme 3 sources séparées
# ─────────────────────────────────────────────────────────────
GROUPES = {
    "generation_1": list(range(1, 11)),    # Bulbasaur -> Caterpie
    "generation_2": list(range(152, 162)), # Chikorita -> Sentret
    "generation_3": list(range(252, 262)), # Treecko -> Wingull
}


# ─────────────────────────────────────────────────────────────
# Tâche 1 — Extraction
# Appelle l'API pour chaque groupe, stocke la réponse JSON brute.
# Aucune logique métier ici : on garde tout ce que l'API renvoie
# et on laisse la tâche transform décider quoi garder.
# ─────────────────────────────────────────────────────────────
def extract_pokemon(**context):
    """
    Récupère les données brutes de l'API PokeAPI pour chaque groupe.
    Stocke la réponse JSON complète sans filtrage ni transformation.
    """
    raw_by_group = {}

    for groupe, ids in GROUPES.items():
        log.info("Groupe '%s' — %d Pokémon à récupérer...", groupe, len(ids))
        raw_list = []

        for pid in ids:
            url = f"https://pokeapi.co/api/v2/pokemon/{pid}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            raw_list.append(data)
            log.info("  -> #%d %s OK", pid, data["name"])

        raw_by_group[groupe] = raw_list
        log.info("Groupe '%s' termine — %d entrées brutes.", groupe, len(raw_list))

    context["ti"].xcom_push(key="raw_by_group", value=raw_by_group)


# ─────────────────────────────────────────────────────────────
# Tâche 2 — Transformation
# Identifie les champs utiles pour le besoin métier et restructure
# les données en vue de la table cible.
#
# Champs retenus et justification :
#   - id          : identifiant unique, clé primaire naturelle
#   - name        : nom du Pokémon, lisible et exploitable
#   - groupe      : la "source" (génération), permet de filtrer par origine
#   - hp          : jauge de survie, indicateur de robustesse
#   - attack      : stat offensive principale
#   - defense     : stat défensive principale
#   - speed       : détermine l'ordre d'action en combat
#   - score       : calculé (hp + attack + defense + speed), résumé de puissance
#   - types       : liste des types (feu, eau...), utile pour analyses croisées
#   - extracted_at: timestamp d'ingestion, indispensable pour tracer la fraîcheur
#
# Champs écartés :
#   - sprites      : URLs d'images, inutile pour une table analytique
#   - moves        : trop volumineux (100+ moves par Pokémon), hors scope
#   - abilities    : non pertinent pour un scoring de puissance
#   - base_experience : donnée de progression, pas de stat de combat
# ─────────────────────────────────────────────────────────────
def transform_pokemon(**context):
    """
    Extrait les champs utiles depuis la réponse brute et construit
    la structure exploitable pour la table cible.
    """
    raw_by_group = context["ti"].xcom_pull(task_ids="extract_pokemon", key="raw_by_group")
    extracted_at = datetime.utcnow().isoformat()

    transformed = []

    for groupe, raw_list in raw_by_group.items():
        for raw in raw_list:
            stats = {s["stat"]["name"]: s["base_stat"] for s in raw["stats"]}

            hp       = stats.get("hp", 0)
            attack   = stats.get("attack", 0)
            defense  = stats.get("defense", 0)
            speed    = stats.get("speed", 0)
            score    = hp + attack + defense + speed
            types    = [t["type"]["name"] for t in raw["types"]]

            transformed.append({
                "id"          : raw["id"],
                "name"        : raw["name"],
                "groupe"      : groupe,
                "hp"          : hp,
                "attack"      : attack,
                "defense"     : defense,
                "speed"       : speed,
                "score"       : score,
                "types"       : types,
                "extracted_at": extracted_at,
            })

    transformed = sorted(transformed, key=lambda x: x["score"], reverse=True)

    log.info(
        "%d Pokémon transformés sur %d bruts.",
        len(transformed),
        sum(len(v) for v in raw_by_group.values()),
    )
    log.info("Champs retenus : id, name, groupe, hp, attack, defense, speed, score, types, extracted_at")

    context["ti"].xcom_push(key="transformed", value=transformed)


# ─────────────────────────────────────────────────────────────
# Tâche 3 — Rapport / aperçu
# Affiche un aperçu des données préparées par groupe.
# ─────────────────────────────────────────────────────────────
def report_pokemon(**context):
    """
    Affiche l'aperçu des données préparées, groupées par génération,
    avec le classement par score.
    """
    transformed = context["ti"].xcom_pull(task_ids="transform_pokemon", key="transformed")

    log.info("========== APERCU DES DONNEES PREPAREES ==========")
    log.info("Total : %d entrées prêtes pour la table cible", len(transformed))
    log.info("Champs : id | name | groupe | hp | atk | def | spd | score | types | extracted_at")
    log.info("-" * 70)

    for groupe in GROUPES.keys():
        groupe_data = [p for p in transformed if p["groupe"] == groupe]
        log.info("--- %s (%d Pokémon) ---", groupe.upper(), len(groupe_data))
        for p in groupe_data:
            types_str = "/".join(p["types"])
            log.info(
                "%-12s hp=%-4d atk=%-4d def=%-4d spd=%-4d score=%-5d types=%s",
                p["name"], p["hp"], p["attack"], p["defense"], p["speed"], p["score"], types_str,
            )

    log.info("Top 3 global :")
    for i, p in enumerate(transformed[:3], 1):
        log.info("  %d. %s (score=%d, groupe=%s)", i, p["name"], p["score"], p["groupe"])

    log.info("=" * 51)


# ─────────────────────────────────────────────────────────────
# Tâche 4 — Load
# Crée la table pokemon_stats si elle n'existe pas,
# puis insère les données transformées (ON CONFLICT pour éviter
# les doublons si on relance le DAG plusieurs fois).
# ─────────────────────────────────────────────────────────────
def load_pokemon(**context):
    """
    Crée la table pokemon_stats et insère les données préparées.
    ON CONFLICT (id) DO UPDATE permet de relancer le DAG sans doublons.
    """
    transformed = context["ti"].xcom_pull(task_ids="transform_pokemon", key="transformed")

    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pokemon_stats (
            id           INTEGER PRIMARY KEY,
            name         VARCHAR(100),
            groupe       VARCHAR(50),
            hp           INTEGER,
            attack       INTEGER,
            defense      INTEGER,
            speed        INTEGER,
            score        INTEGER,
            types        TEXT,
            extracted_at TIMESTAMP
        );
    """)

    inserted = 0
    for p in transformed:
        cur.execute("""
            INSERT INTO pokemon_stats (id, name, groupe, hp, attack, defense, speed, score, types, extracted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name         = EXCLUDED.name,
                groupe       = EXCLUDED.groupe,
                hp           = EXCLUDED.hp,
                attack       = EXCLUDED.attack,
                defense      = EXCLUDED.defense,
                speed        = EXCLUDED.speed,
                score        = EXCLUDED.score,
                types        = EXCLUDED.types,
                extracted_at = EXCLUDED.extracted_at;
        """, (
            p["id"],
            p["name"],
            p["groupe"],
            p["hp"],
            p["attack"],
            p["defense"],
            p["speed"],
            p["score"],
            "/".join(p["types"]),
            p["extracted_at"],
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    log.info("%d lignes insérées / mises à jour dans pokemon_stats.", inserted)


# ─────────────────────────────────────────────────────────────
# Définition du DAG
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "lucas",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="tp2a_pokemon_ingestion",
    description="TP2A — Ingestion PokeAPI : extract -> transform -> report -> load",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp2a", "pokemon", "ingestion"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_pokemon",
        python_callable=extract_pokemon,
    )

    t_transform = PythonOperator(
        task_id="transform_pokemon",
        python_callable=transform_pokemon,
    )

    t_report = PythonOperator(
        task_id="report_pokemon",
        python_callable=report_pokemon,
    )

    t_load = PythonOperator(
        task_id="load_pokemon",
        python_callable=load_pokemon,
    )

    t_extract >> t_transform >> t_report >> t_load