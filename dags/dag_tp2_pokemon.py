"""
TP 2 — DAG Airflow : Pipeline PokeAPI
=======================================
Extrait les 20 premiers Pokémon depuis l'API PokeAPI,
récupère leurs stats, filtre les plus forts, génère un classement.
"""

import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# ─────────────────────────────────────────────
# Tâches
# ─────────────────────────────────────────────

def extract_pokemon(**context):
    """
    Tâche 1 — Extraction
    Récupère la liste des 20 premiers Pokémon puis les stats
    de chacun via l'API PokeAPI.
    """
    url = "https://pokeapi.co/api/v2/pokemon?limit=20"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    results = response.json()["results"]
    print(f"[EXTRACT] {len(results)} Pokémon récupérés.")

    pokemon_list = []
    for p in results:
        detail = requests.get(p["url"], timeout=10).json()
        stats = {s["stat"]["name"]: s["base_stat"] for s in detail["stats"]}
        pokemon_list.append({
            "name": detail["name"],
            "id": detail["id"],
            "hp": stats.get("hp", 0),
            "attack": stats.get("attack", 0),
            "defense": stats.get("defense", 0),
            "speed": stats.get("speed", 0),
        })
        print(f"[EXTRACT]   -> {detail['name']} (id={detail['id']})")

    context["ti"].xcom_push(key="pokemon_list", value=pokemon_list)


def transform_pokemon(**context):
    """
    Tâche 2 — Transformation
    Calcule un score global (hp + attack + defense + speed),
    filtre les Pokémon avec un score > 200, trie par score décroissant.
    """
    pokemon_list = context["ti"].xcom_pull(task_ids="extract_pokemon", key="pokemon_list")

    for p in pokemon_list:
        p["score"] = p["hp"] + p["attack"] + p["defense"] + p["speed"]

    top = [p for p in pokemon_list if p["score"] > 200]
    top = sorted(top, key=lambda x: x["score"], reverse=True)

    print(f"[TRANSFORM] {len(top)} Pokémon avec un score > 200 (sur {len(pokemon_list)}).")
    for p in top:
        print(f"[TRANSFORM]   {p['name']:<12} score={p['score']}  (hp={p['hp']} atk={p['attack']} def={p['defense']} spd={p['speed']})")

    context["ti"].xcom_push(key="top_pokemon", value=top)


def report_pokemon(**context):
    """
    Tâche 3 — Rapport
    Affiche le classement final des meilleurs Pokémon.
    """
    top = context["ti"].xcom_pull(task_ids="transform_pokemon", key="top_pokemon")

    print("[REPORT] ========== CLASSEMENT POKEMON ==========")
    print(f"[REPORT] {'Rang':<5} {'Nom':<12} {'Score':<8} {'HP':<5} {'ATK':<5} {'DEF':<5} {'SPD'}")
    print("[REPORT] " + "-" * 50)
    for i, p in enumerate(top, start=1):
        print(f"[REPORT] {i:<5} {p['name']:<12} {p['score']:<8} {p['hp']:<5} {p['attack']:<5} {p['defense']:<5} {p['speed']}")
    print("[REPORT] ================================================")


# ─────────────────────────────────────────────
# Définition du DAG
# ─────────────────────────────────────────────

default_args = {
    "owner": "lucas",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="tp2_pokemon_pipeline",
    description="Pipeline PokeAPI : extract -> transform -> report",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp2", "pokemon"],
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

    t_extract >> t_transform >> t_report
