"""
plugins/weather/quality.py
Contrôles qualité sur les données météo transformées.
"""

import logging

logger = logging.getLogger(__name__)

# ─── Seuils de qualité ────────────────────────────────────────────────────────
TEMP_MIN      = -50.0   # °C   — valeur physiquement impossible sous ce seuil
TEMP_MAX      = 55.0    # °C   — valeur physiquement impossible au-dessus
WIND_MAX      = 300.0   # km/h — record mondial ~408 km/h, 300 = seuil anomalie
HUMIDITY_MIN  = 0.0
HUMIDITY_MAX  = 100.0
NULL_RATE_MAX = 0.10    # max 10% de valeurs nulles tolérées


def check_data_quality(records: list[dict], force_anomaly: bool = False) -> tuple[bool, dict]:
    """
    Contrôle la qualité des données transformées.

    Règles appliquées :
    1. Aucun enregistrement vide
    2. Taux de nulls < NULL_RATE_MAX
    3. Températures dans [TEMP_MIN, TEMP_MAX]
    4. Vitesse du vent dans [0, WIND_MAX]
    5. Humidité dans [HUMIDITY_MIN, HUMIDITY_MAX]

    Args:
        records: liste de dicts produits par transformer.transform_data
        force_anomaly: si True, injecte une anomalie artificielle (tests)

    Returns:
        (is_valid: bool, report: dict avec détails)
    """
    report = {
        "total_records": len(records),
        "errors": [],
        "warnings": [],
        "null_rate": 0.0,
    }

    # ── Cas trivial ────────────────────────────────────────────────────────────
    if not records:
        report["errors"].append("Aucun enregistrement reçu (dataset vide)")
        logger.error("[QUALITY] ❌ Dataset vide")
        return False, report

    # ── Injection d'anomalie pour tests ───────────────────────────────────────
    if force_anomaly:
        records[0]["temperature_c"] = 999.0
        logger.warning("[QUALITY] ⚠️ Anomalie artificielle injectée (temperature_c = 999)")

    total = len(records)
    null_count = 0
    temp_errors   = []
    wind_errors   = []
    humidity_errors = []

    for rec in records:
        city = rec.get("city", "?")
        ts   = rec.get("observation_time", "?")

        # Nulls
        if rec.get("has_nulls"):
            null_count += 1

        # Température
        temp = rec.get("temperature_c")
        if temp is not None:
            if temp < TEMP_MIN or temp > TEMP_MAX:
                msg = f"{city} @ {ts} : temperature_c={temp} hors [{TEMP_MIN}, {TEMP_MAX}]"
                temp_errors.append(msg)
                logger.warning(f"[QUALITY] ⚠️ {msg}")

        # Vent
        wind = rec.get("windspeed_kmh")
        if wind is not None:
            if wind < 0 or wind > WIND_MAX:
                msg = f"{city} @ {ts} : windspeed_kmh={wind} hors [0, {WIND_MAX}]"
                wind_errors.append(msg)
                logger.warning(f"[QUALITY] ⚠️ {msg}")

        # Humidité
        hum = rec.get("humidity_pct")
        if hum is not None:
            if hum < HUMIDITY_MIN or hum > HUMIDITY_MAX:
                msg = f"{city} @ {ts} : humidity_pct={hum} hors [{HUMIDITY_MIN}, {HUMIDITY_MAX}]"
                humidity_errors.append(msg)
                logger.warning(f"[QUALITY] ⚠️ {msg}")

    # ── Calcul taux de nulls ──────────────────────────────────────────────────
    null_rate = null_count / total if total > 0 else 0
    report["null_rate"] = round(null_rate, 4)

    if null_rate > NULL_RATE_MAX:
        report["errors"].append(
            f"Taux de nulls trop élevé : {null_rate:.1%} > {NULL_RATE_MAX:.0%}"
        )

    # ── Consolidation des erreurs ─────────────────────────────────────────────
    if temp_errors:
        report["errors"].append(f"{len(temp_errors)} erreur(s) de température : {temp_errors[:3]}")
    if wind_errors:
        report["errors"].append(f"{len(wind_errors)} erreur(s) de vent : {wind_errors[:3]}")
    if humidity_errors:
        report["errors"].append(f"{len(humidity_errors)} erreur(s) d'humidité : {humidity_errors[:3]}")

    is_valid = len(report["errors"]) == 0

    if is_valid:
        logger.info(f"[QUALITY] ✅ {total} enregistrements valides (null_rate={null_rate:.1%})")
    else:
        logger.error(f"[QUALITY] ❌ {len(report['errors'])} erreur(s) détectée(s)")

    return is_valid, report
