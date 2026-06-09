-- ═══════════════════════════════════════════════════════════════════════════
-- TP 2B — Script d'initialisation de la base métier météo
-- Exécuté automatiquement au premier démarrage de postgres-meteo
-- ═══════════════════════════════════════════════════════════════════════════

-- ─── Table cible : données météo structurées ─────────────────────────────────
CREATE TABLE IF NOT EXISTS public.weather_facts (
    id              SERIAL          PRIMARY KEY,
    city            VARCHAR(100)    NOT NULL,
    -- date de la mesure (issue de la réponse API Open-Meteo, pas la date d'ingestion)
    measure_date    DATE            NOT NULL,
    measure_hour    SMALLINT        NOT NULL CHECK (measure_hour BETWEEN 0 AND 23),
    temperature_c   NUMERIC(5, 2),
    precipitation_mm NUMERIC(6, 2),
    windspeed_kmh   NUMERIC(6, 2),
    weathercode     SMALLINT,
    -- traçabilité : identifiant du run Airflow qui a produit cette ligne
    airflow_run_id  VARCHAR(250),
    inserted_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- évite les doublons si le DAG rejoue la même période
    UNIQUE (city, measure_date, measure_hour)
);

COMMENT ON TABLE public.weather_facts IS
    'Données horaires Open-Meteo transformées — table cible du pipeline TP2B.';

-- ─── Table de suivi d'ingestion (traçabilité) ────────────────────────────────
CREATE TABLE IF NOT EXISTS public.ingestion_log (
    id              SERIAL          PRIMARY KEY,
    dag_id          VARCHAR(250)    NOT NULL,
    run_id          VARCHAR(250)    NOT NULL,
    -- période de données couverte par ce run (data interval Airflow)
    data_interval_start  TIMESTAMPTZ,
    data_interval_end    TIMESTAMPTZ,
    source          VARCHAR(100)    NOT NULL DEFAULT 'open-meteo',
    cities_requested TEXT,           -- liste JSON des villes demandées
    rows_received   INTEGER,         -- lignes brutes récupérées depuis l'API
    rows_inserted   INTEGER,         -- lignes effectivement insérées dans weather_facts
    rows_skipped    INTEGER,         -- doublons ignorés (ON CONFLICT DO NOTHING)
    status          VARCHAR(20)      NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'success', 'failed')),
    error_message   TEXT,
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

COMMENT ON TABLE public.ingestion_log IS
    'Traçabilité de chaque run du pipeline météo : source, volume, statut.';

-- ─── Index utiles ────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_wf_city_date
    ON public.weather_facts (city, measure_date);

CREATE INDEX IF NOT EXISTS idx_il_dag_run
    ON public.ingestion_log (dag_id, run_id);

-- ─── Vue de contrôle rapide ──────────────────────────────────────────────────
CREATE OR REPLACE VIEW public.v_latest_weather AS
SELECT DISTINCT ON (city)
    city,
    measure_date,
    measure_hour,
    temperature_c,
    precipitation_mm,
    windspeed_kmh,
    inserted_at
FROM public.weather_facts
ORDER BY city, measure_date DESC, measure_hour DESC;

COMMENT ON VIEW public.v_latest_weather IS
    'Dernière mesure disponible par ville (commodité de lecture).';
