-- =============================================================================
-- sql/init.sql
-- Initialisation des tables PostgreSQL pour le pipeline weather_pipeline
-- Idempotent : CREATE TABLE IF NOT EXISTS + contraintes UNIQUE
-- =============================================================================

-- ─── Table principale : observations météo ───────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_observations (
    id                  SERIAL          PRIMARY KEY,
    city                VARCHAR(100)    NOT NULL,
    observation_time    TIMESTAMP       NOT NULL,           -- heure de l'observation
    execution_date      DATE            NOT NULL,           -- date d'exécution du DAG
    latitude            NUMERIC(9,6),
    longitude           NUMERIC(9,6),
    temperature_c       NUMERIC(6,2),                       -- °C
    apparent_temp_c     NUMERIC(6,2),                       -- température ressentie
    precipitation_mm    NUMERIC(8,2),                       -- mm
    windspeed_kmh       NUMERIC(7,2),                       -- km/h
    humidity_pct        NUMERIC(5,2),                       -- %
    weather_code        INTEGER,
    weather_label       VARCHAR(100),
    loaded_at           TIMESTAMP       DEFAULT NOW(),

    -- Clé de déduplication pour UPSERT idempotent
    CONSTRAINT uq_city_obs_time UNIQUE (city, observation_time)
);

COMMENT ON TABLE weather_observations IS
    'Données météo horaires par ville, chargées depuis Open-Meteo.';

COMMENT ON CONSTRAINT uq_city_obs_time ON weather_observations IS
    'Garantit l''idempotence des relances : pas de doublon (ville, heure).';


-- ─── Table de traçabilité : log d'ingestion ──────────────────────────────────
CREATE TABLE IF NOT EXISTS ingestion_log (
    id              SERIAL          PRIMARY KEY,
    dag_id          VARCHAR(200)    NOT NULL,
    run_id          VARCHAR(500)    NOT NULL,
    execution_date  DATE            NOT NULL,
    status          VARCHAR(50)     NOT NULL,               -- 'success' | 'skipped'
    rows_inserted   INTEGER         DEFAULT 0,
    rows_skipped    INTEGER         DEFAULT 0,
    quality_report  TEXT,                                   -- JSON sérialisé
    logged_at       TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE ingestion_log IS
    'Trace chaque exécution du DAG : statut, volumes, rapport qualité.';


-- ─── Table de traçabilité : anomalies qualité ────────────────────────────────
CREATE TABLE IF NOT EXISTS quality_anomaly_log (
    id              SERIAL          PRIMARY KEY,
    dag_id          VARCHAR(200)    NOT NULL,
    run_id          VARCHAR(500)    NOT NULL,
    execution_date  DATE            NOT NULL,
    anomaly_report  TEXT            NOT NULL,               -- détail de l'anomalie
    logged_at       TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE quality_anomaly_log IS
    'Enregistre les anomalies qualité détectées (chargement bloqué).';


-- ─── Index pour les requêtes fréquentes ─────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_obs_city       ON weather_observations (city);
CREATE INDEX IF NOT EXISTS idx_obs_exec_date  ON weather_observations (execution_date);
CREATE INDEX IF NOT EXISTS idx_obs_time       ON weather_observations (observation_time);
CREATE INDEX IF NOT EXISTS idx_log_exec_date  ON ingestion_log (execution_date);
CREATE INDEX IF NOT EXISTS idx_anom_exec_date ON quality_anomaly_log (execution_date);
