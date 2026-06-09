version: '3.8'

# ─────────────────────────────────────────────────────────
# TP 2B — Airflow + PostgreSQL + MinIO
# ─────────────────────────────────────────────────────────
#
# Après démarrage, configurer depuis l'UI Airflow :
#
#   Admin > Connections
#   ┌─────────────────┬──────────────────────────────────────────────────┐
#   │ Conn ID         │ postgres_meteo                                   │
#   │ Conn Type       │ Postgres                                         │
#   │ Host            │ postgres-meteo                                   │
#   │ Port            │ 5432                                             │
#   │ Login           │ meteo_user                                       │
#   │ Password        │ meteo_pass                                       │
#   │ Schema          │ meteo_db                                         │
#   └─────────────────┴──────────────────────────────────────────────────┘
#   ┌─────────────────┬──────────────────────────────────────────────────┐
#   │ Conn ID         │ minio_meteo                                      │
#   │ Conn Type       │ Amazon Web Services                              │
#   │ Login           │ minio_access_key                                 │
#   │ Password        │ minio_secret_key                                 │
#   │ Extra           │ {"endpoint_url": "http://minio:9000"}            │
#   └─────────────────┴──────────────────────────────────────────────────┘
#
#   Admin > Variables  (toutes ont un default_var dans le DAG)
#   ┌──────────────────────────┬─────────────────────────────────────────┐
#   │ meteo_cities             │ ["Paris","Lyon","Marseille","Bordeaux","Lille"] │
#   │ meteo_fields             │ temperature_2m,precipitation,windspeed_10m,weathercode │
#   │ meteo_api_base_url       │ https://api.open-meteo.com/v1/forecast  │
#   │ meteo_api_timeout        │ 30                                      │
#   │ meteo_raw_bucket         │ raw-meteo                               │
#   │ meteo_processed_bucket   │ processed-meteo                         │
#   │ meteo_schema             │ public                                  │
#   │ meteo_target_table       │ weather_facts                           │
#   │ meteo_tracking_table     │ ingestion_log                           │
#   └──────────────────────────┴─────────────────────────────────────────┘

x-airflow-common: &airflow-common
  image: apache/airflow:2.9.3
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
    AIRFLOW__CORE__FERNET_KEY: 'jzPCe_xFkSHv4oKuAfHk_zWj5JQ3mAakMr5XUHW3r4I='
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  depends_on:
    postgres-airflow:
      condition: service_healthy
    minio:
      condition: service_healthy
  networks:
    - tp2b-net

services:

  # ── PostgreSQL métadonnées Airflow ───────────────────────────────────────────
  postgres-airflow:
    image: postgres:16
    container_name: tp2b-postgres-airflow
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - pgdata-airflow:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 10
    networks:
      - tp2b-net

  # ── PostgreSQL données métier météo ──────────────────────────────────────────
  postgres-meteo:
    image: postgres:16
    container_name: tp2b-postgres-meteo
    environment:
      POSTGRES_USER: meteo_user
      POSTGRES_PASSWORD: meteo_pass
      POSTGRES_DB: meteo_db
    volumes:
      - pgdata-meteo:/var/lib/postgresql/data
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "meteo_user", "-d", "meteo_db"]
      interval: 5s
      retries: 10
    networks:
      - tp2b-net

  # ── MinIO ─────────────────────────────────────────────────────────────────────
  minio:
    image: minio/minio:latest
    container_name: tp2b-minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minio_access_key
      MINIO_ROOT_PASSWORD: minio_secret_key
    volumes:
      - minio-data:/data
    ports:
      - "9000:9000"
      - "9001:9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - tp2b-net

  # ── MinIO init — crée les buckets ────────────────────────────────────────────
  minio-init:
    image: minio/mc:latest
    container_name: tp2b-minio-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: /bin/sh
    command:
      - -c
      - |
        mc alias set local http://minio:9000 minio_access_key minio_secret_key
        mc mb --ignore-existing local/raw-meteo
        mc mb --ignore-existing local/processed-meteo
        echo "Buckets MinIO créés."
    networks:
      - tp2b-net

  # ── Init Airflow — uniquement migrations DB + user admin ─────────────────────
  airflow-init:
    <<: *airflow-common
    container_name: tp2b-airflow-init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        pip install apache-airflow-providers-amazon --quiet
        airflow db migrate
        airflow users create \
          --username admin \
          --password admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com
        echo "Init Airflow terminée — configurer Connections et Variables depuis l'UI."
    depends_on:
      postgres-airflow:
        condition: service_healthy
      minio-init:
        condition: service_completed_successfully

  # ── Webserver ─────────────────────────────────────────────────────────────────
  airflow-webserver:
    <<: *airflow-common
    container_name: tp2b-airflow-webserver
    command: >
      bash -c "pip install apache-airflow-providers-amazon --quiet && airflow webserver"
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 15s
      timeout: 10s
      retries: 5
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  # ── Scheduler ─────────────────────────────────────────────────────────────────
  airflow-scheduler:
    <<: *airflow-common
    container_name: tp2b-airflow-scheduler
    command: >
      bash -c "pip install apache-airflow-providers-amazon --quiet && airflow scheduler"
    depends_on:
      airflow-init:
        condition: service_completed_successfully

volumes:
  pgdata-airflow:
  pgdata-meteo:
  minio-data:

networks:
  tp2b-net:
    driver: bridge
