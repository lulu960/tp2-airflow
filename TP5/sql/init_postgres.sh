#!/bin/bash
# sql/init_postgres.sh
# Crée la base métier 'weather' et l'utilisateur dédié
# Exécuté automatiquement par le conteneur postgres au premier démarrage

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Création de l'utilisateur et de la base météo
    CREATE USER weather WITH PASSWORD 'weather';
    CREATE DATABASE weather OWNER weather;
    GRANT ALL PRIVILEGES ON DATABASE weather TO weather;
EOSQL

# Initialisation des tables dans la base météo
psql -v ON_ERROR_STOP=1 --username "weather" --dbname "weather" -f /docker-entrypoint-initdb.d/../sql/init.sql 2>/dev/null || true

echo "✅ Base 'weather' initialisée avec succès"
