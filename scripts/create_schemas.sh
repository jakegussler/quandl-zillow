#!/bin/bash

#Get variables from .env file
ENV_FILE="${1:-../.env}"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo "can not find .env file in $ENV_FILE, exiting..."
    return 1
fi

#Set environment variables
SCHEMA_RAW="RAW"
SCHEMA_SILVER="SILVER"
SCHEMA_GOLD="GOLD"
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}

#Set password for postgresql
export PGPASSWORD="$POSTGRES_PASSWORD"

#Connect to postgres as super user and run commands
psql -U zillow_user -d zillow_analytics -w <<EOF

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

EOF

echo "Schema creation completed"


