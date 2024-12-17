#!/bin/bash

#Get environment variables from .env file
ENV_FILE="${1:-../.env}"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo ".env file not found in $ENV_FILE, exiting"
    return 1
fi

#Set environment variables
DB_NAME=${POSTGRES_DATABASE}
DB_USER=${POSTGRES_USER}
DB_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_SUPERUSER_PASSWORD=${POSTGRES_SUPERUSER_PASSWORD}

export PGPASSWORD="$POSTGRES_SUPERUSER_PASSWORD"

echo "$DB_NAME"
echo "$DB_USER"
echo "$DB_PASSWORD"
echo "$POSTGRES_SUPERUSER_PASSWORD"

#Connect to postgres as super user and run commands
psql -U postgres -h localhost -w <<EOF

--Drop database if exists
DROP DATABASE IF EXISTS $DB_NAME;

--Create database
CREATE DATABASE $DB_NAME;

--Drop user if exists
DROP USER IF EXISTS $DB_USER;

--Create user
CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';

--Grant priviliges to user
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
EOF

unset PGPASSWORD
echo "database and user setup complete"