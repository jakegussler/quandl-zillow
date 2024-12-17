#!/bin/bash

#Database variables
DB_NAME="zillow_analytics"
DB_USER="zillow_user"
DB_PASSWORD="zillow_password"

#Connect to postgres as super user and run commands
psql -U postgres <<EOF

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

echo "database and user setup complete"