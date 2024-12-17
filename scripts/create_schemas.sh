#!/bin/bash

#Database variables
SCHEMA_RAW="RAW"
SCHEMA_SILVER="SILVER"
SCHEMA_GOLD="GOLD"

#Connect to postgres as super user and run commands
psql -U zillow_user <<EOF

#Set current database to zillow database
