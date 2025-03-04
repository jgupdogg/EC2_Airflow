#!/bin/bash

# Set environment for AWS deployment
export AIRFLOW_HOME="/home/jgupdogg/dev/aws/MWAA/airflow"

# Ensure the directory exists
mkdir -p $AIRFLOW_HOME

# Start docker-compose
docker-compose up