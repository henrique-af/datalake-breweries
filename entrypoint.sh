#!/bin/bash

# Start PostgreSQL for Airflow metadata
echo "Starting PostgreSQL..."
/etc/init.d/postgresql start

# Initialize Airflow database
echo "Initializing Airflow DB..."
airflow db init

# Create a user for Airflow (admin/admin)
echo "Creating Airflow user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@example.com

# Start Airflow scheduler and webserver
echo "Starting Airflow scheduler..."
airflow scheduler &

echo "Starting Airflow webserver..."
airflow webserver --port 8080 &

# Start Spark master
echo "Starting Spark master..."
$SPARK_HOME/sbin/start-master.sh

# Start Spark worker(s)
echo "Starting Spark workers..."
$SPARK_HOME/sbin/start-worker.sh spark://spark:7077

# Keep the container running
tail -f /dev/null