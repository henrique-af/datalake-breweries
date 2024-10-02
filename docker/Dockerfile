# Base image for Python and Spark
FROM bitnami/spark:3.4.1

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Install necessary system packages
RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Install Airflow and other Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install apache-airflow==2.6.2 \
    apache-airflow-providers-http \
    pandas \
    requests \
    pyarrow \
    pyspark

# Create directories for Airflow and Spark
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the working directory
WORKDIR $AIRFLOW_HOME

# Expose ports for Airflow and Spark
EXPOSE 8080 7077 4040 18080

# Start Airflow webserver and Spark
CMD ["/entrypoint.sh"]