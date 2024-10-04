# Breweries API Project

## Objective

The objective of this project is to build a data pipeline that ingests brewery-related data from the [Open Brewery DB API](https://api.openbrewerydb.org/), processes it, and stores it in various layers of a data lake following the Medallion Architecture. This project was implemented using Airflow, with data transformations handled by Pandas, and all orchestration running within Docker containers. The choice to use Pandas over Spark was due to the dataset being relatively small (about 8k rows), making Spark unnecessary for this scenario.

## Project Structure

The project contains the following files and folders:

- **dags**: Contains Airflow DAGs. The DAGs orchestrate the data pipeline, fetching, transforming, and storing data.
- **Dockerfile**: Defines the container with the Astro Runtime environment, which includes Airflow.
- **requirements.txt**: Python dependencies needed for the project (e.g., Pandas).
- **airflow_settings.yaml**: Allows configuration of Airflow connections and variables for local development.

## Data Lake Architecture

The project follows the Medallion Architecture, splitting the data into three layers:

1. **Bronze Layer**: Raw data ingested directly from the Open Brewery DB API and stored as Parquet files.
2. **Silver Layer**: Data is transformed into Parquet format and partitioned by country, creating separate directories for each country to optimize data clustering and query performance. This structured organization ensures that data pertaining to each country is easily accessible and efficiently stored.
3. **Gold Layer**: An aggregated view containing brewery counts by type and location.

## Pipeline Overview

1. **API Ingestion**: The DAG pulls data from the Open Brewery DB API and stores it in the Bronze layer.
2. **Transformation**: Data is processed using Pandas, cleaned, and written to the Silver layer in Parquet format. The Silver layer data is partitioned by country, with separate directories for each country to improve data clustering and query performance.
3. **Aggregation**: Aggregated brewery counts by type and location are stored in the Gold layer for analytical purposes.

## Prerequisites

Before you can run the project, make sure you have the following installed:

- **Git**  
  [Download Git](https://git-scm.com/downloads)
  
- **Docker**  
  [Install Docker](https://docs.docker.com/get-docker/)

- **Astro CLI**
  ```bash 
  winget install -e --id Astronomer.Astro
## Installation

1. Clone the repository:

   ```bash
   $ git clone https://github.com/henrique-af/datalake-brewries.git
   $ cd datalake-brewries
   ```

2. Build and start the Docker containers for Airflow:

   ```bash
   $ astro dev start
   ```

   This command will spin up four Docker containers: Postgres, Airflow Webserver, Airflow Scheduler, and Airflow Triggerer.

3. Access the Airflow UI by navigating to http://localhost:8080 in your browser and logging in with:

   Username: admin  
   Password: admin

## Running the DAG

Once logged into Airflow, locate the DAG named `dag_breweries` on the home page.  

Toggle the DAG to "On" by clicking the switch.  

Trigger the DAG manually or wait for the scheduled runs.  

The pipeline will fetch the brewery data from the API, process it, and store it in the Bronze, Silver, and Gold layers.

## Future Enhancements

Although this project is currently running locally using Docker containers, a more robust production deployment could involve:

- **Cloud Storage Integration**: Storing the data in S3 or Google Cloud Storage (GCS) instead of local Docker volumes.
- **Spark Integration**: If the data grows in size, switching from Pandas to Spark would improve performance for handling large datasets.

## Monitoring and Alerting

For monitoring and alerting:

- **Data Quality Checks**: Implement data validation tasks within Airflow to check for data integrity issues in the pipeline.
- **Alerting**: Configure email or Slack alerts in Airflow to notify of failures, retries, or data quality issues.

## Design Choices

- **Airflow for Orchestration**: Airflow was chosen for this project because of its ability to handle complex workflows, scheduling, and retry logic. It also integrates well with Docker for local development.
- **Pandas for Data Transformation**: Given the small size of the dataset (~8k rows), Pandas was sufficient for data processing tasks. Using Spark would have added unnecessary complexity.
- **Docker for Containerization**: Docker was used to ensure that the project could run in isolated containers, making it easy to replicate the environment across different machines.
- **Local Development with Airflow**: The project runs within a Dockerized Airflow environment, but for production, it's recommended to push the data to S3 or GCS and consume from there.

## Conclusion

This project demonstrates how to build a simple data pipeline using Airflow for orchestration and Pandas for data transformation. The next steps would be to deploy the pipeline in the cloud and implement more robust monitoring and alerting systems.