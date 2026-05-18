# CSV ETL Pipeline with Airflow and Pandas

## Overview
This project implements an ETL pipeline using Apache Airflow that:
- **Extracts** data from a CSV file
- **Transforms** data using pandas (cleaning, enrichment, aggregations)
- **Loads** transformed data to another CSV file

## Setup Instructions

### Local Installation (without Docker)

1. Install Airflow:
```bash
export AIRFLOW_HOME=~/airflow
pip install apache-airflow pandas numpy
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

2. Copy the DAG to Airflow's dags folder:
cp dags/csv_etl_pipeline.py $AIRFLOW_HOME/dags/

3. Create data directories:
mkdir -p data/{input,output,temp,logs}

4. Generate sample data:
python generate_sample_data.py

5. Start Airflow:
airflow webserver -p 8080
airflow scheduler

### Docker Setup
docker-compose up -d

Access Airflow UI at http://localhost:8080 (username: admin, password: admin)

