# CEPIK Scraping Project

# CEPIK Scraping Project

[![Airflow](https://img.shields.io/badge/Airflow-2.9.0-017CEE)](#)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C)](#)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-1E90FF)](#)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-336791)](#)
[![Python](https://img.shields.io/badge/Python-3.10-3776AB)](#)
[![HDFS](https://img.shields.io/badge/HDFS-3.3.6-FFCC00)](#)


## Overview
This project provides **DAGs and ETL pipelines** for scraping, transforming, and loading data from the **CEPIK (Centralna Ewidencja Pojazdów i Kierowców)** vehicle registration database in Poland.  

The pipelines automate:
- Downloading **dictionaries** (e.g., vehicle brands, fuel types, voivodeships).
- Scraping **historical and current vehicle registrations**.
- Persisting raw JSON into **HDFS (bronze layer)**.
- Transforming JSON into **Delta Lake (silver layer)** with a clean schema.
- Loading data into **PostgreSQL (gold layer)** for analytics and reporting.

## Features
- **Airflow DAGs** orchestrate scraping and ETL workflows.
- **Scraping layer**:
  - `scraping/dictionaries/get_raw_dictionary.py` – fetch CEPIK dictionaries.
  - `scraping/vehicles/get_raw_vehicles.py` – fetch vehicle registration data.
- **Transformation layer**:
  - `silver_save_dictionary_voivodeships_to_delta.py` – normalize and flatten dictionaries.
  - `silver_save_vehicles_to_delta.py` – normalize vehicle data and write to Delta Lake.
  - `gold_save_dictionary_voivodeshipts_to_psql.py` – load voivodeship dictionary into PostgreSQL.
- **DAGs**:
  - `dag_Cepik_dictionary_voivodeships.py` – scrape and store voivodeship dictionary.
  - `dag_cepik_vehicles.py` – scrape current vehicle registrations.
  - `dag_cepik_vehicles_history.py` – iterate over years to scrape historical registrations.

## Technology Stack
- **Apache Airflow** – workflow orchestration.
- **Apache Spark + Delta Lake** – distributed data processing and storage.
- **PostgreSQL** – relational database for curated data.
- **HDFS (WebHDFS)** – raw data storage.
- **Python** – scraping and ETL logic.
- **Requests + SSL adapter** – robust API integration with CEPIK.

## Database Schema
- `dict_voivodeships` – dictionary of voivodeships (from CEPIK API).  
- `dict_voivodeships_coords` – voivodeship names, capitals, and geocoordinates.  
- `vehicles` – main upserted table of vehicle registrations.  
- `staging_vehicles` – temporary table for incoming daily data.  

Supporting stored functions and procedures:
- `upsert_vehicles_from_stage()` – merges staging into main `vehicles`.  
- `updateVoivodeshioCoordsTableKeys()` – enriches `dict_voivodeships_coords` with CEPIK codes.  

## Data Flow
1. **Scraping**: Airflow DAG fetches JSON from CEPIK API.  
2. **Bronze**: Raw JSON stored in HDFS.  
3. **Silver**: Spark jobs flatten JSON into Delta Lake tables.  
4. **Gold**: Spark jobs load curated data into PostgreSQL.  
5. **Analytics**: PostgreSQL data can be used in BI tools like Power BI.  

## Setup
### Requirements
- Docker / Kubernetes for running Airflow & Spark.
- PostgreSQL 14+.
- HDFS cluster with WebHDFS enabled.
- Python ≥ 3.9 with:
  - `requests`
  - `beautifulsoup4`
  - `lxml`
  - `pyspark`
  - `delta-spark`

### Environment Variables / Airflow Variables
- `hdfs-data-path` – base path in HDFS.
- `local-data-path` – local temp path for scraped files.
- `task_pool` – Airflow pool for API requests.
- Connections:
  - `conn-webhdfs` – WebHDFS connection.
  - `conn-pg-cepik-db` – PostgreSQL connection for CEPIK DB.
  - `spark-conn` – Spark connection.

### Running
1. Deploy Airflow DAGs in your Airflow environment.  
2. Ensure Spark jobs are mounted under `/opt/airflow/libs/cepik/transformations/`.  
3. Start the DAGs from the Airflow UI:
   - `cepik_dictionary_voivodeships_*`
   - `cepik_vehicles_current_v*`
   - `cepik_vehicles_history_*`

## Example Usage
- **Fetch and load voivodeship dictionary** into PostgreSQL:
  - Run `dag_Cepik_dictionary_voivodeships.py`.
- **Scrape last 3 days of vehicle registrations**:
  - Run `dag_cepik_vehicles.py`.
- **Iterate through history starting from 1935**:
  - Run `dag_cepik_vehicles_history.py`.

## License
MIT License – free to use and adapt.
