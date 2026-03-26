# Batch ETL Pipeline — E-Commerce Market Analysis

## Project Overview
This project implements a batch ETL pipeline to extract e-commerce product data, transform and validate it, load it into PostgreSQL, orchestrate runs with Apache Airflow, and expose insights in Metabase.

## Tech Stack
- Python 3.11
- Apache Airflow 2.7.3
- PostgreSQL
- Metabase
- Pandas
- SQLAlchemy

## Project Structure
```text
dags/
etl/
tests/
sql/
config/
logs/
staging/
archive/
