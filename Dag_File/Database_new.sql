-- Active: 1660399575886@@127.0.0.1@3306@airflow
CREATE DATABASE airflow;
ALTER DATABASE airflow SET READ_COMMITTED_SNAPSHOT ON;
CREATE LOGIN airflow_user WITH PASSWORD='airflow_pass123%';
USE airflow;
CREATE USER airflow_user FROM LOGIN airflow_user;
GRANT ALL PRIVILEGES ON DATABASE::airflow TO airflow_user;
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql://postgres@localhost:5432/my_database?options=-csearch_path%3Dairflow"
export AIRFLOW__DATABASE__SQL_ALCHEMY_SCHEMA="airflow"