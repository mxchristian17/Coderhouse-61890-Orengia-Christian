#!/bin/bash
airflow standalone

airflow db migrate

# Crear el usuario de Airflow
airflow users create \
  --username admin \
  --firstname Christian \
  --lastname Orengia \
  --role Admin \
  --email orengiachristian@gmail.com \
  --password 123456

# Iniciar el webserver y el scheduler
airflow webserver --port 8080 &
airflow scheduler
