# Job Control Tower

Plataforma de observabilidad para pipelines de datos: monitoreo de jobs, alertas, calidad de datos y linaje.

## Stack
- Apache Airflow
- PostgreSQL
- dbt-core
- Metabase

## Estado del proyecto
En construcción — MVP inicial.

## mostrar logs de creación de la base de datos y el usuario admin
docker compose up airflow-init

## Levantar todos los servicios
docker compose up -d

## Airflow en el navegado
http://localhost:18080

## Metabase en el navegado
http://localhost:3000