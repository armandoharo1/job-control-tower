-- Agregar tenant_id y data_source_id a la tabla operativa existente
ALTER TABLE job_executions
    ADD COLUMN tenant_id UUID REFERENCES tenants(id),
    ADD COLUMN data_source_id UUID REFERENCES tenant_data_sources(id);

-- Crear la fuente de datos que representa tu Airflow actual, asociada al tenant de practica
INSERT INTO tenant_data_sources (tenant_id, tipo_orquestador, nombre, config_conexion)
SELECT id, 'airflow', 'Airflow Local Practica',
       '{"url": "http://airflow-webserver:8080/api/v1", "auth_var": "AIRFLOW_CREDENTIALS"}'::jsonb
FROM tenants WHERE slug = 'practica-armando';

-- Backfill: asignar todas las filas existentes al tenant y fuente de practica
UPDATE job_executions
SET tenant_id = (SELECT id FROM tenants WHERE slug = 'practica-armando'),
    data_source_id = (SELECT id FROM tenant_data_sources WHERE nombre = 'Airflow Local Practica');

-- Ahora que todo tiene tenant_id, lo hacemos obligatorio
ALTER TABLE job_executions ALTER COLUMN tenant_id SET NOT NULL;

-- Indice para que los filtros por tenant sean rapidos a futuro
CREATE INDEX idx_job_executions_tenant ON job_executions(tenant_id);