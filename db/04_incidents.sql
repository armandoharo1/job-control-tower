CREATE TABLE incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    data_source_id UUID REFERENCES tenant_data_sources(id),
    dag_id VARCHAR(250) NOT NULL,
    run_id VARCHAR(250) NOT NULL,
    causa_raiz_task_id VARCHAR(250),
    causa_raiz_mensaje TEXT,
    tareas_afectadas TEXT[],
    cantidad_afectados INT DEFAULT 0,
    area VARCHAR(100),
    proyecto VARCHAR(100),
    severidad VARCHAR(20),
    detectado_en TIMESTAMP DEFAULT NOW(),
    actualizado_en TIMESTAMP DEFAULT NOW(),
    UNIQUE(tenant_id, dag_id, run_id)
);

CREATE INDEX idx_incidents_tenant ON incidents(tenant_id);