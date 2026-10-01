-- Activar RLS en la tabla operativa
ALTER TABLE job_executions ENABLE ROW LEVEL SECURITY;

-- Politica: solo se pueden ver filas del tenant activo en la sesion actual
CREATE POLICY aislamiento_tenant ON job_executions
    USING (tenant_id = current_setting('app.current_tenant', true)::uuid);

-- Nota: mientras no se configure app.current_tenant en una sesion,
-- esta politica bloquea TODO acceso (comportamiento seguro por defecto).
-- El rol 'airflow' (superusuario en este entorno local) sigue viendo todo,
-- asi que las herramientas internas (ingesta, Metabase) no se rompen.