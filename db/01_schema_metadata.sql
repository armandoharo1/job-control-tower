-- Extension necesaria para generar UUIDs
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Tabla maestra de clientes (tenants)
CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre VARCHAR(200) NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL,
    sector VARCHAR(100),                       -- 'banca', 'seguros', 'telecom', 'salud', etc.
    plan VARCHAR(50) DEFAULT 'starter',
    estado VARCHAR(20) DEFAULT 'activo',
    creado_en TIMESTAMP DEFAULT NOW()
);

-- Roles disponibles en el sistema
CREATE TABLE roles (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(50) UNIQUE NOT NULL          -- 'admin', 'editor', 'viewer'
);

INSERT INTO roles (nombre) VALUES ('admin'), ('editor'), ('viewer');

-- Fuentes de datos / orquestadores configurados por cada tenant
CREATE TABLE tenant_data_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    tipo_orquestador VARCHAR(50) NOT NULL,      -- 'airflow', 'control_m', 'databricks', 'adf'
    nombre VARCHAR(200) NOT NULL,
    config_conexion JSONB NOT NULL,             -- referencias a secretos, nunca credenciales en crudo
    activo BOOLEAN DEFAULT TRUE,
    creado_en TIMESTAMP DEFAULT NOW()
);

-- Usuarios y a que tenant pertenecen
CREATE TABLE tenant_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email VARCHAR(250) NOT NULL,
    nombre VARCHAR(200),
    creado_en TIMESTAMP DEFAULT NOW(),
    UNIQUE(tenant_id, email)
);

-- Que rol tiene cada usuario dentro de su tenant
CREATE TABLE tenant_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_user_id UUID NOT NULL REFERENCES tenant_users(id) ON DELETE CASCADE,
    role_id INT NOT NULL REFERENCES roles(id)
);

-- Tenant de practica, para que puedas seguir trabajando sobre algo real
INSERT INTO tenants (nombre, slug, sector, plan)
VALUES ('Práctica Armando', 'practica-armando', 'banca', 'starter');