-- ==============================================================================
-- 🌐 RELATIONAL DATA LAYER: CORE METADATA AND TELEMETRY SCHEMA
-- SYSTEM ENGINE: PostgreSQL (Production-Ready Layout)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. DICTIONARIES & LOOKUP TABLES
-- ------------------------------------------------------------------------------

CREATE TABLE public.connection_types (
    id int4 NOT NULL,
    name varchar(50) NOT NULL,
    CONSTRAINT connection_types_pkey PRIMARY KEY (id),
    CONSTRAINT connection_types_name_key UNIQUE (name)
);
COMMENT ON TABLE public.connection_types IS 'Supported network interfaces (1: Vendor A, 2: Vendor B, 3: Siemens S7, 4: Modbus TCP)';

CREATE TABLE public.resource_types (
    id int4 NOT NULL,
    name varchar(50) NOT NULL,
    CONSTRAINT resource_types_pkey PRIMARY KEY (id),
    CONSTRAINT resource_types_name_key UNIQUE (name)
);
COMMENT ON TABLE public.resource_types IS 'Monitored engineering utility types (e.g., Cold Water, Hot Water, Gas, Electricity)';

-- ------------------------------------------------------------------------------
-- 2. INFRASTRUCTURE CONNECTIONS
-- ------------------------------------------------------------------------------

CREATE TABLE public.connections (
    id serial4 NOT NULL,
    id_connection_type int4 NOT NULL,
    name varchar(100) NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT connections_pkey PRIMARY KEY (id),
    CONSTRAINT connections_name_key UNIQUE (name),
    CONSTRAINT fk_connections_connection_type FOREIGN KEY (id_connection_type) REFERENCES public.connection_types(id) ON DELETE RESTRICT
);
COMMENT ON COLUMN public.connections.config IS 'Dynamic connection parameters: {"ip": "127.0.0.1", "port": 502}';

-- ------------------------------------------------------------------------------
-- 3. METER POINTS METADATA (CORE ASSET MODEL)
-- ------------------------------------------------------------------------------

CREATE TABLE public.mp (
    id serial4 NOT NULL,
    id_connection int4 NOT NULL,
    id_resource_type int4 NOT NULL,
    id_object int4 NOT NULL,
    is_active bool DEFAULT true NOT NULL,
    send_to_email bool DEFAULT false NOT NULL,
    id_mp_src varchar(50) NOT NULL,
    name varchar(100) NOT NULL,
    config jsonb NULL,
    CONSTRAINT mp_pkey PRIMARY KEY (id),
    CONSTRAINT mp_name_key UNIQUE (name),
    CONSTRAINT fk_mp_connection FOREIGN KEY (id_connection) REFERENCES public.connections(id) ON DELETE RESTRICT,
    CONSTRAINT fk_mp_resource_type FOREIGN KEY (id_resource_type) REFERENCES public.resource_types(id) ON DELETE RESTRICT
);
COMMENT ON COLUMN public.mp.id_mp_src IS 'Legacy or external hardware tracking identifier from source system mapping';
COMMENT ON COLUMN public.mp.config IS 'Hardware register configurations: {"db_number": 10, "start_byte": 20, "data_type": "REAL"}';

-- ------------------------------------------------------------------------------
-- 4. BATCH HISTORICAL ARCHIVE (TELEMETRY LOGS)
-- ------------------------------------------------------------------------------

CREATE TABLE public.mp_hourly_values (
    dt timestamp NOT NULL,
    id_mp int4 NOT NULL,
    value jsonb NOT NULL,
    CONSTRAINT mp_hourly_values_pkey PRIMARY KEY (dt, id_mp),
    CONSTRAINT fk_mp_hourly_values_mp FOREIGN KEY (id_mp) REFERENCES public.mp(id) ON DELETE CASCADE
);
COMMENT ON COLUMN public.mp_hourly_values.value IS 'Unified telemetry storage frame: {"code_err": 0, "value_plus": 124.5, "value_minus": 0.0}';

-- Operational performance indexes optimized for Data Engineering workloads
CREATE INDEX idx_mp_hourly_real_id_dt ON public.mp_hourly_values USING btree (id_mp, dt DESC);
COMMENT ON INDEX public.idx_mp_hourly_real_id_dt IS 'Optimized for high-speed UI trend charts and instant time-series queries (freshest data first)';

CREATE INDEX idx_mp_hourly_values_dt_desc ON public.mp_hourly_values USING btree (dt DESC);
CREATE INDEX idx_mp_hourly_values_mp ON public.mp_hourly_values USING btree (id_mp);

-- ------------------------------------------------------------------------------
-- 5. SYSTEM ENVIRONMENT CONFIGURATION & AUTOMATION TRIGGERS
-- ------------------------------------------------------------------------------

CREATE TABLE public.sys_config (
    cfg_key varchar(100) NOT NULL,
    cfg_value jsonb NOT NULL,
    description text NULL,
    cfg_updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT sys_config_pkey PRIMARY KEY (cfg_key)
);
COMMENT ON COLUMN public.sys_config.cfg_value IS 'System payloads, including target email distribution grids for reports';

-- Automatic modification timestamp synchronization mechanism
CREATE OR REPLACE FUNCTION public.update_sys_config_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.cfg_updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sys_config_modtime 
    BEFORE UPDATE ON public.sys_config 
    FOR EACH ROW 
    EXECUTE FUNCTION public.update_sys_config_modified_column();
COMMENT ON TRIGGER update_sys_config_modtime ON public.sys_config IS 'Enforces chronological update tracking across core configuration profiles';
