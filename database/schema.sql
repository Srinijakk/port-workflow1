-- =====================================================
-- Port Management Database Schema
-- Database: portmanagement
-- PostgreSQL Version: 17
-- =====================================================

-- Drop tables if exist (for clean re-creation)
DROP TABLE IF EXISTS transport_mean CASCADE;
DROP TABLE IF EXISTS storage CASCADE;
DROP TABLE IF EXISTS container CASCADE;
DROP TABLE IF EXISTS process_instance CASCADE;

-- =====================================================
-- 1. PROCESS_INSTANCE TABLE
-- Stores Camunda workflow process instance information
-- This table starts empty - populated dynamically by worker
-- =====================================================
CREATE TABLE process_instance (
    process_instance_key SERIAL PRIMARY KEY,
    description VARCHAR(255)
);

COMMENT ON TABLE process_instance IS 'Stores Camunda workflow process instance keys (populated in real-time)';
COMMENT ON COLUMN process_instance.process_instance_key IS 'Camunda workflow key fetched from running process';
COMMENT ON COLUMN process_instance.description IS 'Optional description of the process instance';

-- =====================================================
-- 2. CONTAINER TABLE
-- Stores container information
-- =====================================================
CREATE TABLE container (
    container_id VARCHAR(50) PRIMARY KEY,
    operation_type VARCHAR(20) NOT NULL CHECK (operation_type IN ('loading', 'unloading')),
    weight INTEGER NOT NULL
);

COMMENT ON TABLE container IS 'Stores container information including ID, operation type, and weight';
COMMENT ON COLUMN container.container_id IS 'Unique container identifier (e.g., C1001-C1010)';
COMMENT ON COLUMN container.operation_type IS 'Type of operation: loading or unloading';
COMMENT ON COLUMN container.weight IS 'Container weight in kilograms';

-- =====================================================
-- 3. STORAGE TABLE
-- Stores container storage status
-- =====================================================
CREATE TABLE storage (
    storage_id SERIAL PRIMARY KEY,
    container_id VARCHAR(50) NOT NULL REFERENCES container(container_id) ON DELETE CASCADE,
    storage_status VARCHAR(50) NOT NULL CHECK (storage_status IN ('complete', 'incomplete'))
);

COMMENT ON TABLE storage IS 'Tracks storage status for each container';
COMMENT ON COLUMN storage.storage_id IS 'Unique storage record identifier';
COMMENT ON COLUMN storage.container_id IS 'References container table';
COMMENT ON COLUMN storage.storage_status IS 'Either complete or incomplete';

-- =====================================================
-- 4. TRANSPORT_MEAN TABLE
-- Stores transportation information (trucks and ships)
-- =====================================================
CREATE TABLE transport_mean (
    transportation_id VARCHAR(20) PRIMARY KEY,
    container_id VARCHAR(50) NOT NULL REFERENCES container(container_id) ON DELETE CASCADE,
    operation_type VARCHAR(20) NOT NULL CHECK (operation_type IN ('loading', 'unloading')),
    storage_id INTEGER NOT NULL REFERENCES storage(storage_id) ON DELETE CASCADE,
    check_in TIMESTAMP,
    check_out TIMESTAMP,
    CONSTRAINT check_truck_timestamps CHECK (
        (transportation_id LIKE 'truck%' AND check_in IS NOT NULL AND check_out IS NOT NULL) OR
        (transportation_id LIKE 'ship%' AND check_in IS NULL AND check_out IS NULL)
    )
);

COMMENT ON TABLE transport_mean IS 'Stores transport vehicle information (trucks and ships)';
COMMENT ON COLUMN transport_mean.transportation_id IS 'Transport ID (e.g., truck101, ship9109)';
COMMENT ON COLUMN transport_mean.container_id IS 'References container table';
COMMENT ON COLUMN transport_mean.operation_type IS 'Type of operation: loading or unloading';
COMMENT ON COLUMN transport_mean.storage_id IS 'References storage table';
COMMENT ON COLUMN transport_mean.check_in IS 'Check-in timestamp (trucks only)';
COMMENT ON COLUMN transport_mean.check_out IS 'Check-out timestamp (trucks only)';

-- =====================================================
-- INDEXES for Performance
-- =====================================================
CREATE INDEX idx_container_operation_type ON container(operation_type);
CREATE INDEX idx_storage_container_id ON storage(container_id);
CREATE INDEX idx_storage_status ON storage(storage_status);
CREATE INDEX idx_transport_container_id ON transport_mean(container_id);
CREATE INDEX idx_transport_storage_id ON transport_mean(storage_id);
CREATE INDEX idx_transport_type ON transport_mean(transportation_id);

-- =====================================================
-- VIEWS for Analytics
-- =====================================================

-- View: Active operations (incomplete storage)
CREATE OR REPLACE VIEW active_operations AS
SELECT 
    c.container_id,
    c.operation_type,
    c.weight,
    s.storage_id,
    s.storage_status,
    t.transportation_id,
    t.check_in,
    t.check_out
FROM container c
JOIN storage s ON c.container_id = s.container_id
JOIN transport_mean t ON c.container_id = t.container_id
WHERE s.storage_status = 'incomplete'
ORDER BY c.container_id;

-- View: Completed operations summary
CREATE OR REPLACE VIEW completed_operations_summary AS
SELECT 
    c.operation_type,
    COUNT(*) as total_operations,
    AVG(c.weight) as avg_weight,
    MIN(c.weight) as min_weight,
    MAX(c.weight) as max_weight
FROM container c
JOIN storage s ON c.container_id = s.container_id
WHERE s.storage_status = 'complete'
GROUP BY c.operation_type
ORDER BY c.operation_type;

-- View: Truck operations
CREATE OR REPLACE VIEW truck_operations AS
SELECT 
    t.transportation_id,
    c.container_id,
    c.operation_type,
    c.weight,
    s.storage_status,
    t.check_in,
    t.check_out,
    EXTRACT(EPOCH FROM (t.check_out - t.check_in))/60 as duration_minutes
FROM transport_mean t
JOIN container c ON t.container_id = c.container_id
JOIN storage s ON t.storage_id = s.storage_id
WHERE t.transportation_id LIKE 'truck%'
ORDER BY t.check_in DESC;

-- View: Ship operations
CREATE OR REPLACE VIEW ship_operations AS
SELECT 
    t.transportation_id,
    c.container_id,
    c.operation_type,
    c.weight,
    s.storage_status
FROM transport_mean t
JOIN container c ON t.container_id = c.container_id
JOIN storage s ON t.storage_id = s.storage_id
WHERE t.transportation_id LIKE 'ship%'
ORDER BY t.transportation_id;

-- =====================================================
-- Grant permissions (adjust user as needed)
-- =====================================================
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;