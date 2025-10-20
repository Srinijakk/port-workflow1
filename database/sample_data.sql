-- =====================================================
-- Sample Data for Port Management System
-- Database: portmanagement
-- 10 Sample Container Operations with Transport
-- =====================================================

-- =====================================================
-- 1. PROCESS_INSTANCE TABLE
-- Starts empty - will be populated dynamically by Camunda worker
-- when workflow instances are created at localhost:8080
-- =====================================================
-- No initial data

-- =====================================================
-- 2. CONTAINER TABLE
-- 10 containers alternating between loading and unloading
-- =====================================================
-- =====================================================
-- Port Management Sample Data (Extended)
-- Database: portmanagement
-- =====================================================

-- 1. process_instance
-- No data inserted initially; will be populated in real-time from Camunda

-- =====================================================
-- 2. container
-- =====================================================
INSERT INTO container (container_id, operation_type, weight) VALUES
-- Existing 10
('C1001', 'loading', 12000),
('C1002', 'unloading', 15000),
('C1003', 'loading', 10000),
('C1004', 'unloading', 18000),
('C1005', 'loading', 13000),
('C1006', 'unloading', 16000),
('C1007', 'loading', 14000),
('C1008', 'unloading', 12500),
('C1009', 'loading', 15500),
('C1010', 'unloading', 17000),
-- Newly added 10
('C1011', 'loading', 11000),
('C1012', 'unloading', 14500),
('C1013', 'loading', 13500),
('C1014', 'unloading', 15000),
('C1015', 'loading', 14200),
('C1016', 'unloading', 17500),
('C1017', 'loading', 12800),
('C1018', 'unloading', 16500),
('C1019', 'loading', 13800),
('C1020', 'unloading', 17200);

-- =====================================================
-- 3. storage
-- =====================================================
INSERT INTO storage (storage_id, container_id, storage_status) VALUES
-- Existing 10
(1, 'C1001', 'complete'),
(2, 'C1002', 'incomplete'),
(3, 'C1003', 'complete'),
(4, 'C1004', 'complete'),
(5, 'C1005', 'incomplete'),
(6, 'C1006', 'complete'),
(7, 'C1007', 'incomplete'),
(8, 'C1008', 'complete'),
(9, 'C1009', 'complete'),
(10, 'C1010', 'incomplete'),
-- Newly added 10
(11, 'C1011', 'complete'),
(12, 'C1012', 'complete'),
(13, 'C1013', 'incomplete'),
(14, 'C1014', 'complete'),
(15, 'C1015', 'incomplete'),
(16, 'C1016', 'complete'),
(17, 'C1017', 'complete'),
(18, 'C1018', 'incomplete'),
(19, 'C1019', 'complete'),
(20, 'C1020', 'incomplete');

-- =====================================================
-- 4. transport_mean
-- =====================================================
INSERT INTO transport_mean (transportation_id, container_id, operation_type, storage_id, check_in, check_out) VALUES
-- Existing 10
('truck101', 'C1001', 'loading', 1, '2025-10-08 08:00:00', '2025-10-08 10:00:00'),
('ship9109', 'C1002', 'unloading', 2, NULL, NULL),
('truck102', 'C1003', 'loading', 3, '2025-10-08 09:00:00', '2025-10-08 11:30:00'),
('ship9110', 'C1004', 'unloading', 4, NULL, NULL),
('truck103', 'C1005', 'loading', 5, '2025-10-08 10:15:00', '2025-10-08 12:45:00'),
('ship9111', 'C1006', 'unloading', 6, NULL, NULL),
('truck104', 'C1007', 'loading', 7, '2025-10-08 11:00:00', '2025-10-08 13:30:00'),
('ship9112', 'C1008', 'unloading', 8, NULL, NULL),
('truck105', 'C1009', 'loading', 9, '2025-10-08 12:00:00', '2025-10-08 14:30:00'),
('ship9113', 'C1010', 'unloading', 10, NULL, NULL),
-- Newly added 10 (ships and trucks performing both loading/unloading)
('ship9114', 'C1011', 'loading', 11, NULL, NULL),
('truck106', 'C1012', 'unloading', 12, '2025-10-08 13:00:00', '2025-10-08 15:15:00'),
('ship9115', 'C1013', 'loading', 13, NULL, NULL),
('truck107', 'C1014', 'unloading', 14, '2025-10-08 14:00:00', '2025-10-08 16:30:00'),
('ship9116', 'C1015', 'unloading', 15, NULL, NULL),
('truck108', 'C1016', 'loading', 16, '2025-10-08 15:00:00', '2025-10-08 17:20:00'),
('ship9117', 'C1017', 'unloading', 17, NULL, NULL),
('truck109', 'C1018', 'loading', 18, '2025-10-08 16:00:00', '2025-10-08 18:15:00'),
('ship9118', 'C1019', 'loading', 19, NULL, NULL),
('truck110', 'C1020', 'unloading', 20, '2025-10-08 17:00:00', '2025-10-08 19:30:00');

-- =====================================================
-- Verification Queries
-- =====================================================

-- Total records summary
SELECT 
    (SELECT COUNT(*) FROM process_instance) as process_instances,
    (SELECT COUNT(*) FROM container) as containers,
    (SELECT COUNT(*) FROM storage) as storage_records,
    (SELECT COUNT(*) FROM transport_mean) as transport_records;

-- Storage status breakdown
SELECT 
    storage_status,
    COUNT(*) as count
FROM storage
GROUP BY storage_status
ORDER BY storage_status;

-- Transport type breakdown
SELECT 
    CASE 
        WHEN transportation_id LIKE 'truck%' THEN 'Truck'
        WHEN transportation_id LIKE 'ship%' THEN 'Ship'
        ELSE 'Other'
    END as transport_type,
    COUNT(*) as count
FROM transport_mean
GROUP BY 
    CASE 
        WHEN transportation_id LIKE 'truck%' THEN 'Truck'
        WHEN transportation_id LIKE 'ship%' THEN 'Ship'
        ELSE 'Other'
    END
ORDER BY transport_type;

-- Operation type breakdown
SELECT 
    operation_type,
    COUNT(*) as count
FROM container
GROUP BY operation_type
ORDER BY operation_type;

-- Complete data view
SELECT 
    c.container_id,
    c.operation_type,
    c.weight,
    s.storage_status,
    t.transportation_id,
    t.check_in,
    t.check_out
FROM container c
JOIN storage s ON c.container_id = s.container_id
JOIN transport_mean t ON c.container_id = t.container_id
ORDER BY c.container_id;