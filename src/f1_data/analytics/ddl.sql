-- ClickHouse DDL for F1 Analytics
-- Database
CREATE DATABASE IF NOT EXISTS f1_analytics;

-- Switch to DB
USE f1_analytics;

-- =========================================================================
-- 1. DRIVERS (Dimension)
-- =========================================================================
CREATE TABLE IF NOT EXISTS drivers (
    driver_id String,
    url String,
    given_name String,
    family_name String,
    date_of_birth Date,
    nationality String,
    permanent_number Nullable(String),
    code Nullable(String),
    batch_id String,
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(batch_id)
ORDER BY driver_id;

-- =========================================================================
-- 2. CONSTRUCTORS (Dimension)
-- =========================================================================
CREATE TABLE IF NOT EXISTS constructors (
    constructor_id String,
    url String,
    name String,
    nationality String,
    batch_id String,
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(batch_id)
ORDER BY constructor_id;

-- =========================================================================
-- 3. RACES (Dimension / Hierarchy)
-- =========================================================================
CREATE TABLE IF NOT EXISTS races (
    season UInt16,
    round UInt8,
    race_name String,
    date Date,
    time Nullable(String),
    url String,
    circuit_circuit_id String,
    circuit_circuit_name String,
    batch_id String,
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(batch_id)
ORDER BY (season, round);

-- =========================================================================
-- 4. RESULTS (Fact Table)
-- =========================================================================
CREATE TABLE IF NOT EXISTS results (
    season UInt16,
    round UInt8,
    race_name String,
    date Date,
    
    -- Result Info
    number String,
    position UInt8,
    position_text String,
    points Float32,
    grid UInt8,
    laps UInt8,
    status String,
    
    -- Foreign Keys (Denormalized names included for ease)
    driver_driver_id String,
    driver_given_name String,
    driver_family_name String,
    driver_nationality String,
    constructor_constructor_id String,
    constructor_name String,
    
    -- Timings
    time_millis Nullable(UInt32),
    time_time Nullable(String),
    fastest_lap_rank Nullable(UInt8),
    fastest_lap_lap Nullable(UInt8),
    fastest_lap_time_time Nullable(String),
    
    batch_id String,
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY season
ORDER BY (season, round, position);

-- =========================================================================
-- 5. LAPS (High Volume Fact)
-- =========================================================================
CREATE TABLE IF NOT EXISTS laps (
    season UInt16,
    round UInt8,
    race_name String,
    date Date,
    circuit_circuit_id String,
    
    lap UInt8,
    driver_id String, 
    position UInt8,
    time String,
    
    batch_id String
) ENGINE = MergeTree()
PARTITION BY season
ORDER BY (season, round, lap, driver_id);

-- =========================================================================
-- 6. PITSTOPS (Fact)
-- =========================================================================
CREATE TABLE IF NOT EXISTS pitstops (
    season UInt16,
    round UInt8,
    race_name String,
    date Date,
    
    driver_id String,
    lap UInt8,
    stop UInt8,
    time String,
    duration String,
    
    batch_id String
) ENGINE = MergeTree()
PARTITION BY season
ORDER BY (season, round, lap, driver_id);

-- =========================================================================
-- 7. QUALIFYING (Fact)
-- =========================================================================
CREATE TABLE IF NOT EXISTS qualifying (
    season UInt16,
    round UInt8,
    race_name String,
    date Date,
    
    position UInt8,
    number String,
    driver_driver_id String,
    constructor_constructor_id String,
    
    q1 Nullable(String),
    q2 Nullable(String),
    q3 Nullable(String),
    
    batch_id String
) ENGINE = MergeTree()
PARTITION BY season
ORDER BY (season, round, position);

-- =========================================================================
-- 8. DRIVER STANDINGS (Fact / Snapshot)
-- =========================================================================
CREATE TABLE IF NOT EXISTS driverstandings (
    season UInt16,
    round UInt8,
    
    position UInt8,
    position_text String,
    points Float32,
    wins UInt8,
    
    driver_driver_id String,
    driver_url String,
    driver_given_name String,
    driver_family_name String,
    driver_date_of_birth Date,
    driver_nationality String,
    
    batch_id String
) ENGINE = MergeTree()
PARTITION BY season
ORDER BY (season, round, position);

-- =========================================================================
-- 9. CONSTRUCTOR STANDINGS (Fact / Snapshot)
-- =========================================================================
CREATE TABLE IF NOT EXISTS constructorstandings (
    season UInt16,
    round UInt8,
    
    position UInt8,
    position_text String,
    points Float32,
    wins UInt8,
    
    constructor_constructor_id String,
    constructor_url String,
    constructor_name String,
    constructor_nationality String,

    batch_id String
) ENGINE = MergeTree()
PARTITION BY season
ORDER BY (season, round, position);

-- =========================================================================
-- 10. SPRINT (Fact)
-- =========================================================================
CREATE TABLE IF NOT EXISTS sprint (
    season UInt16,
    round UInt8,
    race_name String,
    date Date,
    
    position UInt8,
    number String,
    points Float32,
    grid UInt8,
    laps UInt8,
    status String,
    
    driver_driver_id String,
    constructor_constructor_id String,
    
    time_time Nullable(String),
    time_millis Nullable(UInt32),
    
    batch_id String
) ENGINE = MergeTree()
PARTITION BY season
ORDER BY (season, round, position);
