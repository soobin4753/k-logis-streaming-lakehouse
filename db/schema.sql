DROP TABLE IF EXISTS mart_risk_summary;
DROP TABLE IF EXISTS mart_hub_performance;
DROP TABLE IF EXISTS mart_region_delay;
DROP TABLE IF EXISTS shipment_metrics;

DROP TABLE IF EXISTS dispatch;
DROP TABLE IF EXISTS shipment;
DROP TABLE IF EXISTS vehicle;
DROP TABLE IF EXISTS driver;
DROP TABLE IF EXISTS hub;
DROP TABLE IF EXISTS region;

CREATE TABLE region (
    region_id VARCHAR(20) PRIMARY KEY,
    region_name VARCHAR(50) NOT NULL,
    region_group VARCHAR(50) NOT NULL
);

CREATE TABLE hub (
    hub_id VARCHAR(20) PRIMARY KEY,
    hub_name VARCHAR(100) NOT NULL,
    region_id VARCHAR(20) NOT NULL REFERENCES region(region_id),
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    hub_type VARCHAR(30) NOT NULL
);

CREATE TABLE driver (
    driver_id VARCHAR(20) PRIMARY KEY,
    driver_name VARCHAR(50) NOT NULL,
    region_id VARCHAR(20) NOT NULL REFERENCES region(region_id),
    license_type VARCHAR(30) NOT NULL,
    driver_status VARCHAR(30) NOT NULL
);

CREATE TABLE vehicle (
    vehicle_id VARCHAR(20) PRIMARY KEY,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg INTEGER NOT NULL,
    region_id VARCHAR(20) NOT NULL REFERENCES region(region_id),
    vehicle_status VARCHAR(30) NOT NULL
);

CREATE TABLE shipment (
    shipment_id VARCHAR(50) PRIMARY KEY,
    origin_region_id VARCHAR(20) NOT NULL REFERENCES region(region_id),
    destination_region_id VARCHAR(20) NOT NULL REFERENCES region(region_id),
    origin_hub_id VARCHAR(20) NOT NULL REFERENCES hub(hub_id),
    destination_hub_id VARCHAR(20) NOT NULL REFERENCES hub(hub_id),
    origin_latitude DOUBLE PRECISION NOT NULL,
    origin_longitude DOUBLE PRECISION NOT NULL,
    destination_latitude DOUBLE PRECISION NOT NULL,
    destination_longitude DOUBLE PRECISION NOT NULL,
    cargo_type VARCHAR(50) NOT NULL,
    cargo_weight_kg DOUBLE PRECISION NOT NULL,
    shipping_costs DOUBLE PRECISION,
    lead_time_days DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL,
    promised_delivery_at TIMESTAMP NOT NULL,
    shipment_status VARCHAR(30) NOT NULL
);

CREATE TABLE dispatch (
    dispatch_id VARCHAR(50) PRIMARY KEY,
    shipment_id VARCHAR(50) NOT NULL REFERENCES shipment(shipment_id),
    driver_id VARCHAR(20) NOT NULL REFERENCES driver(driver_id),
    vehicle_id VARCHAR(20) NOT NULL REFERENCES vehicle(vehicle_id),
    assigned_at TIMESTAMP NOT NULL,
    dispatch_status VARCHAR(30) NOT NULL
);

CREATE TABLE shipment_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    shipment_id VARCHAR(50) UNIQUE,
    dispatch_id VARCHAR(50),
    driver_id VARCHAR(20),
    vehicle_id VARCHAR(20),
    origin_region_id VARCHAR(20),
    destination_region_id VARCHAR(20),
    origin_hub_id VARCHAR(20),
    destination_hub_id VARCHAR(20),
    first_event_time TIMESTAMP,
    delivered_time TIMESTAMP,
    total_delivery_minutes DOUBLE PRECISION,
    promised_delivery_at TIMESTAMP,
    delay_minutes DOUBLE PRECISION,
    is_delayed BOOLEAN,
    event_count INTEGER,
    avg_delay_probability DOUBLE PRECISION,
    max_delay_probability DOUBLE PRECISION,
    avg_traffic_congestion_level DOUBLE PRECISION,
    avg_weather_severity DOUBLE PRECISION,
    avg_hub_congestion_level DOUBLE PRECISION,
    exception_count INTEGER,
    final_risk_classification VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mart_region_delay (
    mart_id BIGSERIAL PRIMARY KEY,
    event_date DATE,
    destination_region_id VARCHAR(20),
    total_event_count INTEGER,
    delayed_event_count INTEGER,
    delay_rate DOUBLE PRECISION,
    avg_delay_probability DOUBLE PRECISION,
    avg_eta_variation_minutes DOUBLE PRECISION,
    high_risk_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mart_hub_performance (
    mart_id BIGSERIAL PRIMARY KEY,
    event_date DATE,
    destination_hub_id VARCHAR(20),
    total_event_count INTEGER,
    delayed_event_count INTEGER,
    delay_rate DOUBLE PRECISION,
    avg_traffic_congestion_level DOUBLE PRECISION,
    avg_hub_congestion_level DOUBLE PRECISION,
    avg_delay_probability DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mart_risk_summary (
    mart_id BIGSERIAL PRIMARY KEY,
    event_date DATE,
    risk_classification VARCHAR(30),
    event_count INTEGER,
    avg_delay_probability DOUBLE PRECISION,
    avg_weather_severity DOUBLE PRECISION,
    avg_traffic_congestion_level DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);