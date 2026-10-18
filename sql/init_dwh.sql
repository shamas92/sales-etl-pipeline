CREATE DATABASE sales_dwh;
\connect sales_dwh;

CREATE SCHEMA IF NOT EXISTS dwh;

-- Dimension: Date
CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY,          -- Format: YYYYMMDD (e.g., 20261015)
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dimension: Customer
CREATE TABLE dwh.dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_region VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Product
CREATE TABLE dwh.dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category VARCHAR(100) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL
);

-- Fact Table: Sales Transactions
CREATE TABLE dwh.fact_sales (
    transaction_id VARCHAR(64) PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    customer_id VARCHAR(50) NOT NULL REFERENCES dwh.dim_customer(customer_id),
    product_id VARCHAR(50) NOT NULL REFERENCES dwh.dim_product(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    transaction_total NUMERIC(12, 2) NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- OLAP Optimization Indexes
CREATE INDEX idx_fact_sales_date_key ON dwh.fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON dwh.fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product ON dwh.fact_sales(product_id);