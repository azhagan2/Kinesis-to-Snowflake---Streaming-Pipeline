USE DATABASE RETAIL_DEMO_DB;


/* Creating silver schema for processing valid data and gold schema 
for summary tables for use of business */

CREATE OR REPLACE SCHEMA RETAIL_DEMO_DB.SILVER;

CREATE OR REPLACE SCHEMA RETAIL_DEMO_DB.GOLD;


/* Table used for storing processed only-valid data */ 

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.SILVER.RETAIL_POS_PROCESSED (
    store_id STRING,
    city STRING,
    transaction_id STRING,
    timestamp TIMESTAMP_LTZ,
    product_id STRING,
    price FLOAT,
    quantity INTEGER,
    total_amount FLOAT,
    payment_method STRING,
    is_canceled BOOLEAN,
    is_refunded BOOLEAN,
    refund_amount FLOAT,
    refund_method STRING,
    dq_check_passed BOOLEAN,  -- Optional: flag if record passed DQ rules
    dq_error STRING           -- Optional: stores reason if failed
);


/* Table used for storing processed DQ failed / rejected data */ 

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.SILVER.RETAIL_POS_REJECTED (
    store_id STRING,
    city STRING,
    transaction_id STRING,
    timestamp TIMESTAMP_LTZ,
    product_id STRING,
    price FLOAT,
    quantity INTEGER,
    total_amount FLOAT,
    payment_method STRING,
    raw_refund STRING,
    raw_cancel_item STRING,
    dq_error STRING,
    load_time TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);


-- A table maintained for inventory management

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.SILVER.Inventory (
    id STRING PRIMARY KEY,     -- Product ID as STRING
    item_name STRING,
    price FLOAT,
    units_sold INTEGER DEFAULT 0,
    units_left INTEGER,
    cost_price FLOAT,
    reorder_point INTEGER,
    description STRING,
    image STRING,
    last_updated TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);


-- Store table for maintaining store details

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.SILVER.dim_store (
    store_id STRING PRIMARY KEY,
    store_name STRING,
    city STRING,
    region STRING,
    country STRING,
    is_active BOOLEAN
);


-- Products table for maintaining product details

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.SILVER.dim_product (
    product_id STRING PRIMARY KEY,
    product_name STRING,
    category STRING,
    sub_category STRING,
    brand STRING,
    price FLOAT
);


-- Payments meta table

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.SILVER.dim_payment_method (
    payment_method STRING PRIMARY KEY,
    method_type STRING,  -- e.g., Card, Cash, Wallet
    provider STRING       -- e.g., Visa, Mastercard, PayPal
);


-- A Common Date and time table useful in transformation logics

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.SILVER.dim_time (
    timestamp TIMESTAMP_LTZ PRIMARY KEY,
    date DATE,
    year INT,
    quarter STRING,
    month INT,
    day INT,
    day_of_week STRING,
    hour INT
);


-- Error log Table 

CREATE TABLE IF NOT EXISTS RETAIL_DEMO_DB.SILVER.Error_log (
    task_name STRING,
    error_message STRING,
    error_time TIMESTAMP
);


-- Daily sales summary table used for Dashboards

CREATE OR REPLACE TABLE RETAIL_DEMO_DB.GOLD.Retail_sales_daily_summary (
    summary_date DATE,
    store_id STRING,
    store_name STRING,
    city STRING,
    total_sales FLOAT,
    total_transactions INT,
    total_quantity INT,
    total_refunds FLOAT
);