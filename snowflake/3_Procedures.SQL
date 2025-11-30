USE DATABASE RETAIL_DEMO_DB;

USE SCHEMA SILVER;


/*This Procedure will helps in extracting data from raw_json data into stage table, 
followed by it undergoes data quality checks, and marked by some columns, only 
valid data enters into main table, merge the valid good data into RETAIL_POS_PROCESSED 
and store the bad data in RETAIL_POS_REJECTED*/

CREATE OR REPLACE PROCEDURE RETAIL_DEMO_DB.SILVER.PROCESS_RETAIL_POS_PROC()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN 
    -- Step 1: Create temp staging table
    CREATE OR REPLACE TEMP TABLE RETAIL_DEMO_DB.SILVER.RETAIL_POS_STAGING AS 
    WITH FLATTENED AS (
        SELECT 
            raw_data:"store_id"::STRING AS store_id,
            raw_data:"city"::STRING AS city,
            raw_data:"transaction_id"::STRING AS transaction_id,
            raw_data:"timestamp"::TIMESTAMP_LTZ AS timestamp,
            item.value:"product_id"::STRING AS product_id,
            item.value:"price"::FLOAT AS price,
            item.value:"quantity"::INTEGER AS quantity,
            item.value:"canceled"::BOOLEAN AS cancelled,
            raw_data:"total_amount"::FLOAT AS total_amount,
            raw_data:"payment_method"::STRING AS payment_method,
            raw_data:"refund"::STRING AS refund_raw,
            raw_data:"cancel_item"::STRING AS cancel_item_raw
        FROM RETAIL_DEMO_DB.BRONZE.RETAIL_RAW_POS_STREAM,
            LATERAL FLATTEN(input => raw_data:"items") item
    
    ),

    parsed as (
        SELECT *, 
            TRY_PARSE_JSON(refund_raw) as refun_json, 
            TRY_PARSE_JSON(cancel_item_raw) as cancel_json
        FROM FLATTEN
    )

    SELECT *, 
        CASE WHEN cancelled = TRUE THEN TRUE ELSE FALSE END AS is_canceled,
        CASE WHEN refund_json:"refund_amount"::FLOAT IS NOT NULL THEN TRUE ELSE FALSE END AS is_refunded,
        refund_json:"refund_amount"::FLOAT AS refund_amount,
        refund_json:"refund_method"::STRING AS refund_method,
        CASE 
            WHEN store_id IS NULL OR transaction_id IS NULL OR product_id IS NULL THEN FALSE
            WHEN price IS NULL OR price <= 0 THEN FALSE
            WHEN quantity IS NULL OR quantity <= 0 THEN FALSE
            ELSE TRUE
        END AS dq_check_passed,
        CASE 
            WHEN store_id IS NULL OR transaction_id IS NULL OR product_id IS NULL THEN 'Missing required fields'
            WHEN price IS NULL OR price <= 0 THEN 'Invalid price'
            WHEN quantity IS NULL OR quantity <= 0 THEN 'Invalid quantity'
            ELSE NULL
        END AS dq_error
    FROM parsed;


    -- Step 2: MERGE good data
    MERGE INTO RETAIL_DEMO_DB.SILVER.RETAIL_POS_PROCESSED AS tgt 
    USING (SELECT * FROM RETAIL_DEMO_DB.SILVER.RETAIL_POS_STAGING WHERE dq_check_passed = TRUE) as src
    ON tgt.transaction_id = src_transaction_id and tgt.product_id = src.product_id
    WHEN MATCHED THEN UPDATE SET 
        store_id = src.store_id,
        city = src.city,
        timestamp = src.timestamp,
        price = src.price,
        quantity = src.quantity,
        total_amount = src.total_amount,
        payment_method = src.payment_method,
        is_canceled = src.is_canceled,
        is_refunded = src.is_refunded,
        refund_amount = src.refund_amount,
        refund_method = src.refund_method,
        dq_check_passed = TRUE,
        dq_error = NULL
    WHEN NOT MATCHED THEN INSERT (
        store_id, city, transaction_id, timestamp, product_id,
        price, quantity, total_amount, payment_method,
        is_canceled, is_refunded, refund_amount, refund_method,
        dq_check_passed, dq_error
    ) VALUES (
        src.store_id, src.city, src.transaction_id, src.timestamp, src.product_id,
        src.price, src.quantity, src.total_amount, src.payment_method,
        src.is_canceled, src.is_refunded, src.refund_amount, src.refund_method,
        TRUE, NULL
    );

    -- Step 3: Insert rejected data
    INSERT INTO retail_analytics_db.silver.retail_pos_rejected (
        store_id, city, transaction_id, timestamp, product_id,
        price, quantity, total_amount, payment_method,
        raw_refund, raw_cancel_item, dq_error)
    SELECT
        store_id, city, transaction_id, timestamp, product_id,
        price, quantity, total_amount, payment_method,
        refund_raw, cancel_item_raw, dq_error
    FROM retail_analytics_db.silver.retail_pos_staging
    WHERE dq_check_passed = FALSE;

RETURN 'Processing complete.';

END;
$$;






    