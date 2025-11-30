USE DATABASE RETAIL_DEMO_DB;

USE SCHEMA SILVER;


-- Create task for calling the PROCESS_RETAIL_POS_PROC for process valid data 

CREATE OR REPLACE TASK RETAIL_DEMO_DB.SILVER.PROCESS_RETAIL_POS_RAW_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '2 Minutes'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DEMO_DB.BRONZE.RETAIL_POS_RAW_STREAM')
AS 
CALL RETAIL_DEMO_DB.SILVER.PROCESS_RETAIL_POS_PROC();




-- Creating two streams for use cases like, inventory and daily sales dashboard

CREATE OR REPLACE STREAM RETAIL_DEMO_DB.SILVER.RETAIL_POS_PROCESSED_STREAM_1 
ON TABLE RETAIL_DEMO_DB.SILVER.RETAIL_POS_PROCESSED
APPEND_ONLY = TRUE;


CREATE OR REPLACE STREAM RETAIL_DEMO_DB.SILVER.RETAIL_POS_PROCESSED_STREAM_2
ON TABLE RETAIL_DEMO_DB.SILVER.RETAIL_POS_PROCESSED
APPEND_ONLY = TRUE;




-- Creating a task for updating enteries in the Inventory table, by checking if its not there

CREATE OR REPLACE TASK RETAIL_DEMO_DB.SILVER.INVENTORY_INSERT_TASK
WAREHOUSE = COMPUTE_WH
AFTER RETAIL_DEMO_DB.SILVER.PROCESS_RETAIL_POS_RAW_TASK
AS
INSERT INTO RETAIL_DEMO_DB.SILVER.INVENTORY 
(id, item_name, price, units_sold, units_left, cost_price, reorder_point, description, image)

SELECT DISTINCT r.product_id, p.product_name, r.price, 0, 100, p.price, 75, '', ''
FROM RETAIL_DEMO_DB.SILVER.RETAIL_POS_PROCESSED r
JOIN RETAIL_DEMO_DB.SILVER.dim_product p ON r.product_id = p.product_id
WHERE r.dq_check_passed = TRUE
AND NOT EXISTS (
      SELECT 1 FROM retail_analytics_db.silver.inventory i WHERE i.id = r.product_id
  );



-- Creating a task for updaing the inventory updating from the RETAIL_POS_PROCESSED_STREAM_1


CREATE OR REPLACE TASK RETAIL_DEMO_DB.SILVER.UPDATE_INVENTORY_TASK
WAREHOUSE = COMPUTE_WH
AFTER RETAIL_DEMO_DB.SILVER.INVENTORY_INSERT_TASK
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DEMO_DB.BRONZE.RETAIL_POS_PROCESSED_STREAM_1')
AS
MERGE INTO retail_analytics_db.silver.inventory AS inv
USING (
  SELECT product_id, 
         SUM(CASE WHEN is_canceled THEN 0
                  WHEN is_refunded THEN -quantity ELSE quantity 
            END) AS net_quantity,
         SUM(CASE WHEN is_canceled THEN 0
                  WHEN is_refunded THEN -1 ELSE 1 
            END) AS net_units
  FROM RETAIL_DEMO_DB.BRONZE.RETAIL_POS_PROCESSED_STREAM_1
  WHERE dq_check_passed
  GROUP BY product_id
  ) AS tx
ON inv.id = tx.product_id
WHEN MATCHED THEN  
UPDATE SET 
    units_left = inv.units_left - tx.net_units,
    units_sold = inv.units_sold + tx.net_units;


    
-- Creating a Task for updating the daily sales summary table

CREATE OR REPLACE TASK RETAIL_DEMO_DB.SILVER.RETAIL_SALES_DAILY_SUMMARY_TASK
WAREHOUSE = COMPUTE_WH
AFTER RETAIL_DEMO_DB.SILVER.PROCESS_RETAIL_POS_RAW_TASK
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DEMO_DB.BRONZE.RETAIL_POS_PROCESSED_STREAM_2')
AS
MERGE INTO RETAIL_DEMO_DB.GOLD.RETAIL_SALES_DAILY_SUMMARY tgt
USING (
    SELECT
        DATE(rp.timestamp) AS summary_date,
        rp.store_id,
        ds.store_name,
        ds.city,
        SUM(rp.total_amount) AS total_sales,
        COUNT(DISTINCT rp.transaction_id) AS total_transactions,
        SUM(rp.quantity) AS total_quantity,
        SUM(CASE WHEN rp.is_refunded THEN rp.refund_amount ELSE 0 END) AS total_refunds
    FROM RETAIL_DEMO_DB.SILVER.RETAIL_POS_PROCESSED_STREAM_2 rp
    LEFT JOIN RETAIL_DEMO_DB.SILVER.dim_store ds ON rp.store_id = ds.store_id
    WHERE rp.is_canceled = FALSE
    GROUP BY 1, 2, 3, 4
) src
ON tgt.store_id = src.store_id AND tgt.summary_date = src.summary_date
WHEN MATCHED THEN UPDATE SET
    store_name = src.store_name,
    city = src.city,
    total_sales = src.total_sales,
    total_transactions = src.total_transactions,
    total_quantity = src.total_quantity,
    total_refunds = src.total_refunds
WHEN NOT MATCHED THEN INSERT (
    summary_date, store_id, store_name, city, total_sales, total_transactions, total_quantity, total_refunds)
VALUES (
    src.summary_date, src.store_id, src.store_name, src.city, src.total_sales, src.total_transactions, src.total_quantity, src.total_refunds
);


