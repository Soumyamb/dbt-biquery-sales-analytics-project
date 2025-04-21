
WITH raw_customers_data AS (
    -- Fetch data from the source and add `current_flag`
    SELECT 
        customer_id,
        gender,
        payment_method,
        age,
        1 AS current_flag   -- Set current_flag to 1 for new records
    FROM {{ source('marketing_test', 'customers_data') }}
),

existing AS (
    -- Get existing data from the incremental table
    SELECT *
    FROM {{ this }}  -- `{{ this }}` references the current model
),

new_records AS (
    -- Get new or changed records (left join to check if they already exist)
    SELECT 
        raw.* 
    FROM raw_customers_data raw
    LEFT JOIN existing e 
        ON raw.customer_id = e.customer_id
        AND (raw.payment_method != e.payment_method OR raw.age != e.age)  -- Check for changes in payment_method or age
        AND e.current_flag = 1  -- Only consider active (current) records
    WHERE e.customer_id IS NULL  -- Select only new records or changed records
),

updates AS (
    -- Update old records by setting `current_flag` to 0 and creating a new row with `current_flag` as 1
    SELECT 
        e.customer_id,
        e.gender,
        e.payment_method,
        e.age,
        0 AS current_flag  -- Mark old records as expired (current_flag = 0)
    FROM existing e
    WHERE e.customer_id IN (SELECT customer_id FROM new_records)  -- Filter only the records that are being updated
      AND e.current_flag = 1  -- Ensure that we only update active records
)

-- Final SELECT to combine both new and updated records
SELECT 
    customer_id,
    gender,
    payment_method,
    age,
    current_flag
FROM new_records

UNION ALL

SELECT 
    customer_id,
    gender,
    payment_method,
    age,
    current_flag
FROM updates
