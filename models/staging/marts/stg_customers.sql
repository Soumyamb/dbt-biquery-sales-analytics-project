WITH raw_customers_data AS (
    SELECT *
    FROM {{ source('marketing_test', 'customers_data') }}
)

SELECT * FROM raw_customers_data
