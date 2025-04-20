WITH image_invoices_cleaned AS (
    SELECT 
        invoice_no AS invoice_id,
        invoice_date,
        customer_id,
        CASE 
            WHEN category IN ('Toys', 'Clothing', 'Shoes') THEN 'Sport'
            ELSE 'Stock'
        END AS image_type,
        quantity,
        price,
        CASE 
            WHEN shopping_mall = 'Cevahir AVM' THEN 'Anna Müller'
            WHEN shopping_mall = 'Emaar Square Mall' THEN 'Luca Rossi'
            WHEN shopping_mall = 'Forum Istanbul' THEN 'Emilie Dubois'
            WHEN shopping_mall = 'Istinye Park' THEN 'Tomáš Novák'
            WHEN shopping_mall = 'Kanyon' THEN 'Sofia Petrova'
            WHEN shopping_mall = 'Mall of Istanbul' THEN 'Erik Johansson'
            WHEN shopping_mall = 'Metrocity' THEN 'Maria García'
            WHEN shopping_mall = 'Metropol AVM' THEN 'Andreas Papadopoulos'
            WHEN shopping_mall = 'Viaport Outlet' THEN 'Nora Schmidt'
            WHEN shopping_mall = 'Zorlu Center' THEN 'Olivier Martin'
            ELSE 'Unknown'
        END AS employee_name     
    FROM {{ source('marketing_test', 'image_sales') }}  -- Use source function to reference the source table
)

SELECT * 
FROM image_invoices_cleaned
