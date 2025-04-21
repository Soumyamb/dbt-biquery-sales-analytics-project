WITH int_image_invoices_customers as (
 Select invoice_id,
        invoice_date,
        customer_id
        image_type,
        quantity,
        price,
        quantity * price as total_amount
        employee_name,
FROM {{ref ('stg_image_invoices')}}
)

Select 