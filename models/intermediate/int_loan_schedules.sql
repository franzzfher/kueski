{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key = '_installment_id', 
        partition_by={
            "field": "expected_due_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["loan_id"],
        require_partition_filter = false
    )
}}

WITH loans AS (
    SELECT 
        DISTINCT loan_id
        ,TO_JSON_STRING(original_schedule) AS original_schedule
  
    FROM {{ ref('stg_loans') }}
    WHERE original_schedule IS NOT NULL
  
    {% if is_incremental() %}
        AND disbursed_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -15 DAY)
    {% endif %}
)
  
, flattened_schedule AS (
    SELECT
        loan_id
        ,SAFE_CAST(JSON_VALUE(schedule_item, '$.due_date') AS DATE) AS expected_due_date
        ,SAFE_CAST(JSON_VALUE(schedule_item, '$.payment_due') AS FLOAT64) AS expected_total_payment
        ,SAFE_CAST(JSON_VALUE(schedule_item, '$.principal_due') AS FLOAT64) AS expected_principal
        ,SAFE_CAST(JSON_VALUE(schedule_item, '$.interest_due') AS FLOAT64) AS expected_interest

    FROM loans,
    UNNEST(JSON_EXTRACT_ARRAY(original_schedule)) AS schedule_item
)

SELECT 
    *
    ,ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY expected_due_date ASC) AS installment_number
    
    -- key for the merge strategy
    ,TO_HEX(MD5(CONCAT(
        loan_id, 
        '-', 
        CAST(expected_due_date AS STRING)
    ))) AS _installment_id
  
FROM flattened_schedule
