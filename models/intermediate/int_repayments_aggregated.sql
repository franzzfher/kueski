{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge', 
        unique_key = 'loan_id',
        cluster_by = ["user_id", "loan_id"],
        partition_by={
            "field": "last_payment_date", 
            "data_type": "date",
            "granularity": "day"
        } 
    )
}}

-- if incremental, we first find the ids that need updating.
{% if is_incremental() %}
    WITH changed_loans AS (
        SELECT DISTINCT loan_id
        FROM {{ ref('stg_repays') }}
        WHERE DATE(event_date) >= DATE_ADD(CURRENT_DATE(), INTERVAL -15 DAY)
    )
{% endif %}

SELECT 
    a.loan_id
    ,a.user_id
    
    -- 1. total rev components 
    ,SUM(a.paid_principal) AS total_principal_paid
    ,SUM(a.paid_interest) AS total_interest_paid
    ,SUM(a.paid_fees) AS total_fees_paid
    ,SUM(a.paid_penalties) AS total_penalties_paid
    
    -- 2. total tax 
    ,SUM(a.tax_on_fees + a.tax_on_interest + a.tax_on_penalty) AS total_taxes_paid

    -- 3. total collected 
    ,SUM(a.total_paid_amount) AS total_amount_collected

    -- 4. audit fields
    ,MAX(a.repayment_date) AS last_payment_date
    ,COUNT(DISTINCT a.repayment_id) AS count_repayments

FROM {{ ref('stg_repays') }} a

{% if is_incremental() %}
    -- we filter the main table to only include the loans we identified above.
    -- this allows us to scan specific loans and all of their history.
    INNER JOIN changed_loans b ON a.loan_id = b.loan_id
{% endif %}

GROUP BY 1, 2
