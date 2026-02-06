{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='loan_id',
        cluster_by=['user_id', 'vintage_month', 'risk_segment'],
        partition_by={
            "field": "disbursed_date",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}


WITH loan_snapshots AS (
    SELECT 
        loan_id
        ,user_id
        ,disbursed_date
        ,limit_month
        ,capital_balance
        ,charge_off
        ,cogs_total_cost
        ,interest_rate
        ,requested_amount
        ,loan_term_months
        ,delinquency_status
        ,funded_amount
        
        -- first non-null founded_amount
        ,FIRST_VALUE(funded_amount IGNORE NULLS) OVER (
            PARTITION BY loan_id 
            ORDER BY limit_month 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS founded_amount_orig
        
        -- last non-null delinquency status
        ,LAST_VALUE(delinquency_status IGNORE NULLS) OVER (
            PARTITION BY loan_id 
            ORDER BY limit_month 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_delinquency_status
        
        -- last positive capital balance
        ,LAST_VALUE(IF(capital_balance > 0, capital_balance, NULL) IGNORE NULLS) OVER (
            PARTITION BY loan_id 
            ORDER BY limit_month 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_capital_balance
        
        -- row numbers for filtering
        ,ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY limit_month DESC) AS rn_last
        ,ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY limit_month ASC) AS rn_first
        
    FROM {{ ref('stg_loans') }}
)

, loan_aggregated AS (
    SELECT 
        loan_id
        ,user_id
        ,disbursed_date
        ,interest_rate
        ,requested_amount
        ,founded_amount_orig AS funded_amount
        ,last_delinquency_status
        ,last_capital_balance
        
        -- agg costs
        ,SUM(cogs_total_cost) AS total_cogs
        ,SUM(COALESCE(charge_off, 0)) AS total_charge_off
        
        -- date range
        ,MIN(limit_month) AS first_limit_month
        ,MAX(limit_month) AS last_limit_month
        ,COUNT(DISTINCT limit_month) AS snapshot_count
        
    FROM loan_snapshots
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT 
    a.loan_id
    ,a.user_id
    ,a.disbursed_date
    
    -- vintage classification
    ,DATE_TRUNC(a.disbursed_date, MONTH) AS vintage_month
    
    -- attributes
    ,a.requested_amount
    ,a.funded_amount
    ,a.interest_rate
    
    -- status with charge-off logic
    ,CASE 
        WHEN a.total_charge_off > 0 
            AND a.last_delinquency_status IN ('Past due (180<)', 'Past due (90-179)', 'Past due (60-89)') 
            THEN 'Charge Off'
        WHEN a.total_charge_off = 0
            AND a.last_delinquency_status IN ('Past due (180<)', 'Past due (90-179)', 'Past due (60-89)') 
            THEN 'Open Default'
        WHEN a.last_delinquency_status IS NULL 
            THEN 'Current'
        ELSE a.last_delinquency_status 
    END AS loan_status
    
    -- financials
    ,a.total_cogs
    ,a.total_charge_off
    ,a.last_capital_balance
    
    -- customer data
    ,b.acquisition_date
    ,b.acquisition_channel
    ,b.risk_band
    ,b.cac_amount
    ,b.city
    ,b.state
    
    -- risk classification
    ,CASE 
        WHEN b.risk_band IS NULL OR b.risk_band = 'missing_score' THEN 'Unknown'
        WHEN SAFE_CAST(b.risk_band AS FLOAT64) <= 2 THEN 'Low Risk (1-2)'
        WHEN SAFE_CAST(b.risk_band AS FLOAT64) <= 3 THEN 'Medium Risk (3)'
        WHEN SAFE_CAST(b.risk_band AS FLOAT64) <= 4.2 THEN 'High Risk (4-4.2)'
        ELSE 'Very High Risk (5+)'
    END AS risk_segment
    
    -- audit fields
    ,a.first_limit_month
    ,a.last_limit_month
    ,a.snapshot_count

FROM loan_aggregated a
LEFT JOIN {{ ref('stg_customers') }} b
    ON a.user_id = b.user_id

{% if is_incremental() %}
WHERE a.disbursed_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -15 DAY)
{% endif %}

