{{
    config(
        materialized='table',
        cluster_by=['vintage_month', 'risk_segment'],
        partition_by={
            "field": "vintage_month",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}


SELECT 
    loan_id
    ,user_id
    ,disbursed_date
    ,vintage_month
    ,risk_segment
    
    -- loan
    ,requested_amount
    ,funded_amount
    ,interest_rate
    ,loan_status
    
    -- repayment
    ,total_principal_paid
    ,total_interest_paid
    ,total_fees_paid
    ,total_penalties_paid
    ,total_amount_collected
    
    -- revenue and margins
    ,total_revenue
    ,total_cogs
    ,total_charge_off
    ,financial_margin
    ,contribution_margin
    
    -- ratios
    ,roi
    ,loss_rate
    ,yield_rate
    ,principal_recovery_rate
    
    -- customer
    ,acquisition_date
    ,acquisition_channel
    ,cac_amount
    ,city
    ,state
    
    -- audit
    ,last_payment_date
    ,count_repayments

FROM {{ ref('int_loans_with_repayments') }}

WHERE vintage_month IN ('2025-01-01', '2025-02-01', '2025-03-01')
