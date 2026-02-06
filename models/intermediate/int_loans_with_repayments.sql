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


SELECT 
    a.loan_id
    ,a.user_id
    ,a.disbursed_date
    ,a.vintage_month
    ,a.risk_segment
    
    -- loan
    ,a.requested_amount
    ,a.funded_amount
    ,a.interest_rate
    ,a.loan_status
    
    -- repayment
    ,COALESCE(b.total_principal_paid, 0) AS total_principal_paid
    ,COALESCE(b.total_interest_paid, 0) AS total_interest_paid
    ,COALESCE(b.total_fees_paid, 0) AS total_fees_paid
    ,COALESCE(b.total_penalties_paid, 0) AS total_penalties_paid
    ,COALESCE(b.total_taxes_paid, 0) AS total_taxes_paid
    ,COALESCE(b.total_amount_collected, 0) AS total_amount_collected
    
    -- rev
    ,COALESCE(b.total_interest_paid, 0) 
        + COALESCE(b.total_fees_paid, 0) 
        + COALESCE(b.total_penalties_paid, 0) AS total_revenue
    
    -- costs
    ,a.total_cogs
    ,a.total_charge_off
    
    -- margins
    ,COALESCE(b.total_interest_paid, 0) 
        + COALESCE(b.total_fees_paid, 0) 
        + COALESCE(b.total_penalties_paid, 0) 
        - a.total_charge_off AS financial_margin
        
    ,COALESCE(b.total_interest_paid, 0) 
        + COALESCE(b.total_fees_paid, 0) 
        + COALESCE(b.total_penalties_paid, 0) 
        - a.total_charge_off 
        - a.total_cogs AS contribution_margin
    
    -- performance ratios
    ,SAFE_DIVIDE(
        COALESCE(b.total_interest_paid, 0) 
            + COALESCE(b.total_fees_paid, 0) 
            + COALESCE(b.total_penalties_paid, 0) 
            - a.total_charge_off 
            - a.total_cogs,
        a.funded_amount
    ) AS roi
    
    ,SAFE_DIVIDE(a.total_charge_off, a.funded_amount) AS loss_rate
    
    ,SAFE_DIVIDE(
        COALESCE(b.total_interest_paid, 0) 
            + COALESCE(b.total_fees_paid, 0) 
            + COALESCE(b.total_penalties_paid, 0),
        a.funded_amount
    ) AS yield_rate
    
    ,SAFE_DIVIDE(COALESCE(b.total_principal_paid, 0), a.funded_amount) AS principal_recovery_rate
    
    -- customer
    ,a.acquisition_date
    ,a.acquisition_channel
    ,a.cac_amount
    ,a.city
    ,a.state
    
    -- audit fields
    ,b.last_payment_date
    ,COALESCE(b.count_repayments, 0) AS count_repayments

FROM {{ ref('int_loans_enriched') }} a
LEFT JOIN {{ ref('int_repayments_aggregated') }} b
    ON a.loan_id = b.loan_id

{% if is_incremental() %}
WHERE a.disbursed_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -15 DAY)
{% endif %}
