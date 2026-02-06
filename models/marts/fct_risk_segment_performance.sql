{{
    config(
        materialized='view'
    )
}}

SELECT
    risk_segment
    ,risk_band
    ,loan_count
    ,customer_count
    ,total_funded
    ,avg_loan_amount
    ,avg_interest_rate
    ,total_expected_interest
    ,expected_yield_rate
    ,total_interest_paid
    ,actual_yield_rate
    ,interest_collection_rate
    ,yield_gap
    ,total_charge_off
    ,loss_rate
    ,total_cogs
    ,total_cac
    ,avg_cac
    ,total_revenue
    ,revenue_rate
    ,total_financial_margin
    ,total_contribution_margin
    ,total_net_margin
    ,net_margin_rate
    -- status breakdown
    ,npl_count
    ,npl_rate_count
    ,charged_off_count
    ,charged_off_rate

FROM {{ ref('int_risk_segment_performance') }}
