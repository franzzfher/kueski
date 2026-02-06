{{
    config(
        materialized='view'
    )
}}

SELECT
    vintage_month
    ,loan_count
    ,customer_count
    ,total_funded
    ,total_requested
    ,avg_loan_amount
    ,avg_interest_rate
    ,total_expected_interest
    ,total_expected_principal
    ,total_interest_paid
    ,total_principal_paid
    ,total_fees_paid
    ,total_penalties_paid
    ,total_collected
    ,total_revenue
    ,interest_collection_rate
    ,principal_collection_rate
    ,expected_yield_rate
    ,actual_yield_rate
    ,total_cogs
    ,total_charge_off
    ,loss_rate
    ,total_cac
    ,avg_cac
    ,total_financial_margin
    ,total_contribution_margin
    ,total_net_margin
    ,net_margin_rate
    -- status breakdown
    ,npl_count
    ,charged_off_count
    ,fully_paid_count
    ,open_default_count
    ,current_count

FROM {{ ref('int_vintage_performance') }}
