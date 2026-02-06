{{
    config(
        materialized='view'
    )
}}

SELECT
    -- primary key
    loan_id

    -- foreign key
    ,user_id

    -- temporal
    ,disbursed_date
    ,vintage_month
    ,acquisition_date

    -- loan characteristics
    ,funded_amount
    ,requested_amount
    ,interest_rate
    ,loan_term_months

    -- expected amounts
    ,expected_principal
    ,expected_interest
    ,expected_total_payment

    -- customer info
    ,city
    ,state
    ,acquisition_channel
    ,risk_band
    ,risk_segment
    ,cac_amount

    -- actual collections
    ,paid_principal
    ,paid_interest
    ,paid_fees
    ,paid_penalties
    ,paid_taxes
    ,total_collected
    ,count_repayments
    ,last_payment_date

    -- revenue
    ,total_revenue

    -- collection rates
    ,principal_collection_rate
    ,interest_collection_rate

    -- yield rates
    ,expected_yield_rate
    ,actual_yield_rate

    -- costs
    ,total_cogs
    ,total_charge_off

    -- margins
    ,financial_margin
    ,contribution_margin
    ,net_margin

    -- status
    ,last_delinquency_status
    ,final_status
    ,dpd_bucket
    ,is_npl
    ,is_charged_off
    ,last_positive_capital_balance

FROM {{ ref('int_loans_enriched') }}
