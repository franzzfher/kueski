{{
    config(
        materialized='table',
        partition_by={
            "field": "vintage_month",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["vintage_month"]
    )
}}

SELECT
    vintage_month

    -- counts
    ,COUNT(DISTINCT loan_id) AS loan_count
    ,COUNT(DISTINCT user_id) AS customer_count

    -- funded amounts
    ,SUM(funded_amount) AS total_funded
    ,SUM(requested_amount) AS total_requested
    ,AVG(funded_amount) AS avg_loan_amount
    ,AVG(interest_rate) AS avg_interest_rate

    -- expected from schedules
    ,SUM(expected_interest) AS total_expected_interest
    ,SUM(expected_principal) AS total_expected_principal

    -- actual collections
    ,SUM(paid_interest) AS total_interest_paid
    ,SUM(paid_principal) AS total_principal_paid
    ,SUM(paid_fees) AS total_fees_paid
    ,SUM(paid_penalties) AS total_penalties_paid
    ,SUM(total_collected) AS total_collected

    -- revenue
    ,SUM(total_revenue) AS total_revenue

    -- collection rates
    ,SAFE_DIVIDE(SUM(paid_interest), SUM(expected_interest)) * 100 AS interest_collection_rate
    ,SAFE_DIVIDE(SUM(paid_principal), SUM(expected_principal)) * 100 AS principal_collection_rate

    -- yield rates
    ,SAFE_DIVIDE(SUM(expected_interest), SUM(funded_amount)) * 100 AS expected_yield_rate
    ,SAFE_DIVIDE(SUM(paid_interest), SUM(funded_amount)) * 100 AS actual_yield_rate

    -- costs
    ,SUM(total_cogs) AS total_cogs
    ,SUM(total_charge_off) AS total_charge_off
    ,SAFE_DIVIDE(SUM(total_charge_off), SUM(funded_amount)) * 100 AS loss_rate

    -- cac
    ,SUM(cac_amount) AS total_cac
    ,AVG(cac_amount) AS avg_cac

    -- margins
    ,SUM(financial_margin) AS total_financial_margin
    ,SUM(contribution_margin) AS total_contribution_margin
    ,SUM(net_margin) AS total_net_margin
    ,SAFE_DIVIDE(SUM(net_margin), SUM(funded_amount)) * 100 AS net_margin_rate

    -- status breakdown
    ,SUM(CAST(is_npl AS INT64)) AS npl_count
    ,SUM(CAST(is_charged_off AS INT64)) AS charged_off_count
    ,SUM(CASE WHEN final_status = 'Fully Paid' THEN 1 ELSE 0 END) AS fully_paid_count
    ,SUM(CASE WHEN final_status = 'Open Default' THEN 1 ELSE 0 END) AS open_default_count
    ,SUM(CASE WHEN final_status = 'Current' THEN 1 ELSE 0 END) AS current_count

FROM {{ ref('int_loans_enriched') }}
GROUP BY 1
