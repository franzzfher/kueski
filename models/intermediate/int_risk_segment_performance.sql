{{
    config(
        materialized='table'
    )
}}

SELECT
    risk_segment
    ,risk_band

    -- Counts
    ,COUNT(DISTINCT loan_id) AS loan_count
    ,COUNT(DISTINCT user_id) AS customer_count

    -- Funded amounts
    ,SUM(funded_amount) AS total_funded
    ,AVG(funded_amount) AS avg_loan_amount
    ,AVG(interest_rate) AS avg_interest_rate

    -- Expected
    ,SUM(expected_interest) AS total_expected_interest
    ,SAFE_DIVIDE(SUM(expected_interest), SUM(funded_amount)) * 100 AS expected_yield_rate

    -- Actual
    ,SUM(paid_interest) AS total_interest_paid
    ,SAFE_DIVIDE(SUM(paid_interest), SUM(funded_amount)) * 100 AS actual_yield_rate

    -- Collection rate
    ,SAFE_DIVIDE(SUM(paid_interest), SUM(expected_interest)) * 100 AS interest_collection_rate

    -- Yield gap
    ,SAFE_DIVIDE(SUM(expected_interest), SUM(funded_amount)) * 100 - SAFE_DIVIDE(SUM(paid_interest), SUM(funded_amount)) * 100 AS yield_gap

    -- Costs
    ,SUM(total_charge_off) AS total_charge_off
    ,SAFE_DIVIDE(SUM(total_charge_off), SUM(funded_amount)) * 100 AS loss_rate
    ,SUM(total_cogs) AS total_cogs

    -- CAC
    ,SUM(cac_amount) AS total_cac
    ,AVG(cac_amount) AS avg_cac

    -- Revenue
    ,SUM(total_revenue) AS total_revenue
    ,SAFE_DIVIDE(SUM(total_revenue), SUM(funded_amount)) * 100 AS revenue_rate

    -- Margins
    ,SUM(financial_margin) AS total_financial_margin
    ,SUM(contribution_margin) AS total_contribution_margin
    ,SUM(net_margin) AS total_net_margin
    ,SAFE_DIVIDE(SUM(net_margin), SUM(funded_amount)) * 100 AS net_margin_rate

    -- Status breakdown
    ,SUM(CAST(is_npl AS INT64)) AS npl_count
    ,SAFE_DIVIDE(SUM(CAST(is_npl AS INT64)), COUNT(*)) * 100 AS npl_rate_count
    ,SUM(CAST(is_charged_off AS INT64)) AS charged_off_count
    ,SAFE_DIVIDE(SUM(CAST(is_charged_off AS INT64)), COUNT(*)) * 100 AS charged_off_rate


FROM {{ ref('int_loans_enriched') }}
GROUP BY 1, 2
ORDER BY 
    CASE risk_segment
        WHEN 'Low Risk (1-2)' THEN 1
        WHEN 'Medium Risk (3)' THEN 2
        WHEN 'High Risk (4-4.2)' THEN 3
        WHEN 'Very High Risk (5)' THEN 4
        ELSE 5
    END
