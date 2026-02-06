{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='loan_id',
        partition_by={
            "field": "disbursed_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["user_id", "vintage_month", "risk_segment"],
        require_partition_filter=false
    )
}}

-- get the latest snapshot for each loan with proper status handling
WITH latest_snapshot AS (
    SELECT
        loan_id
        ,user_id
        ,disbursed_date
        ,funded_amount
        ,requested_amount
        ,interest_rate
        ,loan_term_months
        ,last_delinquency_status
        ,last_positive_capital_balance
        -- aggregate cogs and charge-offs
        ,SUM(cogs_total_cost) AS total_cogs
        ,SUM(COALESCE(charge_off, 0)) AS total_charge_off
    FROM {{ ref('stg_loans') }}
    
    {% if is_incremental() %}
        WHERE disbursed_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
    {% endif %}
    
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- first calculate final status to make it available for downstream logic
loan_status_calc AS (
    SELECT
        *
        -- determine final status
        ,CASE 
            -- charge-off issued and was past due
            WHEN total_charge_off > 0 
                AND last_delinquency_status IN ('Past due (180<)', 'Past due (90-179)', 'Past due (60-89)', 'Past due (30-59)') 
                THEN 'Charge Off'
            -- no charge-off but still past due
            WHEN total_charge_off = 0
                AND last_delinquency_status IN ('Past due (180<)', 'Past due (90-179)', 'Past due (60-89)', 'Past due (30-59)')
                THEN 'Open Default'
            -- otherwise use the last delinquency status
            ELSE last_delinquency_status
        END AS final_status

        -- is charged off flag
        ,CASE WHEN total_charge_off > 0 THEN TRUE ELSE FALSE END AS is_charged_off

        -- vintage month
        ,DATE_TRUNC(disbursed_date, MONTH) AS vintage_month
    FROM latest_snapshot
),

-- now calculate metrics dependent on final status
loan_with_final_status AS (
    SELECT
        *
        -- dpd bucket from final status
        ,CASE 
            WHEN final_status = 'Current' THEN 0
            WHEN final_status = 'Past due (1-29)' THEN 1
            WHEN final_status = 'Past due (30-59)' THEN 30
            WHEN final_status = 'Past due (60-89)' THEN 60
            WHEN final_status = 'Past due (90-179)' THEN 90
            WHEN final_status IN ('Past due (180<)', 'Charge Off', 'Open Default') THEN 180
            WHEN final_status = 'Fully Paid' THEN -1
            WHEN final_status = 'Sold' THEN -2
            ELSE NULL
        END AS dpd_bucket

        -- npl flag (90+ dpd or charge-off)
        ,CASE 
            WHEN final_status IN ('Past due (90-179)', 'Past due (180<)', 'Charge Off', 'Open Default')
                THEN TRUE
            ELSE FALSE
        END AS is_npl
    FROM loan_status_calc
),

-- customer data
customer_data AS (
    SELECT
        user_id
        ,city
        ,state
        ,acquisition_date
        ,acquisition_channel
        ,risk_band
        ,cac_amount
    FROM {{ ref('stg_customers') }}
),

-- repayment data
repayment_data AS (
    SELECT
        loan_id
        ,total_principal_paid
        ,total_interest_paid
        ,total_fees_paid
        ,total_penalties_paid
        ,total_taxes_paid
        ,total_amount_collected
        ,last_payment_date
        ,count_repayments
    FROM {{ ref('int_repayments_aggregated') }}
),

-- expected schedule data
expected_schedule AS (
    SELECT
        loan_id
        ,SUM(expected_principal) AS expected_principal_total
        ,SUM(expected_interest) AS expected_interest_total
        ,SUM(expected_total_payment) AS expected_total_payment
        ,COUNT(*) AS num_installments
    FROM {{ ref('int_loan_schedules') }}
    GROUP BY 1
)

SELECT
    -- primary key
    a.loan_id

    -- foreign keys
    ,a.user_id

    -- temporal
    ,a.disbursed_date
    ,a.vintage_month

    -- loan characteristics
    ,a.funded_amount
    ,a.requested_amount
    ,a.interest_rate
    ,a.loan_term_months

    -- customer data
    ,b.city
    ,b.state
    ,b.acquisition_date
    ,b.acquisition_channel
    ,b.risk_band
    ,{{ risk_segment('risk_band') }} AS risk_segment
    ,b.cac_amount

    -- expected from schedule
    ,COALESCE(d.expected_principal_total, 0) AS expected_principal
    ,COALESCE(d.expected_interest_total, 0) AS expected_interest
    ,COALESCE(d.expected_total_payment, 0) AS expected_total_payment

    -- actual payments
    ,COALESCE(c.total_principal_paid, 0) AS paid_principal
    ,COALESCE(c.total_interest_paid, 0) AS paid_interest
    ,COALESCE(c.total_fees_paid, 0) AS paid_fees
    ,COALESCE(c.total_penalties_paid, 0) AS paid_penalties
    ,COALESCE(c.total_taxes_paid, 0) AS paid_taxes
    ,COALESCE(c.total_amount_collected, 0) AS total_collected
    ,c.last_payment_date
    ,COALESCE(c.count_repayments, 0) AS count_repayments

    -- revenue calculation
    ,COALESCE(c.total_interest_paid, 0) + COALESCE(c.total_fees_paid, 0) + COALESCE(c.total_penalties_paid, 0) AS total_revenue

    -- collection rates
    ,SAFE_DIVIDE(COALESCE(c.total_principal_paid, 0), d.expected_principal_total) * 100 AS principal_collection_rate
    ,SAFE_DIVIDE(COALESCE(c.total_interest_paid, 0), d.expected_interest_total) * 100 AS interest_collection_rate

    -- yield metrics
    ,SAFE_DIVIDE(d.expected_interest_total, a.funded_amount) * 100 AS expected_yield_rate
    ,SAFE_DIVIDE(COALESCE(c.total_interest_paid, 0), a.funded_amount) * 100 AS actual_yield_rate

    -- costs
    ,a.total_cogs
    ,a.total_charge_off

    -- final status
    ,a.last_delinquency_status
    ,a.final_status
    ,a.dpd_bucket
    ,a.is_npl
    ,a.is_charged_off
    ,a.last_positive_capital_balance

    -- financial margins
    ,COALESCE(c.total_interest_paid, 0) + COALESCE(c.total_fees_paid, 0) + COALESCE(c.total_penalties_paid, 0) - a.total_charge_off AS financial_margin
    ,COALESCE(c.total_interest_paid, 0) + COALESCE(c.total_fees_paid, 0) + COALESCE(c.total_penalties_paid, 0) - a.total_charge_off - a.total_cogs AS contribution_margin
    ,COALESCE(c.total_interest_paid, 0) + COALESCE(c.total_fees_paid, 0) + COALESCE(c.total_penalties_paid, 0) - a.total_charge_off - a.total_cogs - b.cac_amount AS net_margin

FROM loan_with_final_status a
LEFT JOIN customer_data b ON a.user_id = b.user_id
LEFT JOIN repayment_data c ON a.loan_id = c.loan_id
LEFT JOIN expected_schedule d ON a.loan_id = d.loan_id

{% if is_incremental() %}
WHERE a.disbursed_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
{% endif %}
