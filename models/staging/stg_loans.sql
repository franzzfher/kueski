{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key = '_unique_id',
        partition_by={
            "field": "disbursed_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["user_id", "loan_id", "delinquency_status"],
        require_partition_filter = false
    )
}}


WITH loans AS (    
      SELECT     
          --primary keys
          SAFE_CAST(loan_id AS STRING) AS loan_id
          ,SAFE_CAST(user_id AS STRING) AS user_id
          
          ,DATE(disbursed_date) AS disbursed_date
          ,DATE(limit_month) AS limit_month
      
          --status
          ,TRIM(delinquency_status) AS delinquency_status
      
          --finance
          ,SAFE_CAST(requested_amount AS FLOAT64) AS requested_amount
          ,SAFE_CAST(founded_amount AS FLOAT64) AS funded_amount
          ,SAFE_CAST(capital_balance AS FLOAT64) AS capital_balance
          ,SAFE_CAST(interest_rate AS FLOAT64) AS interest_rate
      
          --risk
          ,COALESCE(SAFE_CAST(charge_off AS FLOAT64), 0) AS charge_off
          ,COALESCE(SAFE_CAST(cogs_total_cost AS FLOAT64), 0) AS cogs_total_cost
      
          ,original_schedule --object
          ,SAFE_CAST(term AS INT64) AS loan_term_months
          
      --FROM random-480703.test.ae_challenge_loans
      FROM {{ source('test', 'ae_challenge_loans') }}
    
    {% if is_incremental() %}
        WHERE DATE(limit_month) >= DATE_ADD(CURRENT_DATE(), INTERVAL -15 DAY)
    {% endif %}

)


SELECT * EXCEPT(funded_amount)
    
    ,FIRST_VALUE(funded_amount IGNORE NULLS) OVER (PARTITION BY loan_id, user_id ORDER BY limit_month 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS funded_amount
    ,LAST_VALUE(IF(capital_balance > 0, capital_balance, null) IGNORE NULLS) OVER (PARTITION BY loan_id, user_id ORDER BY limit_month 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_positive_capital_balance
    ,LAST_VALUE(delinquency_status IGNORE NULLS) OVER (PARTITION BY loan_id, user_id ORDER BY limit_month 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_delinquency_status
    ,ROW_NUMBER() OVER(PARTITION BY loan_id, user_id ORDER BY limit_month DESC) AS is_latest_snapshot
    ,ROW_NUMBER() OVER(PARTITION BY loan_id, user_id ORDER BY limit_month ASC) AS snapshot_number

   ,TO_HEX(MD5(CONCAT(
      IFNULL(SAFE_CAST(loan_id AS STRING), ''), 
      IFNULL(SAFE_CAST(user_id AS STRING), ''), 
      IFNULL(SAFE_CAST(limit_month AS STRING), '')
    ))) AS _unique_id

FROM loans
