{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key = 'repayment_id',
        partition_by={
            "field": "repayment_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ["user_id", "loan_id"],
        require_partition_filter = false
    )
}}

  
  SELECT
      -- primary key
      SAFE_CAST(repayment_transaction_id AS STRING) AS repayment_id
      
      -- foreign key
      ,SAFE_CAST(loan_id AS STRING) AS loan_id
      ,SAFE_CAST(user_id AS STRING) AS user_id

      -- temporal data
      ,CAST(event_date AS TIMESTAMP) AS repayment_timestamp
      ,DATE(event_date) AS repayment_date

      -- financials
      ,SAFE_CAST(amount_trans AS FLOAT64) AS total_paid_amount
      
      ,SAFE_CAST(principalamount_trans AS FLOAT64) AS paid_principal
      ,SAFE_CAST(interestamount_trans AS FLOAT64) AS paid_interest
      ,SAFE_CAST(feesamount_trans AS FLOAT64) AS paid_fees
      ,SAFE_CAST(penaltyamount_trans AS FLOAT64) AS paid_penalties --0

      -- tax
      ,SAFE_CAST(taxonfeesamount_trans AS FLOAT64) AS tax_on_fees
      ,SAFE_CAST(taxoninterestamount_trans AS FLOAT64) AS tax_on_interest
      ,SAFE_CAST(taxonpenaltyamount_trans AS FLOAT64) AS tax_on_penalty --0

  --FROM random-480703.test.ae_challenge_repayments
  FROM {{ source('test', 'ae_challenge_repayments') }}

{% if is_incremental() %}
    WHERE DATE(event_date) >= DATE_ADD(CURRENT_DATE(), INTERVAL -15 DAY)
{% endif %}
