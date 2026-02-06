{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key = 'user_id',
        cluster_by = ["user_id", "state", "risk_band"],
        require_partition_filter = false
    )
}}

  
  SELECT
    SAFE_CAST(user_id AS STRING) AS user_id
    
    -- demographics
    ,TRIM(city) AS city
    ,TRIM(state) AS state
    
    -- acquisition 
    ,SAFE_CAST(acquisition_date AS DATE) AS acquisition_date
    ,TRIM(channel) AS acquisition_channel
    
    -- risk profile
    ,SAFE_CAST(risk_band_production AS STRING) AS risk_band

    -- economics
    -- note: if 0 organic or unattritubed. 
    ,COALESCE(SAFE_CAST(acquisition_cost AS FLOAT64), 0) AS cac_amount

  --FROM random-480703.test.ae_challenge_customer
  FROM {{ source('test', 'ae_challenge_customer') }}

{% if is_incremental() %}
    WHERE DATE(acquisition_date) >= DATE_ADD(CURRENT_DATE(), INTERVAL -15 DAY)
{% endif %}
