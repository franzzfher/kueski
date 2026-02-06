{{% macro par_bucket(dpd_bucket_column) %}}

CASE 
    WHEN {{ dpd_bucket_column }} < 0 THEN 'Performing'
    WHEN {{ dpd_bucket_column }} = 0 THEN 'PAR 0'
    WHEN {{ dpd_bucket_column }} <= 30 THEN 'PAR 1-30'
    WHEN {{ dpd_bucket_column }} <= 60 THEN 'PAR 31-60'
    WHEN {{ dpd_bucket_column }} <= 90 THEN 'PAR 61-90'
    WHEN {{ dpd_bucket_column }} > 90 THEN 'PAR 90+'
    ELSE 'Unknown'
END

{{% endmacro %}}
