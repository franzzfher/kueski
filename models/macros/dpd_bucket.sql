{{% macro dpd_bucket(delinquency_status_column) %}}

CASE 
    WHEN {{ delinquency_status_column }} = 'Current' THEN 0
    WHEN {{ delinquency_status_column }} = 'Past due (1-29)' THEN 1
    WHEN {{ delinquency_status_column }} = 'Past due (30-59)' THEN 30
    WHEN {{ delinquency_status_column }} = 'Past due (60-89)' THEN 60
    WHEN {{ delinquency_status_column }} = 'Past due (90-179)' THEN 90
    WHEN {{ delinquency_status_column }} = 'Past due (180<)' THEN 180
    WHEN {{ delinquency_status_column }} = 'Fully Paid' THEN -1
    WHEN {{ delinquency_status_column }} = 'Sold' THEN -2
    ELSE NULL
END

{{% endmacro %}}
