{{% macro is_npl(delinquency_status_column) %}}

CASE 
    WHEN {{ delinquency_status_column }} IN ('Past due (90-179)', 'Past due (180<)') THEN TRUE
    ELSE FALSE
END

{{% endmacro %}}
