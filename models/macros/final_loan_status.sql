{{% macro final_loan_status(charge_off_column, last_delinquency_column) %}}

CASE 
    -- Charge-off issued and was past due
    WHEN {{ charge_off_column }} > 0 
        AND {{ last_delinquency_column }} IN ('Past due (180<)', 'Past due (90-179)', 'Past due (60-89)', 'Past due (30-59)') 
        THEN 'Charge Off'
    -- No charge-off but still past due (open default)
    WHEN {{ charge_off_column }} = 0
        AND {{ last_delinquency_column }} IN ('Past due (180<)', 'Past due (90-179)', 'Past due (60-89)', 'Past due (30-59)')
        THEN 'Open Default'
    -- Otherwise use the last delinquency status
    ELSE {{ last_delinquency_column }}
END

{{% endmacro %}}
