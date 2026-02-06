{{% macro risk_segment(risk_band_column) %}}

CASE 
    WHEN SAFE_CAST({{ risk_band_column }} AS FLOAT64) <= 2 THEN 'Low Risk (1-2)'
    WHEN SAFE_CAST({{ risk_band_column }} AS FLOAT64) <= 3 THEN 'Medium Risk (3)'
    WHEN SAFE_CAST({{ risk_band_column }} AS FLOAT64) <= 4.2 THEN 'High Risk (4-4.2)'
    WHEN SAFE_CAST({{ risk_band_column }} AS FLOAT64) > 4.2 THEN 'Very High Risk (5)'
    ELSE 'Unknown'
END

{{% endmacro %}}
