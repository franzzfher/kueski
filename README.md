# KueskiPay Portfolio Analysis - dbt Models

This directory contains the complete dbt model structure for the KueskiPay Portfolio Analysis.

## Model Structure

```
models/
├── sources.yml                    # Source table definitions
├── schema.yml                     # Model documentation and tests
│
├── staging/                       # Staging models (provided by user)
│   ├── stg_loans.sql
│   ├── stg_customers.sql
│   └── stg_repays.sql
│
├── intermediate/                  # Intermediate transformations
│   ├── int_loans_enriched.sql     # Handle snapshot table structure
│   ├── int_loans_with_repayments.sql  # Join loans + repayments
│   ├── int_repayments_aggregated.sql  # (provided by user)
│   └── int_loan_schedules.sql     # (provided by user)
│
└── marts/                         # Final analysis tables
    ├── mart_portfolio_summary.sql     # Loan-level data (Jan-Mar 2025)
    ├── mart_vintage_summary.sql       # Aggregated by vintage
    ├── mart_risk_segment_summary.sql  # Aggregated by risk segment
    ├── mart_ltv_cac_analysis.sql      # Unit economics
    └── mart_delinquency_analysis.sql  # Status distribution
```

## Key Design Decisions

### 1. Handling the Loans Snapshot Table

The `ae_challenge_loans` table is a **monthly snapshot table** where each loan has multiple rows (one per month). The `int_loans_enriched` model handles this using window functions:

```sql
-- Get the first non-null founded_amount (origination amount)
FIRST_VALUE(funded_amount IGNORE NULLS) OVER (
    PARTITION BY loan_id 
    ORDER BY limit_month 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS founded_amount_orig

-- Get the last non-null delinquency status (current status)
LAST_VALUE(delinquency_status IGNORE NULLS) OVER (...) AS last_delinquency_status
```

### 2. Revenue Calculation

Revenue is calculated as:
```sql
total_revenue = paid_interest + paid_fees + paid_penalties
```

### 3. Margin Calculations

```sql
financial_margin = total_revenue - charge_off
contribution_margin = financial_margin - cogs
roi = contribution_margin / funded_amount
```

### 4. Risk Segment Classification

```sql
CASE 
    WHEN risk_band IS NULL OR risk_band = 'missing_score' THEN 'Unknown'
    WHEN risk_band <= 2 THEN 'Low Risk (1-2)'
    WHEN risk_band <= 3 THEN 'Medium Risk (3)'
    WHEN risk_band <= 4.2 THEN 'High Risk (4-4.2)'
    ELSE 'Very High Risk (5+)'
END
```

## Running the Models

```bash
# Run all models
dbt run

# Run specific model
dbt run --select mart_vintage_summary

# Run with full refresh
dbt run --full-refresh

# Test models
dbt test
```

## Key Metrics Available

### Portfolio Level
- Total loans, funded amount, revenue
- Portfolio ROI, loss rate, yield
- LTV, CAC, LTV/CAC ratio

### Vintage Level
- Performance by disbursement month
- Principal recovery rates
- Revenue and margin trends

### Risk Segment Level
- Performance by risk band
- Portfolio distribution
- ROI comparison across segments

### Delinquency
- Status distribution by vintage
- Roll rate analysis
- Charge-off patterns

## Data Quality Notes

1. **Founded Amount**: Only populated in first snapshot per loan
2. **Delinquency Status**: May be NULL for current loans
3. **Charge-offs**: Summed across all snapshots for each loan
4. **COGS**: Summed across all snapshots for each loan
5. **Repayments**: Filtered to only Jan-Mar 2025 vintage loans

## Maintenance

- All models use incremental loading where applicable
- Partitioned by date for query performance
- Clustered by commonly filtered columns
