Certainly! I’ve reformatted your documentation into a clean, professional Markdown structure. I’ve applied a clear hierarchy, utilized LaTeX for the financial formulas to ensure technical precision, and organized the data into scannable tables.

---

# Technical Documentation: Behavioral Clustering Enhancement for Q1 2025 Portfolio Analysis

## Executive Summary

This document describes the **Cluster-Enhanced Portfolio Analysis** system designed to augment traditional risk-based segmentation for the Kueski Pay Q1 2025 vintages (January–March). The methodology transitions from loan-level aggregation to **customer-level behavioral clustering**, enabling micro-segmentation that identifies profitable niches within high-risk bands and dangerous signals within low-risk bands.

**Primary Innovation**: While traditional risk bands (1–5) rely on static credit scores, this system employs unsupervised machine learning (K-Means) on 8 behavioral dimensions to discover actionable customer segments that transcend risk classifications.

---

## 1. Problem Statement & Limitations of Legacy Approach

### 1.1 Traditional Risk Segmentation Constraints

The legacy report segments by `risk_band` (1–2, 3, 4–4.2, 5), yielding the following aggregation paradox:

| Limitation | Impact on Decision Making |
| --- | --- |
| **Static Scoring** | Risk bands calculated at origination; no behavioral adaptation. |
| **Aggregation Bias** | High Risk (4–4.2) shows -17.3% margin, masking profitable sub-segments. |
| **Binary Charge-off** | Customer marked "good" until charge-off, missing early warning patterns. |
| **CAC Uniformity** | Applies single CAC metric to entire risk band, ignoring LTV variation. |

### 1.2 The "Diamond in the Rough" Problem

Analysis indicates that **~12% of Risk Band 5 (Very High Risk) customers are actually profitable**, while **~18% of Risk Band 2 (Low Risk) customers generate negative margins** due to high CAC and low engagement.

---

## 2. Solution Architecture

### 2.1 Core Philosophy

**Behavioral Clustering Hypothesis**: Payment discipline, relationship depth, and yield realization are stronger predictors of ultimate profitability than origination risk scores alone.

### 2.2 Data Transformation Pipeline

```text
Raw Loan Data (enriched.csv)
    │
    ├──► 1. Metric Construction (Loan Level)
    │      ├── Charge-off flag derivation (from final_status)
    │      ├── Expected vs. Actual yield calculation
    │      ├── Margin waterfall (Financial → Contribution → Net)
    │      └── NPL (Non-Performing Loan) identification
    │
    └──► 2. Customer Aggregation (User Level)
           ├── Sum: funded_amount, revenue, charge_off_amount
           ├── Mean: interest_rate, collection_rates
           ├── Max: risk_band (worst exposure)
           └── Any: is_charged_off (binary customer flag)
    │
    └──► 3. Behavioral Clustering
           ├── StandardScaler normalization
           ├── K-Means (K=4) segmentation
           └── Auto-labeling by profitability/risk profile

```

---

## 3. Technical Methodology

### 3.1 Feature Engineering

**Derived Metrics from Source Columns**:

* **Expected Yield Rate**: 

* **Actual Yield Rate**: 

* **Yield Gap**: 

* **Charge-off Amount**: 

 *(For defaulted loans only)*

**Customer Lifetime Calculations**:

* **LTV/CAC Ratio**: 

* **Customer Tenure**: Days between first and last loan date.

### 3.2 Clustering Algorithm Configuration

* **Model**: K-Means Clustering
* **Normalization**: `StandardScaler` (z-score normalization)
* **Optimal K**: Determined via Elbow Method (inertia plot), default **K=4**.

**Feature Vector (8 Dimensions)**:

| Feature | Weight | Business Logic |
| --- | --- | --- |
| `loan_count` | 1.0x | Relationship depth indicator |
| `avg_interest_rate` | 1.0x | Price acceptance (inelasticity proxy) |
| `interest_collection_rate` | **1.5x** | **High weight**: Payment discipline |
| `yield_gap` | 1.2x | Performance vs. promise gap |
| `loss_rate` | 1.3x | Historical credit outcome |
| `net_margin_rate` | **1.5x** | **High weight**: Ultimate profitability |
| `total_funded` | 1.0x | Customer value tier |
| `avg_principal_coll_rate` | 1.2x | Principal recovery behavior |

---

## 4. Key Outputs & Deliverables

### 4.1 Enhanced Reporting Layer

* **Cross-Tabulation Matrix**: Identifies "cluster migration" (e.g., which Risk Band 4 customers behave like Risk Band 2).
* **Cluster Performance Profile**: Generates unit economics (LTV/CAC, Margin %) for each segment.
* **Vintage Migration Analysis**: Tracks if newer vintages (Jan/Feb/Mar) are improving in quality.

### 4.2 Visualizations

1. **Risk vs. Return Scatter**: Comparison between traditional Risk Bands and new Clusters.
2. **Cluster Composition Stacked Bar**: Reveals which risk bands contain mixed behaviors.
3. **Financial Contribution Waterfall**: Visual identification of profit/loss drivers.

---

## 5. Business Logic & Strategic Insights

> ### 💡 The "Hidden Profitable" Discovery
> 
> 
> Within Risk Band 4.2 (traditionally **-23.3% margin**), the clustering identifies a **"Struggling_Attempting"** cluster: 14% of these customers actually yield a **+8.2% net margin**.
> * **Action**: Retain with payment plans; do not cut credit lines.
> 
> 

### 5.1 Strategic Actions by Segment

* **Toxic_High_Risk**: Immediate pause on acquisition.
* **Low_Risk_Thin_Margin**: Reduce CAC via organic channels; avoid paid acquisition for this profile.
* **Profitable_Core**: Expand bidding and prioritize for retention campaigns.

---

## 6. Implementation Guide

### 6.1 Pre-requisites

* **Environment**: Google Colab (Python 3.8+)
* **Libraries**: `pandas`, `scikit-learn`, `matplotlib`, `seaborn`
* **Input**: `enriched.csv` with Q1 2025 originations.

### 6.2 Execution Flow

1. **Validation**: Check for required columns (COGS estimated at 4% if absent).
2. **Aggregation**: Group by `user_id` to create customer-level profiles.
3. **Clustering**: Standardize  Fit K-Means  Assign Labels.
4. **Export**: Generate `customer_clusters_q1_2025.csv`.

---

## 7. Expected Outcomes & ROI

| Action | Cluster Target | Expected Impact |
| --- | --- | --- |
| **Pause Acquisition** | Toxic_High_Risk | $440K annual loss avoidance |
| **Increase APR +8pts** | Struggling_Attempting | +$180K yield improvement |
| **Reduce CAC Budget** | Low_Risk_Thin_Margin | $120K marketing efficiency gain |

* **Portfolio Margin**: Improvement from -12.11% to projected **+3% to +5%**.
* **Loss Rate**: Reduction from 18.8% to **<10%** via early intervention.

---

## 8. Limitations & Considerations

1. **Censoring**: Recent March 2025 vintages have shorter observation windows.
2. **Seasonality**: Q1 data may be influenced by post-holiday financial stress.
3. **Stability**: Clusters should be re-validated monthly to ensure the K-Means centroids remain representative of current behavior.

**Document Version**: 1.0

**Owner**: Data Science / Risk Analytics Team

---

**Would you like me to generate a Python template to help you start the K-Means implementation for this data?**
