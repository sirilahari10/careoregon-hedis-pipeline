# Healthcare Quality Analytics: HEDIS Prenatal Care Pipeline

For Medicaid and CCO payers, tracking clinical quality measures accurately is critical for regulatory compliance and population health improvement.

This repository demonstrates a foundational PySpark/SQL pipeline designed for a Databricks environment. It simulates the extraction and calculation of the **HEDIS Timeliness of Prenatal Care** metric from raw claims and administrative data.

## Pipeline Architecture
1. **Data Ingestion:** Simulates loading member enrollment and maternal claims data.
2. **Quality Metric Logic:** Uses PySpark DataFrames and SQL to identify live births, calculate the gestational timeline, and flag whether the first prenatal visit occurred within the required first trimester (or within 42 days of enrollment).
3. **Regulatory Aggregation:** Aggregates the compliance rate for strategic quality improvement reporting.


Assumption: The 42-day enrollment grace period is applied strictly, but in a real CareOregon environment, I would cross-reference this with retro-eligibility flags.

Limitation: This PoW uses a simplified 175-day threshold for the first trimester. A true HEDIS pipeline would require parsing specific CPT/HCPCS modifiers for prenatal visits.

Tech Choice: Used PySpark over Pandas to demonstrate readiness for the scale of CCO/Medicaid data within a Databricks environment.
