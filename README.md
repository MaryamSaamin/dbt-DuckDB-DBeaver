# Marketing Ad Reporting – DBT Project

## Overview
This project runs a marketing data pipeline using **DBT**, **DuckDB**, and **DBeaver**.
The goal is to create a **final fact table** for the Marketing Analyst and team.

Final table:
**mart_fact_network_report**

Database file:
**marketingdb.duckdb**

---

## Tools & Platform
- DBT
- DuckDB
- DBeaver
- Conda environment

---

## Project Setup

Profiles and configuration files created:
- profiles.yml
- dbt_project.yml
- schema.yml

Activate environment:
- conda env list  
- conda create -n Ovecell_env python=3.10
- conda activate `Ovecell_env`

Check DBT:
- dbt --version  
- If missing: `pip install dbt-duckdb`

Initialize project:
- dbt init MarketingAD

---

## Data Loading Strategy
Although `dbt seed` can create tables automatically,  
I preferred to **write SQL queries** and place them in:

- `models/raw`

This allows:
- Full control on table creation
- Column renaming
- Data type casting

---

## Step 1: Raw Layer

Create raw tables and rename columns:
- `dbt run --select raw`

Check record counts:
- `dbt run --select helper_raw_table_counts`

---

## Step 2: Staging Layer

### GEO Dictionary
Run:
- `dbt run --select stg_geo_dictionary`

Logic:
- Location names are classified into:
  - Digits only (postal codes)
  - Letters only (names)
  - Mixed values
- `COUNTRY_CODE_ID` is created for each country.

---

### Ad Network 2
Run:
- `dbt run --select stg_ae_ad_network_2_report`
- `dbt run --select stg_ae_ad_network_2_unmapped`

Logic:
- Country codes are extracted from campaign names.
- Country codes are mapped to the GEO dictionary.
- Missing countries (e.g. UK) are added.

---

### Ad Network 1
Run:
- `dbt run --select stg_ae_ad_network_1_country_report`
- `dbt run --select stg_ae_ad_network_1_detailed_report`

Validation:
- `dbt run --select audit_campaign_country_vs_detailed`
- `dbt run --select stg_ae_ad_network_1_country_report_match`

Logic:
- Country codes and IDs are mapped using GEO postal codes(STATE_ID).
- Campaign names are added using campaign update history.
- Three AD Network 1 tables are checked.
- `network_1_country_report` is chosen because it contains all records.

---

## Mart Layer

Check record counts in AD Network1 and AD Network 2:
- `dbt run --select mart_fact_network_1_count`
- `dbt run --select mart_fact_network_2_count`

Validate data and column filling specially country_code and country_code_id, please consider for several state_id , there was one country_code and country_code_id and should be noticed in filling.

Create final fact table:
- `dbt run --select mart_fact_network_report`

Final count check:
- `dbt run --select mart_fact_network_count`

---

## Final Output
- Database: `marketingdb.duckdb`
- Final table: **mart_fact_network_report**
- This table is used by the Marketing Analyst and team. 

---

## Full Rebuild Test
To verify the pipeline from scratch:

- Delete `marketingdb.duckdb`
- Run:
  - dbt clean
  - dbt deps
  - dbt run
  - dbt test

Result:
- Database recreated successfully
- Final table available as expected
- I just removed the records when country_code and country_code_id were null. I kept records when compaign_name is null because there is always campaign_id.
- This design supports future dimensional modeling
  GEO can be separated into a dimension table  and creating surrogate key if needed , if Marketing team wants sometimes the name of location althugh just few records has  filled.

---

## Documentation
Generate DBT documentation:
- `dbt docs generate`
- `dbt docs serve`

- I attached some pictures from DBeaver(presenting fact table and Geo table) and from Conda prompt and how to run and test the DBT project.

## Zipping the Project (Important Note)

When zipping this DBT project on Windows, I encountered **“Access is denied”** errors related to the following path:

`dbt_packages/dbt_utils/...`

This was expected behavior.

### Reason
- `dbt_packages/` contains DBT dependencies downloaded automatically.
- It includes integration tests, macros, and sample data.
- Some files have restricted permissions or very deep paths that Windows cannot read when zipping.

### Recommended Solution 
**I did not include `dbt_packages/` in the ZIP file.**

This folder is **auto-generated** and can always be recreated by running:
```bash
dbt deps
