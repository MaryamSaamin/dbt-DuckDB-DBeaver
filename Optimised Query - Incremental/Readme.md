# Optimized User Activity Query

This SQL query builds a **user activity table** capturing:

- First and last songs played per user  
- Number of guitar challenges discovered  
- First song played after a challenge  

The query was optimized for **incremental updates**, meaning it only processes **new users** since the last run, making it efficient for large datasets.

---

## **Data Flow**

### **Preferred Flow (Single Query / Incremental)**

stg_login -> stg_song -> stg_challenge -> dim_activity (incremental) -> user_engagement (incremental)


**Explanation:**

- `stg_*` tables: minimal cleaning of raw data (login, songs played, challenges).  
- `dim_activity`: calculates first/last song, challenges, and first song after challenge.  
- `user_engagement`: downstream aggregated tables that use `dim_activity`, incremental for efficiency.  

---

## **Problems of the First `dim_activity.sql`**

- Multiple nested queries and joins made the query run slowly on large datasets.

- The original query processed all users every time, instead of only new users.

- Joining all songs with all challenges was heavy and hard to maintain. Killer

- Some computations, like first/last song, were done multiple times.

- not using ref as DBT model query

---

## **Incremental Logic in Optimized Query**

The optimized query includes **incremental filtering** to process only new users:

```sql
-- This part was added to the optimized query to handle incremental linking correctly
{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

{% if is_incremental() %}
  where l.login_at > (select max(first_login) from {{ this }})
{% endif %}

## **Incremental Logic**

The query uses an **incremental filter** to ensure only new users are processed.


### **Option 2 Alternative View**

stg_login -> stg_song -> stg_challenge -> int_user_activity (incremental) -> dim_activity (incremental) -> user_engagement (incremental)

-- In this option should be written 2 queries for int_user_act and dim_activity , I thought you need just one optimised query.


