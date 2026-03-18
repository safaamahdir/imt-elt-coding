## Step 1 — Transform: Bronze → Silver
In this step we implemented a cleaning pipeline to convert raw Bronze data into a conformed Silver layer by removing internal metadata and PII. We applied schema-specific validations, including data type normalization, NULL handling, and business logic enforcement for products, users, and orders.

In order to verify this step we applied the following queries :

```sql
-- Bronze products: ~21 columns
SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = 'bronze_group3' AND table_name = 'products';
```
<img src=".\images\query_2.jpeg" alt="The result of the second query" width="50%" />

```sql
-- Silver dim_products: fewer columns (no _* prefix)
SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = 'silver_group3' AND table_name = 'dim_products';
```
<img src=".\images\query_2.jpeg" alt="The result of the second query" width="50%" />

We can see that there are only 17 columns in the silver layer vs 21 in the bronze one.