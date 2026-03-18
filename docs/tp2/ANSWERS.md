## Step 1 — Transform: Bronze → Silver
In this step we implemented a cleaning pipeline to convert raw Bronze data into a conformed Silver layer by removing internal metadata and PII. We applied schema-specific validations, including data type normalization, NULL handling, and business logic enforcement for products, users, and orders.

In order to verify this step we applied the following queries :

```sql
-- Bronze products: ~21 columns
SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = 'bronze_group3' AND table_name = 'products';
```
<img src=".\images\query_1.jpeg" alt="The result of the first query" width="50%" />

```sql
-- Silver dim_products: fewer columns (no _* prefix)
SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = 'silver_group3' AND table_name = 'dim_products';
```
<img src=".\images\query_2.jpeg" alt="The result of the second query" width="50%" />

We can see that there are only 17 columns in the silver layer vs 21 in the bronze one.

## Step 2 - Gold Layer: Analytics Aggregations

In this step, we configure the gold layer where we prepare the **business-ready** tables that will give direct answers to the teams.  

In order to verify this step, we run the following queries :  

```sql
-- Daily revenue
SELECT * FROM gold_grou3.daily_revenue ORDER BY order_date LIMIT 5;
```
<img src=".\images\query_3.png" alt="The result of the third query" width="50%" />

```sql
-- Top 5 products by revenue
SELECT product_name, brand, total_revenue
FROM gold_group3.product_performance
ORDER BY total_revenue DESC
LIMIT 5;
```
<img src=".\images\query_4.png" alt="The result of the forth query" width="50%" />

```sql
-- Top 5 customers by spending
SELECT first_name, last_name, loyalty_tier, total_spent, total_orders
FROM gold_group3.customer_ltv
ORDER BY total_spent DESC
LIMIT 5;
```
<img src=".\images\query_5.png" alt="The result of the fifth query" width="50%" />

## Step 3 — Run the Full Pipeline 🚀

In this step, we run the complete pipeline. To verify that everything works, we run the following query that shows the number of tables at the different layers:  
```sql
-- Number of tables per schema
SELECT schemaname, COUNT(*) AS nb_tables
FROM pg_tables
WHERE schemaname IN ('bronze_group3', 'silver_group3', 'gold_group3')
GROUP BY schemaname
ORDER BY schemaname;
```
The results are as expected :  
<img src=".\images\query_6.png" alt="The result of the sixth query" width="50%" />





