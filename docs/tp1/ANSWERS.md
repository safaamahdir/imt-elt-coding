## Step 1 — Discover the Data (20 min)

1. `products.csv` has 21 columns. 4 of them start with `_` : `_internal_cost_usd`, `_supplier_id`, `_warehouse_location` and `_internal_cost_code`.

2. `users.csv` has 28 columns. We can not see directly the users' passwords because they are hashed then stocked in the column `_hashed_password`. Concerning the IPs, there is a column named `_last_ip` where we can see the last IP address of the user.

3. In `orders.csv`, the possible values od `status` are : delivered, shipped, returned, processing, cancelled, chargeback.

4. In `order_line_items.csv`, if we compute `unit_price_usd × quantity`, we observe that it is equal to `line_total_usd`. 

5. In `reviews.jsonl`, there are 5 columns that start with `_` : _moderation_score, _sentiment_raw, _toxicity_score, _language_detected, _review_source.  
_moderation_score seems to be a score representing how much the review is likely to violate moderation rules (contains hate speech, harrassment...).  
_sentiment_raw seems to be a score to represent if the review is positive or negative.

6. In the clickstrem Parquet file, the `event_type` column contains the type of the click. In our database, all the raws have the same value _pageview_.  
There are also internal columns such as _ga_client_id, _gtm_container_id, _dom_interactive_ms, _ttfb_ms, _connection_type, _js_heap_size_mb, _consent_string.

## Step 2 — Extract the data 
The data is then extracted from S3 aws and loaded into bronze_group3. 

In order to verify that this step has been done successfully, we applied the following queries :

```sql
-- 1. How many tables in your bronze schema?
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'bronze_group3'
ORDER BY table_name;
```

<img src=".\images\query_1.jpeg" alt="The result of the first query" width="50%" />

As we can see there are 6 tables in total that have been loaded.

```sql
-- Row counts per table
SELECT 'products' AS table_name, COUNT(*) AS rows FROM bronze_group3.products
UNION ALL
SELECT 'users', COUNT(*) FROM bronze_group3.users
UNION ALL
SELECT 'orders', COUNT(*) FROM bronze_group3.orders
UNION ALL
SELECT 'order_line_items', COUNT(*) FROM bronze_group3.order_line_items
UNION ALL
SELECT 'reviews', COUNT(*) FROM bronze_group3.reviews
UNION ALL
SELECT 'clickstream', COUNT(*) FROM bronze_group3.clickstream;
```


<img src=".\images\query_2.jpeg" alt="The result of the second query" width="50%" />

```sql
-- Inspect the columns of the products table — notice the _ prefixed columns
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'bronze_group3' AND table_name = 'products'
ORDER BY ordinal_position;
```
<img src=".\images\query_3.jpeg" alt="The result of the third query" width="50%" />

```sql
-- Quick peek at order statuses
SELECT status, COUNT(*) AS cnt
FROM bronze_group3.orders
GROUP BY status
ORDER BY cnt DESC;
```
<img src=".\images\query_4.jpeg" alt="The result of the fourth query" width="50%" />

```sql
-- Check reviews — what ratings exist?
SELECT rating, COUNT(*) AS cnt
FROM bronze_group3.reviews
GROUP BY rating
ORDER BY rating;
```
<img src=".\images\query_5.jpeg" alt="The result of the sixth query" width="50%" />

```sql
-- Check clickstream — what event types exist?
SELECT event_type, COUNT(*) AS cnt
FROM bronze_group3.clickstream
GROUP BY event_type
ORDER BY cnt DESC;
```
<img src=".\images\query_6.jpeg" alt="The result of the last query" width="50%" />