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