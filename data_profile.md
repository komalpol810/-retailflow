cd ~/retailflow
cat > data_profile.md << 'EOF'
# Data Profile — RetailFlow Raw Layer
Dataset: Olist Brazilian E-Commerce (9 CSVs)
Profiled on: Aug 2026

## 1. Row Counts

| Table | Row Count |
|---|---|
| olist_customers_dataset | 99,441 |
| olist_orders_dataset | 99,441 |
| olist_order_items_dataset | 112,650 |
| olist_order_payments_dataset | 103,886 |
| olist_order_reviews_dataset | 99,224 |
| olist_products_dataset | 32,951 |
| olist_sellers_dataset | 3,095 |
| olist_geolocation_dataset | 1,000,163 |
| product_category_name_translation | 71 |

Note: geolocation is ~10x larger than every other table — expected, since it's a reference table with many lat/lng points per zip prefix, not one row per entity (see section 7).

## 2. Primary Key Checks

### orders (order_id)
- Duplicates found: 0 — clean primary key.

### customers (customer_id)
- Duplicates found: 0 — clean primary key.

### products (product_id)
- Duplicates found: 0 — clean primary key.

### reviews (review_id)
- Duplicates found: YES — 789 review_ids appear more than once (mostly 2x, a handful 3x).
- Interpretation: review_id is NOT a safe primary key on its own. Likely cause: Olist reuses review_id when a customer is asked to re-review, or multiple orders can share a review record. For dimensional modeling later, the real grain should probably be (review_id, order_id) together, not review_id alone — flagging this as a decision for Phase 3.

## 3. Foreign Key / Orphan Checks

### order_items → orders
- Orphaned rows: 0 — every order_item has a matching order.

### order_items → products
- Orphaned rows: 0 — every order_item references a valid product.

### orders → customers
- Orphaned rows: 0 — every order references a valid customer.

### orders with no matching order_items
- Count found: 775 orders have zero rows in order_items.
- Hypothesis: likely orders that were canceled or marked "unavailable" before any item/seller was finalized — worth cross-checking order_status for these 775 order_ids in a follow-up query. This is a candidate exclusion rule for the silver layer (Phase 2) depending on what the fact table needs.

## 4. Null Profiling

### orders — lifecycle timestamps (out of 99,441 total)
- order_approved_at nulls: 160
- order_delivered_carrier_date nulls: 1,783
- order_delivered_customer_date nulls: 2,965
- Hypothesis: nulls likely increase at each downstream stage of the order lifecycle (approved → carrier → delivered) because not every order makes it that far — canceled/unavailable orders stop early. Needs a follow-up query joining these nulls against order_status to confirm.

### reviews — comment fields (out of 99,224 total)
- review_comment_title nulls: 87,656 (88%)
- review_comment_message nulls: 58,247 (59%)
- Interpretation: most customers leave a star rating without free text — title is optional and rarely filled in even when a message is written. Not a data quality problem, just real-world behavior; comment fields should be treated as optional/sparse in downstream modeling, not something to impute.

## 5. Date Range

- order_purchase_timestamp: 2016-09-04 to 2018-10-17 (~2 years of data)

## 6. Categorical Distributions

### order_status
| Status | Count |
|---|---|
| delivered | 96,478 |
| shipped | 1,107 |
| canceled | 625 |
| unavailable | 609 |
| invoiced | 314 |
| processing | 301 |
| created | 5 |
| approved | 2 |

- 97% of all orders are "delivered" — heavily imbalanced. "created" and "approved" statuses have almost no rows (5 and 2) — these might be orders caught mid-lifecycle at data extraction time.

### payment_type
| Type | Count |
|---|---|
| credit_card | 76,795 |
| boleto | 19,784 |
| voucher | 5,775 |
| debit_card | 1,529 |
| not_defined | 3 |

- "not_defined" (3 rows) is a real data quality issue — 3 payments with no identifiable type. Small enough to investigate manually later or drop, worth a note not a fix right now.

## 7. Geolocation Anomaly

- Confirmed: many lat/lng rows per zip_code_prefix.
- Top offender: zip 24220 has 1,146 separate lat/lng rows; several other zips exceed 700-1,100 rows.
- Implication: geolocation has no natural primary key. Before this table can be joined to customers/sellers (which reference zip prefix), it will need aggregation — e.g. AVG(lat), AVG(lng) grouped by zip_code_prefix — decision deferred to Phase 2 (silver layer).

## 8. Data Type Issues (everything currently TEXT)

Columns to cast in Phase 2:
- order_items.price, freight_value → NUMERIC
- order_payments.payment_value → NUMERIC
- order_payments.payment_installments → INTEGER
- orders.order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date → TIMESTAMP
- reviews.review_creation_date, review_answer_timestamp → TIMESTAMP
- geolocation.geolocation_lat, geolocation_lng → NUMERIC

## 9. Summary of Data Quality Issues Found

1. review_id is not a unique primary key (789 duplicates) — grain needs redefining before dimensional modeling.
2. 775 orders have no order_items — needs investigation against order_status before deciding to include/exclude in the fact table.
3. Nulls increase across the order lifecycle timestamps (160 → 1,783 → 2,965) — likely tied to order_status, not random missingness.
4. geolocation has no primary key at the zip level — requires aggregation before it can join cleanly to customers/sellers.
5. 3 payments have payment_type = "not_defined" — minor, but a real gap.
6. review comment fields are heavily null (59-88%) — expected/optional, not a defect.

## 10. Open Questions / Decisions for Later Phases

- Should the fact table grain be (order_id) or (order_id, order_item_id)? Given order_items can have multiple rows per order, this affects the whole star schema design.
- How to aggregate geolocation down to one row per zip (avg lat/lng, or pick most frequent)?
- Should the 775 orders with no items be excluded from the sales fact table, or kept for a "canceled/incomplete orders" analysis?
- How to handle review_id duplication — dedupe, or model at (review_id + order_id) grain?

## Idempotency Test
- Ran ingest_raw.py twice; row counts identical before and after second run (table replaced via if_exists="replace"). Confirmed no duplication.
EOF# Data Profile — RetailFlow Raw Layer
Dataset: Olist Brazilian E-Commerce
Profiled on: [today's date]

## 1. Row Counts

| Table | Row Count |
|---|---|
| olist_customers_dataset | |
| olist_orders_dataset | |
| olist_order_items_dataset | |
| olist_order_payments_dataset | |
| olist_order_reviews_dataset | |
| olist_products_dataset | |
| olist_sellers_dataset | |
| olist_geolocation_dataset | |
| product_category_name_translation | |

## 2. Primary Key Checks

### orders (order_id)
- Duplicates found: [yes/no — how many?]

### customers (customer_id)
- Duplicates found:

### products (product_id)
- Duplicates found:

### reviews (review_id)
- Duplicates found:
- Note: [what you observed — is review_id truly unique?]

## 3. Foreign Key / Orphan Checks

### order_items → orders
- Orphaned rows (order_item exists but no matching order):

### order_items → products
- Orphaned rows:

### orders → customers
- Orphaned rows:

### orders with no order_items
- Count found:
- Hypothesis for why: [e.g. canceled before items assigned?]

## 4. Null Profiling

### orders — lifecycle timestamps
- order_approved_at nulls: 
- order_delivered_carrier_date nulls:
- order_delivered_customer_date nulls:
- Hypothesis: [e.g. correlate with order_status = canceled?]

### reviews — comment fields
- review_comment_title nulls:
- review_comment_message nulls:
- Hypothesis: [most customers don't leave a written review]

## 5. Date Ranges

- order_purchase_timestamp: min = ____, max = ____

## 6. Categorical Distributions

### order_status
| Status | Count |
|---|---|
| | |

### payment_type
| Type | Count |
|---|---|

## 7. Geolocation Anomaly

- Are there multiple lat/lng rows per zip_code_prefix? [yes/no]
- Example zip with most duplicates: 
- Implication: geolocation has no natural primary key — will need aggregation (e.g. AVG lat/lng per zip) in a later phase.

## 8. Data Type Issues (all columns currently TEXT)

List columns that are clearly not text and will need casting in Phase 2:
- price, freight_value, payment_value → should be NUMERIC
- order_purchase_timestamp and other date columns → should be TIMESTAMP
- payment_installments → should be INTEGER

## 9. Summary of Data Quality Issues Found

1. 
2. 
3. 

## 10. Open Questions / Decisions for Later Phases

- How to handle geolocation's many-rows-per-zip? 
- How to handle orders with missing carrier/delivery timestamps?
1. review_id is not a unique primary key (789 duplicates) — grain needs redefining before dimensional modeling.
2. 775 orders have no order_items — needs investigation against order_status before deciding to include/exclude in the fact table.
3. Nulls increase across the order lifecycle timestamps (160 → 1,783 → 2,965) — likely tied to order_status, not random missingness.
4. geolocation has no primary key at the zip level — requires aggregation before it can join cleanly to customers/sellers.
5. 3 payments have payment_type = "not_defined" — minor, but a real gap.
6. review comment fields are heavily null (59-88%) — expected/optional, not a defect.
cd ~/retailflow
cat > data_profile.md << 'EOF'
# Data Profile — RetailFlow Raw Layer
Dataset: Olist Brazilian E-Commerce (9 CSVs)
Profiled on: Aug 2026

## 1. Row Counts

| Table | Row Count |
|---|---|
| olist_customers_dataset | 99,441 |
| olist_orders_dataset | 99,441 |
| olist_order_items_dataset | 112,650 |
| olist_order_payments_dataset | 103,886 |
| olist_order_reviews_dataset | 99,224 |
| olist_products_dataset | 32,951 |
| olist_sellers_dataset | 3,095 |
| olist_geolocation_dataset | 1,000,163 |
| product_category_name_translation | 71 |

Note: geolocation is ~10x larger than every other table — expected, since it's a reference table with many lat/lng points per zip prefix, not one row per entity (see section 7).

## 2. Primary Key Checks

### orders (order_id)
- Duplicates found: 0 — clean primary key.

### customers (customer_id)
- Duplicates found: 0 — clean primary key.

### products (product_id)
- Duplicates found: 0 — clean primary key.

### reviews (review_id)
- Duplicates found: YES — 789 review_ids appear more than once (mostly 2x, a handful 3x).
- Interpretation: review_id is NOT a safe primary key on its own. Likely cause: Olist reuses review_id when a customer is asked to re-review, or multiple orders can share a review record. For dimensional modeling later, the real grain should probably be (review_id, order_id) together, not review_id alone — flagging this as a decision for Phase 3.

## 3. Foreign Key / Orphan Checks

### order_items → orders
- Orphaned rows: 0 — every order_item has a matching order.

### order_items → products
- Orphaned rows: 0 — every order_item references a valid product.

### orders → customers
- Orphaned rows: 0 — every order references a valid customer.

### orders with no matching order_items
- Count found: 775 orders have zero rows in order_items.
- Hypothesis: likely orders that were canceled or marked "unavailable" before any item/seller was finalized — worth cross-checking order_status for these 775 order_ids in a follow-up query. This is a candidate exclusion rule for the silver layer (Phase 2) depending on what the fact table needs.

## 4. Null Profiling

### orders — lifecycle timestamps (out of 99,441 total)
- order_approved_at nulls: 160
- order_delivered_carrier_date nulls: 1,783
- order_delivered_customer_date nulls: 2,965
- Hypothesis: nulls likely increase at each downstream stage of the order lifecycle (approved → carrier → delivered) because not every order makes it that far — canceled/unavailable orders stop early. Needs a follow-up query joining these nulls against order_status to confirm.

### reviews — comment fields (out of 99,224 total)
- review_comment_title nulls: 87,656 (88%)
- review_comment_message nulls: 58,247 (59%)
- Interpretation: most customers leave a star rating without free text — title is optional and rarely filled in even when a message is written. Not a data quality problem, just real-world behavior; comment fields should be treated as optional/sparse in downstream modeling, not something to impute.

## 5. Date Range

- order_purchase_timestamp: 2016-09-04 to 2018-10-17 (~2 years of data)

## 6. Categorical Distributions

### order_status
| Status | Count |
|---|---|
| delivered | 96,478 |
| shipped | 1,107 |
| canceled | 625 |
| unavailable | 609 |
| invoiced | 314 |
| processing | 301 |
| created | 5 |
| approved | 2 |

- 97% of all orders are "delivered" — heavily imbalanced. "created" and "approved" statuses have almost no rows (5 and 2) — these might be orders caught mid-lifecycle at data extraction time.

### payment_type
| Type | Count |
|---|---|
| credit_card | 76,795 |
| boleto | 19,784 |
| voucher | 5,775 |
| debit_card | 1,529 |
| not_defined | 3 |

- "not_defined" (3 rows) is a real data quality issue — 3 payments with no identifiable type. Small enough to investigate manually later or drop, worth a note not a fix right now.

## 7. Geolocation Anomaly

- Confirmed: many lat/lng rows per zip_code_prefix.
- Top offender: zip 24220 has 1,146 separate lat/lng rows; several other zips exceed 700-1,100 rows.
- Implication: geolocation has no natural primary key. Before this table can be joined to customers/sellers (which reference zip prefix), it will need aggregation — e.g. AVG(lat), AVG(lng) grouped by zip_code_prefix — decision deferred to Phase 2 (silver layer).

## 8. Data Type Issues (everything currently TEXT)

Columns to cast in Phase 2:
- order_items.price, freight_value → NUMERIC
- order_payments.payment_value → NUMERIC
- order_payments.payment_installments → INTEGER
- orders.order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date → TIMESTAMP
- reviews.review_creation_date, review_answer_timestamp → TIMESTAMP
- geolocation.geolocation_lat, geolocation_lng → NUMERIC

## 9. Summary of Data Quality Issues Found

1. review_id is not a unique primary key (789 duplicates) — grain needs redefining before dimensional modeling.
2. 775 orders have no order_items — needs investigation against order_status before deciding to include/exclude in the fact table.
3. Nulls increase across the order lifecycle timestamps (160 → 1,783 → 2,965) — likely tied to order_status, not random missingness.
4. geolocation has no primary key at the zip level — requires aggregation before it can join cleanly to customers/sellers.
5. 3 payments have payment_type = "not_defined" — minor, but a real gap.
6. review comment fields are heavily null (59-88%) — expected/optional, not a defect.

## 10. Open Questions / Decisions for Later Phases

- Should the fact table grain be (order_id) or (order_id, order_item_id)? Given order_items can have multiple rows per order, this affects the whole star schema design.
- How to aggregate geolocation down to one row per zip (avg lat/lng, or pick most frequent)?
- Should the 775 orders with no items be excluded from the sales fact table, or kept for a "canceled/incomplete orders" analysis?
- How to handle review_id duplication — dedupe, or model at (review_id + order_id) grain?

## Idempotency Test
- Ran ingest_raw.py twice; row counts identical before and after second run (table replaced via if_exists="replace"). Confirmed no duplication.
EOF
