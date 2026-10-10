# Databricks notebook source
# DBTITLE 1,Demo Overview
# MAGIC %md
# MAGIC # ☕ Coffee Chain Demo - Complete Setup
# MAGIC
# MAGIC **Session:** From Lakehouse to Live App: Building Intelligent Data Apps with Databricks Apps and Genie Agents
# MAGIC
# MAGIC **Architecture:** CSV → Unity Catalog (Bronze → Curated → Gold) → Governance (Masks + Row Filters) → Genie Agent → Databricks App
# MAGIC
# MAGIC **Three NYC stores:** Astoria · Lower Manhattan · Hell's Kitchen
# MAGIC
# MAGIC **Two personas:**
# MAGIC - `coffee_hq` — HQ Operations: sees all stores, all data
# MAGIC - `coffee_store_mgr` — Store Manager (Hell's Kitchen): sees only their store, PII masked
# MAGIC
# MAGIC ---
# MAGIC ⚠️ **Pre-requisite:** Create account groups `coffee_hq` and `coffee_store_mgr` in the Databricks Account Console before running Section 4 (Governance Grants).

# COMMAND ----------

# DBTITLE 1,Section 1 - Schemas
# MAGIC %md
# MAGIC ## 1️⃣ Workspace Setup
# MAGIC Create the medallion architecture schemas. Data lives in `dev_overwatch` catalog.

# COMMAND ----------

# DBTITLE 1,Create Schemas
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_overwatch.coffee_bronze
# MAGIC   COMMENT 'Raw ingested data from CSV files - Coffee Chain Demo';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_overwatch.coffee_curated
# MAGIC   COMMENT 'Cleaned, governed dimension and fact tables - Coffee Chain Demo';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_overwatch.coffee_gold
# MAGIC   COMMENT 'Analytics views and business metrics - Coffee Chain Demo';

# COMMAND ----------

# DBTITLE 1,Section 2 - Bronze
# MAGIC %md
# MAGIC ## 2️⃣ Bronze Layer — Raw Ingestion
# MAGIC Ingest 9 CSV files from the Unity Catalog Volume into bronze tables. Minimal transformation — just column name fixes.

# COMMAND ----------

# DBTITLE 1,Ingest Clean CSVs
# MAGIC %sql
# MAGIC -- Sales Receipts: 49,894 transactions from April 2019
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.sales_receipts AS
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/201904 sales reciepts.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);
# MAGIC
# MAGIC -- Products: 88 items (beverages, food, merchandise)
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.products AS
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/product.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);
# MAGIC
# MAGIC -- Sales Outlets: 9 locations (3 retail + warehouse + HQ)
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.sales_outlets AS
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/sales_outlet.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);
# MAGIC
# MAGIC -- Customers: 2,246 loyalty members
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.customers AS
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/customer.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);
# MAGIC
# MAGIC -- Dates: April 2019 calendar (30 days)
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.dates AS
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/Dates.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);
# MAGIC
# MAGIC -- Generations: birth year to generation mapping
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.generations AS
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/generations.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);

# COMMAND ----------

# DBTITLE 1,Ingest CSVs with Column Fixes
# MAGIC %sql
# MAGIC -- Staff: drop empty _c6, _c7 columns
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.staff AS
# MAGIC SELECT staff_id, first_name, last_name, position, start_date, location
# MAGIC FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/staff.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);
# MAGIC
# MAGIC -- Pastry Inventory: rename "% waste" to waste_pct
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.pastry_inventory AS
# MAGIC SELECT sales_outlet_id, transaction_date, product_id, start_of_day,
# MAGIC        quantity_sold, waste, `% waste` AS waste_pct
# MAGIC FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/pastry inventory.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);
# MAGIC
# MAGIC -- Sales Targets: rename "merchandise _goal"
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_bronze.sales_targets AS
# MAGIC SELECT sales_outlet_id, year_month, beans_goal, beverage_goal,
# MAGIC        food_goal, `merchandise _goal` AS merchandise_goal, total_goal
# MAGIC FROM read_files(
# MAGIC   '/Volumes/dev_overwatch/coffee_raw/raw/sales targets.csv',
# MAGIC   format => 'csv', header => true, inferSchema => true);

# COMMAND ----------

# DBTITLE 1,Section 3 - Curated
# MAGIC %md
# MAGIC ## 3️⃣ Curated Layer — Cleaned & Commented
# MAGIC Clean data types, fix column names, add business-friendly comments. Every table and column is documented — Genie relies on these comments to understand the data.

# COMMAND ----------

# DBTITLE 1,Curated: Stores
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_curated.stores (
# MAGIC   store_id INT COMMENT 'Unique store identifier',
# MAGIC   store_name STRING COMMENT 'Store display name based on neighborhood',
# MAGIC   neighborhood STRING COMMENT 'NYC neighborhood where the store is located',
# MAGIC   store_address STRING COMMENT 'Street address of the store',
# MAGIC   store_city STRING COMMENT 'City where the store is located',
# MAGIC   manager_staff_id INT COMMENT 'Staff ID of the store manager'
# MAGIC )
# MAGIC COMMENT 'Three retail coffee shop locations in New York City. Simulated data for platform demo.';
# MAGIC
# MAGIC INSERT INTO dev_overwatch.coffee_curated.stores
# MAGIC SELECT sales_outlet_id, Neighorhood, Neighorhood, store_address, store_city, manager
# MAGIC FROM dev_overwatch.coffee_bronze.sales_outlets
# MAGIC WHERE sales_outlet_id IN (3, 5, 8);

# COMMAND ----------

# DBTITLE 1,Curated: Products
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_curated.products (
# MAGIC   product_id INT COMMENT 'Unique product identifier',
# MAGIC   product_group STRING COMMENT 'High-level grouping: Whole Bean/Teas, Beverages, Food, etc.',
# MAGIC   product_category STRING COMMENT 'Product category within the group',
# MAGIC   product_type STRING COMMENT 'Specific product type',
# MAGIC   product_name STRING COMMENT 'Display name of the product',
# MAGIC   product_description STRING COMMENT 'Marketing description of the product',
# MAGIC   unit_of_measure STRING COMMENT 'Selling unit (e.g., 12 oz, 1 lb, each)',
# MAGIC   wholesale_price DOUBLE COMMENT 'Current wholesale cost in USD',
# MAGIC   retail_price DOUBLE COMMENT 'Current retail selling price in USD',
# MAGIC   margin_pct DOUBLE COMMENT 'Gross margin percentage: (retail - wholesale) / retail * 100',
# MAGIC   is_promo BOOLEAN COMMENT 'Whether the product is currently on promotion',
# MAGIC   is_new BOOLEAN COMMENT 'Whether the product is flagged as new'
# MAGIC )
# MAGIC COMMENT 'Coffee chain product catalog with 88 items including beverages, food, and merchandise. Simulated data for platform demo.';
# MAGIC
# MAGIC INSERT INTO dev_overwatch.coffee_curated.products
# MAGIC SELECT 
# MAGIC   product_id, product_group, product_category, product_type,
# MAGIC   product AS product_name, product_description, unit_of_measure,
# MAGIC   current_wholesale_price AS wholesale_price,
# MAGIC   CAST(REPLACE(REPLACE(current_retail_price, '$', ''), ' ', '') AS DOUBLE) AS retail_price,
# MAGIC   ROUND((CAST(REPLACE(REPLACE(current_retail_price, '$', ''), ' ', '') AS DOUBLE) - current_wholesale_price) 
# MAGIC     / CAST(REPLACE(REPLACE(current_retail_price, '$', ''), ' ', '') AS DOUBLE) * 100, 1) AS margin_pct,
# MAGIC   CASE WHEN promo_yn = 'Y' THEN true ELSE false END,
# MAGIC   CASE WHEN new_product_yn = 'Y' THEN true ELSE false END
# MAGIC FROM dev_overwatch.coffee_bronze.products;

# COMMAND ----------

# DBTITLE 1,Curated: Customers (PII table)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_curated.customers (
# MAGIC   customer_id INT COMMENT 'Unique customer identifier',
# MAGIC   customer_name STRING COMMENT 'Full name of the customer (PII - masked for non-HQ roles)',
# MAGIC   customer_email STRING COMMENT 'Email address (PII - masked for non-HQ roles)',
# MAGIC   gender STRING COMMENT 'Customer gender (PII - masked for non-HQ roles)',
# MAGIC   birth_year INT COMMENT 'Year of birth',
# MAGIC   generation STRING COMMENT 'Generational cohort: Baby Boomers, Gen X, Millennials, Gen Z',
# MAGIC   home_store_id INT COMMENT 'Store ID where the customer is registered',
# MAGIC   customer_since DATE COMMENT 'Date the customer first registered',
# MAGIC   loyalty_card_number STRING COMMENT 'Loyalty program card number'
# MAGIC )
# MAGIC COMMENT 'Customer profiles with loyalty and demographic data. Contains PII fields protected by column masks. Simulated data for platform demo.';
# MAGIC
# MAGIC INSERT INTO dev_overwatch.coffee_curated.customers
# MAGIC SELECT c.customer_id, c.`customer_first-name`, c.customer_email, c.gender,
# MAGIC        c.birth_year, g.generation, c.home_store, c.customer_since, c.loyalty_card_number
# MAGIC FROM dev_overwatch.coffee_bronze.customers c
# MAGIC LEFT JOIN dev_overwatch.coffee_bronze.generations g ON c.birth_year = g.birth_year;

# COMMAND ----------

# DBTITLE 1,Curated: Staff, Transactions, Pastry Waste, Targets
# MAGIC %sql
# MAGIC -- Staff
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_curated.staff (
# MAGIC   staff_id INT COMMENT 'Unique staff identifier',
# MAGIC   first_name STRING COMMENT 'Staff first name',
# MAGIC   last_name STRING COMMENT 'Staff last name (PII - masked for non-HQ roles)',
# MAGIC   position STRING COMMENT 'Job title: CEO, CFO, Manager, Barista, Roaster, etc.',
# MAGIC   start_date DATE COMMENT 'Employment start date',
# MAGIC   store_id INT COMMENT 'Store assignment (NULL for HQ/warehouse staff)'
# MAGIC )
# MAGIC COMMENT 'Staff directory with 55 employees. Contains PII. Simulated data for platform demo.';
# MAGIC
# MAGIC INSERT INTO dev_overwatch.coffee_curated.staff
# MAGIC SELECT staff_id, first_name, last_name, position, start_date,
# MAGIC        CASE WHEN location IN ('HQ','WH','FL') THEN NULL ELSE CAST(location AS INT) END
# MAGIC FROM dev_overwatch.coffee_bronze.staff;
# MAGIC
# MAGIC -- Transactions (main fact table)
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_curated.transactions (
# MAGIC   transaction_id INT COMMENT 'Unique transaction identifier',
# MAGIC   transaction_date DATE COMMENT 'Date of the transaction (April 2019)',
# MAGIC   transaction_time STRING COMMENT 'Time of day (HH:mm:ss)',
# MAGIC   store_id INT COMMENT 'Store where the transaction took place',
# MAGIC   staff_id INT COMMENT 'Staff member who processed the transaction',
# MAGIC   customer_id INT COMMENT 'Customer ID (NULL for non-loyalty purchases)',
# MAGIC   is_instore BOOLEAN COMMENT 'Whether the order was placed in-store',
# MAGIC   order_number INT COMMENT 'Order number within the transaction',
# MAGIC   line_item_id INT COMMENT 'Line item sequence within the order',
# MAGIC   product_id INT COMMENT 'Product purchased',
# MAGIC   quantity INT COMMENT 'Number of units purchased',
# MAGIC   line_item_amount DOUBLE COMMENT 'Total amount for this line item in USD',
# MAGIC   unit_price DOUBLE COMMENT 'Price per unit in USD',
# MAGIC   is_promo_item BOOLEAN COMMENT 'Whether this item was at a promotional price'
# MAGIC )
# MAGIC COMMENT 'April 2019 sales transactions with ~50K line items across 3 NYC stores. Simulated data for platform demo.';
# MAGIC
# MAGIC INSERT INTO dev_overwatch.coffee_curated.transactions
# MAGIC SELECT transaction_id, transaction_date, DATE_FORMAT(transaction_time, 'HH:mm:ss'),
# MAGIC        sales_outlet_id, staff_id,
# MAGIC        CASE WHEN customer_id = 0 THEN NULL ELSE customer_id END,
# MAGIC        CASE WHEN instore_yn = 'Y' THEN true ELSE false END,
# MAGIC        order, line_item_id, product_id, quantity, line_item_amount, unit_price,
# MAGIC        CASE WHEN promo_item_yn = 'Y' THEN true ELSE false END
# MAGIC FROM dev_overwatch.coffee_bronze.sales_receipts;
# MAGIC
# MAGIC -- Pastry Waste
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_curated.pastry_waste (
# MAGIC   store_id INT COMMENT 'Store where inventory was tracked',
# MAGIC   transaction_date DATE COMMENT 'Date of the inventory record',
# MAGIC   product_id INT COMMENT 'Pastry product identifier',
# MAGIC   start_of_day INT COMMENT 'Units at start of day',
# MAGIC   quantity_sold INT COMMENT 'Units sold during the day',
# MAGIC   waste INT COMMENT 'Units wasted (unsold, discarded)',
# MAGIC   waste_pct DOUBLE COMMENT 'Waste as percentage of start-of-day inventory'
# MAGIC )
# MAGIC COMMENT 'Daily pastry inventory tracking. Simulated data for platform demo.';
# MAGIC
# MAGIC INSERT INTO dev_overwatch.coffee_curated.pastry_waste
# MAGIC SELECT sales_outlet_id, transaction_date, product_id, start_of_day,
# MAGIC        quantity_sold, waste, CAST(REPLACE(waste_pct, '%', '') AS DOUBLE)
# MAGIC FROM dev_overwatch.coffee_bronze.pastry_inventory;
# MAGIC
# MAGIC -- Sales Targets
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_curated.sales_targets (
# MAGIC   store_id INT COMMENT 'Store identifier',
# MAGIC   target_month STRING COMMENT 'Target period (e.g., Apr-19)',
# MAGIC   beans_goal INT COMMENT 'Monthly goal for beans/whole bean in USD',
# MAGIC   beverage_goal INT COMMENT 'Monthly goal for beverages in USD',
# MAGIC   food_goal INT COMMENT 'Monthly goal for food items in USD',
# MAGIC   merchandise_goal INT COMMENT 'Monthly goal for merchandise in USD',
# MAGIC   total_goal INT COMMENT 'Total monthly sales goal in USD'
# MAGIC )
# MAGIC COMMENT 'Monthly sales targets by store. Simulated data for platform demo.';
# MAGIC
# MAGIC INSERT INTO dev_overwatch.coffee_curated.sales_targets
# MAGIC SELECT sales_outlet_id, year_month, beans_goal, beverage_goal,
# MAGIC        food_goal, merchandise_goal, total_goal
# MAGIC FROM dev_overwatch.coffee_bronze.sales_targets WHERE sales_outlet_id IN (3, 5, 8);

# COMMAND ----------

# DBTITLE 1,Section 4 - Governance
# MAGIC %md
# MAGIC ## 4️⃣ Governance — Column Masks, Row Filters & Grants
# MAGIC Unity Catalog enforces governance at the data layer — not in the app, not in the query, not in the dashboard. The same mask and filter apply whether you query via SQL, Genie, or the Databricks App.
# MAGIC
# MAGIC **Column masks:** Customer name, email, gender → hidden for store managers. Staff last name → initial only.
# MAGIC
# MAGIC **Row filter:** Store managers see only their store (Hell's Kitchen = store 8). HQ sees all.
# MAGIC
# MAGIC ⚠️ **Run the grants cell manually** — requires the account groups to exist first.

# COMMAND ----------

# DBTITLE 1,Column Mask Functions
# MAGIC %sql
# MAGIC -- Mask customer/staff names
# MAGIC CREATE OR REPLACE FUNCTION dev_overwatch.coffee_curated.mask_name(val STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Masks names for non-HQ users.'
# MAGIC RETURN CASE WHEN is_account_group_member('coffee_hq') THEN val ELSE 'Masked' END;
# MAGIC
# MAGIC -- Mask email
# MAGIC CREATE OR REPLACE FUNCTION dev_overwatch.coffee_curated.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Masks email for non-HQ users.'
# MAGIC RETURN CASE WHEN is_account_group_member('coffee_hq') THEN email ELSE '***@***.***' END;
# MAGIC
# MAGIC -- Mask gender
# MAGIC CREATE OR REPLACE FUNCTION dev_overwatch.coffee_curated.mask_gender(val STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Masks gender for non-HQ users.'
# MAGIC RETURN CASE WHEN is_account_group_member('coffee_hq') THEN val ELSE 'Masked' END;
# MAGIC
# MAGIC -- Mask staff last name (show initial only)
# MAGIC CREATE OR REPLACE FUNCTION dev_overwatch.coffee_curated.mask_last_name(val STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Masks staff last name for non-HQ users. Shows first initial.'
# MAGIC RETURN CASE WHEN is_account_group_member('coffee_hq') THEN val ELSE CONCAT(LEFT(val, 1), '***') END;

# COMMAND ----------

# DBTITLE 1,Apply Masks to Tables
# MAGIC %sql
# MAGIC -- Customer PII masks
# MAGIC ALTER TABLE dev_overwatch.coffee_curated.customers ALTER COLUMN customer_name SET MASK dev_overwatch.coffee_curated.mask_name;
# MAGIC ALTER TABLE dev_overwatch.coffee_curated.customers ALTER COLUMN customer_email SET MASK dev_overwatch.coffee_curated.mask_email;
# MAGIC ALTER TABLE dev_overwatch.coffee_curated.customers ALTER COLUMN gender SET MASK dev_overwatch.coffee_curated.mask_gender;
# MAGIC
# MAGIC -- Staff PII mask
# MAGIC ALTER TABLE dev_overwatch.coffee_curated.staff ALTER COLUMN last_name SET MASK dev_overwatch.coffee_curated.mask_last_name;

# COMMAND ----------

# DBTITLE 1,Row Filter Function
# MAGIC %sql
# MAGIC -- Row filter: store managers see only store 8 (Hell's Kitchen)
# MAGIC -- IMPORTANT: Add yourself to coffee_hq group, otherwise you'll see no data
# MAGIC CREATE OR REPLACE FUNCTION dev_overwatch.coffee_curated.store_filter(store_id INT)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Row filter: coffee_hq sees all stores, coffee_store_mgr sees only store 8 (Hells Kitchen).'
# MAGIC RETURN 
# MAGIC   is_account_group_member('coffee_hq')
# MAGIC   OR (is_account_group_member('coffee_store_mgr') AND store_id = 8);
# MAGIC
# MAGIC -- Apply to transactions
# MAGIC ALTER TABLE dev_overwatch.coffee_curated.transactions
# MAGIC   SET ROW FILTER dev_overwatch.coffee_curated.store_filter ON (store_id);

# COMMAND ----------

# DBTITLE 1,⚠️ Grants (run manually after creating groups)
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- ⚠️ RUN THIS MANUALLY after creating account groups
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Catalog access
# MAGIC GRANT USE CATALOG ON CATALOG dev_overwatch TO `coffee_hq`;
# MAGIC GRANT USE CATALOG ON CATALOG dev_overwatch TO `coffee_store_mgr`;
# MAGIC
# MAGIC -- Schema access (curated + gold only, NOT bronze)
# MAGIC GRANT USE SCHEMA ON SCHEMA dev_overwatch.coffee_curated TO `coffee_hq`;
# MAGIC GRANT USE SCHEMA ON SCHEMA dev_overwatch.coffee_curated TO `coffee_store_mgr`;
# MAGIC GRANT USE SCHEMA ON SCHEMA dev_overwatch.coffee_gold TO `coffee_hq`;
# MAGIC GRANT USE SCHEMA ON SCHEMA dev_overwatch.coffee_gold TO `coffee_store_mgr`;
# MAGIC
# MAGIC -- Table SELECT grants
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.stores TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.stores TO `coffee_store_mgr`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.products TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.products TO `coffee_store_mgr`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.customers TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.customers TO `coffee_store_mgr`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.staff TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.staff TO `coffee_store_mgr`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.transactions TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.transactions TO `coffee_store_mgr`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.pastry_waste TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.pastry_waste TO `coffee_store_mgr`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.sales_targets TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_curated.sales_targets TO `coffee_store_mgr`;
# MAGIC
# MAGIC -- store_notes: INSERT for flagging actions from the app
# MAGIC GRANT SELECT, MODIFY ON TABLE dev_overwatch.coffee_curated.store_notes TO `coffee_hq`;
# MAGIC GRANT SELECT, MODIFY ON TABLE dev_overwatch.coffee_curated.store_notes TO `coffee_store_mgr`;
# MAGIC
# MAGIC -- Gold views
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_gold.v_coffee_performance TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_gold.v_coffee_performance TO `coffee_store_mgr`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_gold.store_scorecard TO `coffee_hq`;
# MAGIC GRANT SELECT ON TABLE dev_overwatch.coffee_gold.store_scorecard TO `coffee_store_mgr`;
# MAGIC
# MAGIC -- Function EXECUTE grants
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.mask_name TO `coffee_hq`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.mask_name TO `coffee_store_mgr`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.mask_email TO `coffee_hq`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.mask_email TO `coffee_store_mgr`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.mask_gender TO `coffee_hq`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.mask_gender TO `coffee_store_mgr`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.mask_last_name TO `coffee_hq`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.mask_last_name TO `coffee_store_mgr`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.store_filter TO `coffee_hq`;
# MAGIC GRANT EXECUTE ON FUNCTION dev_overwatch.coffee_curated.store_filter TO `coffee_store_mgr`;

# COMMAND ----------

# DBTITLE 1,Section 5 - Gold
# MAGIC %md
# MAGIC ## 5️⃣ Gold Layer — Analytics View + Store Scorecard
# MAGIC The gold view joins transactions with products, stores, and targets. It adds the **Store Health Score** (0–5) — an illustrative business metric.
# MAGIC
# MAGIC The **store_scorecard** powers the app's KPI header. The **store_notes** table receives "Flag waste issue" actions from the app.
# MAGIC
# MAGIC Governance flows through automatically: masks and row filters apply when any user queries these views.

# COMMAND ----------

# DBTITLE 1,Gold: v_coffee_performance
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dev_overwatch.coffee_gold.v_coffee_performance
# MAGIC COMMENT 'Main analytics view. Includes Store Health Score (0-5), an illustrative business metric. Governance enforced automatically per user. Simulated data for platform demo.'
# MAGIC AS
# MAGIC WITH daily_store_stats AS (
# MAGIC   SELECT t.store_id, t.transaction_date,
# MAGIC     COUNT(DISTINCT t.transaction_id) AS num_transactions,
# MAGIC     SUM(t.line_item_amount) AS daily_revenue,
# MAGIC     COUNT(DISTINCT t.customer_id) AS unique_customers,
# MAGIC     SUM(CASE WHEN p.product_group = 'Beverages' THEN t.line_item_amount ELSE 0 END) AS beverage_revenue,
# MAGIC     SUM(t.line_item_amount) AS total_revenue
# MAGIC   FROM dev_overwatch.coffee_curated.transactions t
# MAGIC   JOIN dev_overwatch.coffee_curated.products p ON t.product_id = p.product_id
# MAGIC   GROUP BY t.store_id, t.transaction_date
# MAGIC ),
# MAGIC store_monthly AS (
# MAGIC   SELECT store_id,
# MAGIC     SUM(daily_revenue) AS monthly_revenue, AVG(daily_revenue) AS avg_daily_revenue,
# MAGIC     SUM(num_transactions) AS total_transactions, SUM(unique_customers) AS total_customer_visits,
# MAGIC     SUM(beverage_revenue) / SUM(total_revenue) * 100 AS beverage_share_pct
# MAGIC   FROM daily_store_stats GROUP BY store_id
# MAGIC ),
# MAGIC waste_stats AS (
# MAGIC   SELECT store_id, AVG(waste_pct) AS avg_waste_pct
# MAGIC   FROM dev_overwatch.coffee_curated.pastry_waste GROUP BY store_id
# MAGIC ),
# MAGIC chain_avg AS (
# MAGIC   SELECT AVG(avg_daily_revenue) AS chain_avg_daily_revenue FROM store_monthly
# MAGIC )
# MAGIC SELECT
# MAGIC   t.transaction_id, t.transaction_date, t.transaction_time, t.store_id,
# MAGIC   s.store_name, s.neighborhood, t.staff_id, t.customer_id,
# MAGIC   c.customer_name, c.gender AS customer_gender, c.generation AS customer_generation,
# MAGIC   t.product_id, p.product_name, p.product_group, p.product_category, p.product_type,
# MAGIC   t.quantity, t.line_item_amount, t.unit_price,
# MAGIC   p.retail_price, p.wholesale_price, p.margin_pct AS product_margin_pct,
# MAGIC   t.is_instore, t.is_promo_item,
# MAGIC   sm.monthly_revenue AS store_monthly_revenue,
# MAGIC   st.total_goal AS store_monthly_target,
# MAGIC   ROUND(sm.monthly_revenue / st.total_goal * 100, 1) AS target_attainment_pct,
# MAGIC   ROUND(ws.avg_waste_pct, 1) AS store_avg_waste_pct,
# MAGIC   ROUND(sm.beverage_share_pct, 1) AS store_beverage_share_pct,
# MAGIC   -- Store Health Score (0-5)
# MAGIC   (
# MAGIC     CASE WHEN sm.monthly_revenue / st.total_goal >= 1.0 THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN ws.avg_waste_pct < 30 THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN sm.avg_daily_revenue > ca.chain_avg_daily_revenue THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN sm.beverage_share_pct > 60 THEN 1 ELSE 0 END +
# MAGIC     CASE WHEN sm.total_customer_visits > 1000 THEN 1 ELSE 0 END
# MAGIC   ) AS store_health_score,
# MAGIC   CASE 
# MAGIC     WHEN (
# MAGIC       CASE WHEN sm.monthly_revenue / st.total_goal >= 1.0 THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN ws.avg_waste_pct < 30 THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN sm.avg_daily_revenue > ca.chain_avg_daily_revenue THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN sm.beverage_share_pct > 60 THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN sm.total_customer_visits > 1000 THEN 1 ELSE 0 END
# MAGIC     ) <= 2 THEN 'Needs Attention'
# MAGIC     WHEN (
# MAGIC       CASE WHEN sm.monthly_revenue / st.total_goal >= 1.0 THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN ws.avg_waste_pct < 30 THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN sm.avg_daily_revenue > ca.chain_avg_daily_revenue THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN sm.beverage_share_pct > 60 THEN 1 ELSE 0 END +
# MAGIC       CASE WHEN sm.total_customer_visits > 1000 THEN 1 ELSE 0 END
# MAGIC     ) = 3 THEN 'Solid'
# MAGIC     ELSE 'Thriving'
# MAGIC   END AS store_health_level
# MAGIC FROM dev_overwatch.coffee_curated.transactions t
# MAGIC JOIN dev_overwatch.coffee_curated.products p ON t.product_id = p.product_id
# MAGIC JOIN dev_overwatch.coffee_curated.stores s ON t.store_id = s.store_id
# MAGIC LEFT JOIN dev_overwatch.coffee_curated.customers c ON t.customer_id = c.customer_id
# MAGIC JOIN store_monthly sm ON t.store_id = sm.store_id
# MAGIC JOIN dev_overwatch.coffee_curated.sales_targets st ON t.store_id = st.store_id
# MAGIC JOIN waste_stats ws ON t.store_id = ws.store_id
# MAGIC CROSS JOIN chain_avg ca;

# COMMAND ----------

# DBTITLE 1,Gold: Store Scorecard + Store Notes
# MAGIC %sql
# MAGIC -- Store Scorecard: powers the app KPI header
# MAGIC CREATE OR REPLACE VIEW dev_overwatch.coffee_gold.store_scorecard
# MAGIC COMMENT 'Per-store KPI summary for the app header. Row-filtered via transactions governance. Simulated data for platform demo.'
# MAGIC AS
# MAGIC WITH store_revenue AS (
# MAGIC   SELECT t.store_id, s.store_name, s.neighborhood,
# MAGIC     SUM(t.line_item_amount) AS total_revenue,
# MAGIC     COUNT(DISTINCT t.transaction_id) AS total_transactions,
# MAGIC     COUNT(DISTINCT t.customer_id) AS unique_customers,
# MAGIC     COUNT(DISTINCT t.transaction_date) AS active_days,
# MAGIC     SUM(CASE WHEN p.product_group = 'Beverages' THEN t.line_item_amount ELSE 0 END) 
# MAGIC       / SUM(t.line_item_amount) * 100 AS beverage_share_pct
# MAGIC   FROM dev_overwatch.coffee_curated.transactions t
# MAGIC   JOIN dev_overwatch.coffee_curated.stores s ON t.store_id = s.store_id
# MAGIC   JOIN dev_overwatch.coffee_curated.products p ON t.product_id = p.product_id
# MAGIC   GROUP BY t.store_id, s.store_name, s.neighborhood
# MAGIC ),
# MAGIC waste AS (SELECT store_id, ROUND(AVG(waste_pct), 1) AS avg_waste_pct FROM dev_overwatch.coffee_curated.pastry_waste GROUP BY store_id),
# MAGIC targets AS (SELECT store_id, total_goal FROM dev_overwatch.coffee_curated.sales_targets),
# MAGIC chain_avg AS (SELECT AVG(total_revenue / active_days) AS avg_daily_rev FROM store_revenue)
# MAGIC SELECT r.store_id, r.store_name, r.neighborhood,
# MAGIC   ROUND(r.total_revenue, 2) AS monthly_revenue, tg.total_goal AS monthly_target,
# MAGIC   ROUND(r.total_revenue / tg.total_goal * 100, 1) AS target_attainment_pct,
# MAGIC   r.total_transactions, r.unique_customers,
# MAGIC   ROUND(r.total_revenue / r.active_days, 2) AS avg_daily_revenue,
# MAGIC   w.avg_waste_pct, ROUND(r.beverage_share_pct, 1) AS beverage_share_pct,
# MAGIC   (CASE WHEN r.total_revenue / tg.total_goal >= 1.0 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN w.avg_waste_pct < 30 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.total_revenue / r.active_days > ca.avg_daily_rev THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.beverage_share_pct > 60 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.unique_customers > 1000 THEN 1 ELSE 0 END) AS health_score,
# MAGIC   CASE WHEN (CASE WHEN r.total_revenue / tg.total_goal >= 1.0 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN w.avg_waste_pct < 30 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.total_revenue / r.active_days > ca.avg_daily_rev THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.beverage_share_pct > 60 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.unique_customers > 1000 THEN 1 ELSE 0 END) <= 2 THEN 'Needs Attention'
# MAGIC   WHEN (CASE WHEN r.total_revenue / tg.total_goal >= 1.0 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN w.avg_waste_pct < 30 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.total_revenue / r.active_days > ca.avg_daily_rev THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.beverage_share_pct > 60 THEN 1 ELSE 0 END
# MAGIC    + CASE WHEN r.unique_customers > 1000 THEN 1 ELSE 0 END) = 3 THEN 'Solid'
# MAGIC   ELSE 'Thriving' END AS health_level
# MAGIC FROM store_revenue r
# MAGIC JOIN waste w ON r.store_id = w.store_id
# MAGIC JOIN targets tg ON r.store_id = tg.store_id
# MAGIC CROSS JOIN chain_avg ca;
# MAGIC
# MAGIC -- Store Notes: action table for the app's "Flag waste issue" button
# MAGIC CREATE OR REPLACE TABLE dev_overwatch.coffee_curated.store_notes (
# MAGIC   note_id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Auto-generated note identifier',
# MAGIC   store_id INT COMMENT 'Store this note applies to',
# MAGIC   flagged_by STRING COMMENT 'Username who created the flag',
# MAGIC   flag_type STRING COMMENT 'Type: Waste Issue, Staffing, Inventory, Other',
# MAGIC   note STRING COMMENT 'Free-text note from the store manager',
# MAGIC   created_at TIMESTAMP COMMENT 'When the flag was created'
# MAGIC )
# MAGIC COMMENT 'Store manager action log. Written by the Databricks App. Simulated data for platform demo.';

# COMMAND ----------

# DBTITLE 1,Section 6 - Validation
# MAGIC %md
# MAGIC ## 6️⃣ Validation
# MAGIC Run these checks to confirm everything works.

# COMMAND ----------

# DBTITLE 1,Validate Row Counts and Health Scores
# MAGIC %sql
# MAGIC -- Row counts
# MAGIC SELECT 'transactions' as tbl, COUNT(*) as rows FROM dev_overwatch.coffee_curated.transactions
# MAGIC UNION ALL SELECT 'customers', COUNT(*) FROM dev_overwatch.coffee_curated.customers
# MAGIC UNION ALL SELECT 'products', COUNT(*) FROM dev_overwatch.coffee_curated.products
# MAGIC UNION ALL SELECT 'stores', COUNT(*) FROM dev_overwatch.coffee_curated.stores
# MAGIC UNION ALL SELECT 'staff', COUNT(*) FROM dev_overwatch.coffee_curated.staff
# MAGIC UNION ALL SELECT 'pastry_waste', COUNT(*) FROM dev_overwatch.coffee_curated.pastry_waste
# MAGIC UNION ALL SELECT 'gold_view', COUNT(*) FROM dev_overwatch.coffee_gold.v_coffee_performance;
# MAGIC
# MAGIC -- Store Health Scores
# MAGIC SELECT store_name, monthly_revenue, target_attainment_pct, avg_waste_pct, health_score, health_level
# MAGIC FROM dev_overwatch.coffee_gold.store_scorecard ORDER BY store_name;