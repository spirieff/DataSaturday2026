# Databricks notebook source
# DBTITLE 1,PROMPT 1: Data + Governance
# MAGIC %md
# MAGIC >
# MAGIC > **PROMPT 1 — paste into Assistant:**
# MAGIC >
# MAGIC > I have 9 CSV files uploaded to /Volumes/dev_overwatch/coffee_raw/raw/ for a fictional coffee chain with 3 NYC stores. The files are:
# MAGIC >
# MAGIC > - 201904 sales reciepts.csv (49K transactions, April 2019)
# MAGIC > - product.csv (88 products - note: current_retail_price has $ signs and spaces to clean)
# MAGIC > - sales_outlet.csv (9 outlets, only 3 are retail: IDs 3, 5, 8 - also "Neighorhood" is misspelled)
# MAGIC > - staff.csv (55 staff - has empty _c6, _c7 columns to drop)
# MAGIC > - customer.csv (2,246 customers - "customer_first-name" has a hyphen)
# MAGIC > - Dates.csv, generations.csv
# MAGIC > - pastry inventory.csv ("% waste" column needs renaming)
# MAGIC > - sales targets.csv ("merchandise _goal" has a space in the name)
# MAGIC >
# MAGIC > Please build the complete data pipeline in the dev_overwatch catalog:
# MAGIC >
# MAGIC > 1. Create three schemas: coffee_v2_bronze, coffee_v2_curated, coffee_v2_gold
# MAGIC >
# MAGIC > 2. Bronze layer: Ingest all 9 CSVs into coffee_v2_bronze, fixing the column name issues listed above.
# MAGIC >
# MAGIC > 3. Curated layer in coffee_v2_curated:
# MAGIC >    - stores: only the 3 retail stores (IDs 3, 5, 8) with store_name based on neighborhood
# MAGIC >    - products: clean the dollar signs from prices, calculate margin_pct
# MAGIC >    - customers: join with generations table to add generation column. This is a PII table.
# MAGIC >    - staff: map location codes to store_id. This is a PII table.
# MAGIC >    - transactions: the main fact table from sales receipts, clean transaction_time
# MAGIC >    - pastry_waste: clean the percentage column
# MAGIC >    - sales_targets: only for the 3 active stores
# MAGIC >    - store_notes: an empty table for store managers to flag issues (store_id, flagged_by, flag_type, note, created_at with auto-generated identity column)
# MAGIC >
# MAGIC > 4. Add COMMENT on every table and every column with business-friendly descriptions. Genie will use these to understand the data.
# MAGIC >
# MAGIC > 5. Governance - two personas:
# MAGIC >    - coffee_hq group: sees all stores, all data
# MAGIC >    - coffee_store_mgr group: sees only store 8 (Hell's Kitchen), customer PII masked
# MAGIC >    
# MAGIC >    Create column mask functions:
# MAGIC >    - mask_name: returns full name for coffee_hq, "Masked" for others
# MAGIC >    - mask_email: returns full email for coffee_hq, "***@***.***" for others
# MAGIC >    - mask_gender: returns real value for coffee_hq, "Masked" for others
# MAGIC >    - mask_last_name: returns full name for coffee_hq, first initial + "***" for others
# MAGIC >    
# MAGIC >    Apply masks to: customers (name, email, gender) and staff (last_name)
# MAGIC >    
# MAGIC >    Create a row filter function on the transactions table: coffee_hq sees all stores, coffee_store_mgr sees only store_id = 8.
# MAGIC >
# MAGIC > 6. Gold layer in coffee_v2_gold:
# MAGIC >    - v_coffee_performance: join transactions + products + stores + customers + sales_targets + pastry_waste. Add Store Health Score (0-5) based on: target attainment >= 100%, waste < 30%, daily revenue above chain average, beverage share > 60%, unique customers > 1000. Add health_level: 0-2 = Needs Attention, 3 = Solid, 4-5 = Thriving.
# MAGIC >    - store_scorecard: aggregated per-store KPIs for the app header (monthly revenue, target attainment, waste rate, health score). This inherits the row filter from transactions.
# MAGIC >
# MAGIC > 7. Grant USE CATALOG, USE SCHEMA, SELECT on curated and gold to both groups. Grant MODIFY on store_notes to both groups. Grant EXECUTE on all mask and filter functions. Do NOT grant access to the bronze schema.
# MAGIC >
# MAGIC > Run everything and verify row counts at the end.

# COMMAND ----------

# DBTITLE 1,PROMPT 2: Genie Agent
# MAGIC %md
# MAGIC >
# MAGIC > **PROMPT 2 — paste into Assistant:**
# MAGIC >
# MAGIC > Create a Genie space called "Coffee Chain Explorer v2" using these tables:
# MAGIC > - dev_overwatch.coffee_v2_gold.v_coffee_performance (main analytics view)
# MAGIC > - dev_overwatch.coffee_v2_gold.store_scorecard (per-store KPIs)
# MAGIC > - dev_overwatch.coffee_v2_curated.products (product catalog)
# MAGIC > - dev_overwatch.coffee_v2_curated.stores (store locations)
# MAGIC >
# MAGIC > Use the Serverless Starter Warehouse.
# MAGIC >
# MAGIC > After creating it, give me the exact text to paste into the Genie space settings for:
# MAGIC >
# MAGIC > 1. General instructions - tell it:
# MAGIC >    - This is simulated data for a platform demo, never give health or financial advice
# MAGIC >    - Data covers April 2019 for a fictional coffee chain with 3 NYC stores: Astoria, Lower Manhattan, Hell's Kitchen
# MAGIC >    - Store Health Score is a business rule defined in the view (0-5 scale), explain it if asked
# MAGIC >    - Prefer percentages and averages, round to 1 decimal
# MAGIC >    - When asked about "best-selling", use total line_item_amount (revenue) not quantity
# MAGIC >    - Product groups are: Beverages, Food, Whole Bean/Teas, Merchandise, Coffee beans
# MAGIC >    - Column masks and row filters are active so results vary by user
# MAGIC >
# MAGIC > 2. These exact sample questions:
# MAGIC >    - What is the best-selling product this month?
# MAGIC >    - Which store has the highest pastry waste rate?
# MAGIC >    - Show average daily revenue by store
# MAGIC >    - What percentage of sales are beverages vs food by store?
# MAGIC >    - Which generation spends the most per visit?
# MAGIC >    - How does Hell's Kitchen compare to Astoria on target attainment?
# MAGIC >    - What are the top 5 products by revenue in Lower Manhattan?
# MAGIC >
# MAGIC > 3. Three trusted SQL queries for: average daily revenue per store, beverage percentage by store, and which store has the highest waste
# MAGIC >
# MAGIC > Also grant CAN RUN permission to both coffee_hq and coffee_store_mgr groups.

# COMMAND ----------

# DBTITLE 1,PROMPT 3: Databricks App
# MAGIC %md
# MAGIC >
# MAGIC > **PROMPT 3 — paste into Assistant:**
# MAGIC >
# MAGIC > Create a Databricks App called "coffee-explorer-v2" using the Streamlit template. This is a store manager app for a coffee chain that connects to a Genie space as its AI agent. The Genie space ID is: [PASTE_SPACE_ID_FROM_STEP_2].
# MAGIC >
# MAGIC > The app needs three panels:
# MAGIC >
# MAGIC > 1. KPI Header: Read from dev_overwatch.coffee_v2_gold.store_scorecard using the SQL Statement Execution API (the signed-in user's token). Show each store as a metric card with monthly revenue, health score, and health level. Use emojis for health status.
# MAGIC >
# MAGIC > 2. Chat Panel ("Ask your store"): Connect to the Genie space created in the previous step using the Genie Conversation API. For new questions use start_conversation_and_wait, for follow-ups use create_message_and_wait. Show the text response, a data table if there's a query result, an automatic bar chart when there's one category and one number column, and an expandable section showing the SQL that Genie generated.
# MAGIC >
# MAGIC > 3. Action Button: A "Flag Waste Issue" button that writes a row to dev_overwatch.coffee_v2_curated.store_notes with the store_id, the signed-in user's name, flag_type = 'Waste Issue', an optional note from a text input, and current_timestamp.
# MAGIC >
# MAGIC > Sidebar: Show the signed-in user's name, 5 sample question buttons, and a "New Conversation" button.
# MAGIC >
# MAGIC > Footer: "Simulated data · platform demo · not operational advice"
# MAGIC >
# MAGIC > CRITICAL auth requirements:
# MAGIC > - The app MUST act as the signed-in user (on-behalf-of-user), not as the service principal
# MAGIC > - Get the user token from st.context.headers.get("X-Forwarded-Access-Token")
# MAGIC > - The app runtime injects DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET env vars for its service principal. You MUST temporarily remove these from os.environ before creating WorkspaceClient(host=..., token=user_token), then restore them. Otherwise the SDK raises "more than one authorization method" error.
# MAGIC >
# MAGIC > In app.yaml:
# MAGIC > - Set env vars for GENIE_SPACE_ID, DATABRICKS_HOST (https://adb-xxxxxx.azuredatabricks.net), and DATABRICKS_WAREHOUSE_ID (269dxxxxxxxxx21)
# MAGIC > - Add resources: the genie space with CAN_RUN and the sql warehouse with CAN_USE
# MAGIC >
# MAGIC > requirements.txt: databricks-sdk>=0.30.0, streamlit>=1.35.0, pandas
# MAGIC >
# MAGIC > Deploy the app and give me the URL.