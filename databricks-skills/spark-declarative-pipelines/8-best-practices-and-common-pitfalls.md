# Lakeflow SDP Best Practices & Common Pitfalls

**⚠️ CRITICAL: Read this first before creating any pipeline!**

This guide addresses common mistakes that cause pipeline failures and provides production-ready patterns.

---

## Table Type Selection (MOST CRITICAL)

### When to Use Each Type

| Type | Use Case | Data Source | Updates |
|------|----------|-------------|---------|
| **MATERIALIZED VIEW** | Batch SQL transformations, aggregations | Any query (tables, views, inline SQL) | On-demand or scheduled refresh |
| **STREAMING TABLE** | Continuous processing from streaming sources | Streaming sources (Kafka, Auto Loader, CDC) | Continuous micro-batches |
| **LIVE TABLE** | Virtual views (not stored) | Any query | Computed on read |

### ✅ CORRECT Examples

```sql
-- Bronze: Read from external storage with Auto Loader (streaming)
CREATE OR REFRESH STREAMING TABLE bronze_orders AS
SELECT * FROM read_files('/path/to/files/', format => 'json');

-- Bronze: Generate synthetic data inline (materialized view)
CREATE OR REFRESH MATERIALIZED VIEW bronze_synthetic_events AS
SELECT
  CONCAT('event_', LPAD(CAST(id AS STRING), 6, '0')) AS event_id,
  TIMESTAMP_SECONDS(UNIX_TIMESTAMP() - CAST(RAND() * 2592000 AS BIGINT)) AS event_time
FROM (SELECT EXPLODE(SEQUENCE(1, 10000)) AS id);

-- Silver: Transform from Bronze (materialized view for batch, streaming table for streaming)
CREATE OR REFRESH MATERIALIZED VIEW silver_clean_orders AS
SELECT order_id, customer_id, amount, order_date
FROM bronze_orders
WHERE amount > 0;

-- Gold: Aggregate from Silver (materialized view)
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue AS
SELECT DATE(order_date) AS date, SUM(amount) AS revenue
FROM silver_clean_orders
GROUP BY DATE(order_date);
```

### ❌ WRONG Examples (Causes Failures)

```sql
-- ❌ NEVER: Circular dependency - table reading from itself
CREATE OR REFRESH STREAMING TABLE bronze_orders AS
SELECT * FROM STREAM(my_catalog.catalog.bronze_orders);  -- CIRCULAR!

-- ❌ NEVER: Reading from catalog table with STREAM() in Bronze
CREATE OR REFRESH STREAMING TABLE bronze_orders AS
SELECT * FROM STREAM(catalog.schema.existing_table);  -- Use MATERIALIZED VIEW instead

-- ❌ WRONG: Using STREAMING TABLE for batch aggregations
CREATE OR REFRESH STREAMING TABLE gold_daily_revenue AS  -- Should be MATERIALIZED VIEW
SELECT DATE(order_date) AS date, SUM(amount) AS revenue
FROM silver_clean_orders
GROUP BY DATE(order_date);
```

**RULE OF THUMB:**
- Bronze reading from files/streams → `STREAMING TABLE`
- Bronze generating data inline → `MATERIALIZED VIEW`
- Silver/Gold transformations from catalog tables → `MATERIALIZED VIEW` (unless chaining streaming)
- Streaming sources (Kafka, Auto Loader) → `STREAMING TABLE`

---

## File Organization & Glob Patterns (CRITICAL)

### ✅ CORRECT: Use Glob Patterns

**Folder Structure:**
```
/Workspace/Users/user@example.com/my_pipeline/
├── transformations/           # Use this exact name for clarity
│   ├── bronze/
│   │   ├── bronze_orders.sql
│   │   └── bronze_events.sql
│   ├── silver/
│   │   ├── silver_clean_orders.sql
│   │   └── silver_clean_events.sql
│   └── gold/
│       ├── gold_daily_revenue.sql
│       └── gold_customer_segments.sql
```

**Pipeline Configuration (PREFERRED):**
```python
create_or_update_pipeline(
    name="my_pipeline",
    root_path="/Workspace/Users/user@example.com/my_pipeline",
    catalog="my_catalog",
    schema="my_schema",
    workspace_file_paths=[],  # Leave empty when using glob
    extra_settings={
        "libraries": [
            {"glob": {"include": "/transformations/**/*.sql"}}  # Auto-discovers all SQL files
        ]
    }
)
```

**Benefits of Glob:**
- Auto-discovers all `.sql` files in `transformations/` folder
- No need to list files individually
- Easy to add new tables - just drop a file in the folder
- Maintains clean separation between layers

### ❌ WRONG: Individual File References

```python
# ❌ AVOID: Manually listing every file
workspace_file_paths=[
    "/Workspace/.../pipeline_bronze.sql",
    "/Workspace/.../pipeline_silver.sql",
    "/Workspace/.../pipeline_gold.sql"
]
```

**Problems:**
- Must update pipeline config every time you add a file
- No visual separation between layers
- Harder to maintain

---

## Inline Synthetic Data Generation (New Pattern)

### ✅ CORRECT: Generate Data in SQL (No Python Scripts!)

```sql
-- Bronze: Generate 5,000 synthetic events
CREATE OR REFRESH MATERIALIZED VIEW bronze_events AS
SELECT
  CONCAT('event_', LPAD(CAST(id AS STRING), 6, '0')) AS event_id,
  CONCAT('entity_', LPAD(CAST(MOD(id, 500) + 1 AS STRING), 4, '0')) AS entity_id,
  TIMESTAMP_SECONDS(
    UNIX_TIMESTAMP(CURRENT_DATE()) - (30 * 24 * 3600) +
    CAST(RAND() * 30 * 24 * 3600 AS BIGINT)
  ) AS event_timestamp,
  CASE
    WHEN RAND() < 0.5 THEN 'success'
    ELSE 'failure'
  END AS event_status,
  CASE
    WHEN MOD(id, 4) = 0 THEN 'category_a'
    WHEN MOD(id, 4) = 1 THEN 'category_b'
    WHEN MOD(id, 4) = 2 THEN 'category_c'
    ELSE 'category_d'
  END AS category,
  CAST(RAND() * 100 + 1 AS INT) AS priority_score
FROM (
  SELECT EXPLODE(SEQUENCE(1, 5000)) AS id  -- Generate 5,000 rows
);

-- Bronze: Generate transaction data with correlated amounts
CREATE OR REFRESH MATERIALIZED VIEW bronze_transactions AS
SELECT
  CONCAT('txn_', LPAD(CAST(id AS STRING), 7, '0')) AS transaction_id,
  CONCAT('entity_', LPAD(CAST(MOD(id, 500) + 1 AS STRING), 4, '0')) AS entity_id,
  TIMESTAMP_SECONDS(
    UNIX_TIMESTAMP(CURRENT_DATE()) - CAST(RAND() * 30 * 24 * 3600 AS BIGINT)
  ) AS transaction_timestamp,
  CASE
    WHEN MOD(id, 3) = 0 THEN 9.99
    WHEN MOD(id, 3) = 1 THEN 19.99
    ELSE 4.99
  END AS amount,
  CASE
    WHEN MOD(id, 3) = 0 THEN 'type_a'
    WHEN MOD(id, 3) = 1 THEN 'type_b'
    ELSE 'type_c'
  END AS transaction_type
FROM (
  SELECT EXPLODE(SEQUENCE(1, 5000)) AS id
);
```

**Inline Generation Techniques:**
- `SEQUENCE(1, N)` + `EXPLODE()` → Generate N rows
- `RAND()` → Random float between 0 and 1
- `MOD(id, N)` → Distribute values across N categories evenly
- `TIMESTAMP_SECONDS(UNIX_TIMESTAMP() - CAST(RAND() * seconds AS BIGINT))` → Random timestamps
- `CONCAT()` + `LPAD()` → Generate formatted IDs

### ❌ WRONG: Separate Python Script + Table References

```python
# ❌ AVOID: Generating data with Python script, then referencing in pipeline
# Step 1: Run Python script to create bronze tables
run_python_file_on_databricks("generate_bronze_data.py")

# Step 2: Pipeline tries to read those tables (CAUSES CIRCULAR DEPENDENCY!)
CREATE OR REFRESH STREAMING TABLE bronze_events AS
SELECT * FROM STREAM(my_catalog.my_schema.bronze_events);  -- CIRCULAR!
```

**Why This Fails:**
- Pipeline can't reference tables from the same catalog/schema it's writing to in Bronze
- Creates circular dependency
- Harder to deploy - requires running separate scripts

---

## Avoiding Circular Dependencies

### ❌ NEVER Do This

```sql
-- ❌ Table reading from itself
CREATE OR REFRESH STREAMING TABLE bronze_orders AS
SELECT * FROM STREAM(catalog.schema.bronze_orders);

-- ❌ Table reading from a table it's about to overwrite
CREATE OR REFRESH MATERIALIZED VIEW silver_orders AS
SELECT * FROM catalog.schema.silver_orders WHERE date > '2024-01-01';
```

### ✅ DO This Instead

```sql
-- ✅ Bronze: Read from external source
CREATE OR REFRESH STREAMING TABLE bronze_orders AS
SELECT * FROM read_files('/external/path/', format => 'json');

-- ✅ Bronze: Generate data inline
CREATE OR REFRESH MATERIALIZED VIEW bronze_synthetic AS
SELECT ... FROM (SELECT EXPLODE(SEQUENCE(1, N)) AS id);

-- ✅ Silver: Read from Bronze layer
CREATE OR REFRESH MATERIALIZED VIEW silver_orders AS
SELECT * FROM bronze_orders WHERE amount > 0;

-- ✅ Gold: Read from Silver layer
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_summary AS
SELECT DATE(order_date) AS date, SUM(amount) AS revenue
FROM silver_orders
GROUP BY DATE(order_date);
```

**Rules:**
1. Bronze tables should ONLY read from:
   - External files (`read_files()`, `cloud_files()`)
   - Streaming sources (Kafka, Kinesis)
   - Inline generated data (SEQUENCE, RAND)
2. Silver tables read from Bronze
3. Gold tables read from Silver (or Bronze)
4. NEVER read from a table in the same layer

---

## Complete Working Example: Data Analytics Pipeline

**Folder Structure:**
```
/Workspace/Users/user@example.com/example_pipeline/
└── transformations/
    ├── bronze/
    │   ├── bronze_events.sql
    │   └── bronze_transactions.sql
    ├── silver/
    │   └── silver_entity_metrics.sql
    └── gold/
        ├── gold_daily_summary.sql
        └── gold_entity_segments.sql
```

**bronze/bronze_events.sql:**
```sql
CREATE OR REFRESH MATERIALIZED VIEW bronze_events AS
SELECT
  CONCAT('event_', LPAD(CAST(id AS STRING), 6, '0')) AS event_id,
  CONCAT('entity_', LPAD(CAST(MOD(id, 500) + 1 AS STRING), 4, '0')) AS entity_id,
  TIMESTAMP_SECONDS(
    UNIX_TIMESTAMP(CURRENT_DATE()) - (30 * 24 * 3600) +
    CAST(RAND() * 30 * 24 * 3600 AS BIGINT)
  ) AS event_timestamp,
  CASE WHEN RAND() < 0.5 THEN 'success' ELSE 'failure' END AS event_status,
  CAST(RAND() * 100 AS INT) AS metric_value,
  CAST(RAND() * 300 + 180 AS INT) AS duration_seconds,
  CASE
    WHEN MOD(id, 4) = 0 THEN 'category_a'
    WHEN MOD(id, 4) = 1 THEN 'category_b'
    WHEN MOD(id, 4) = 2 THEN 'category_c'
    ELSE 'category_d'
  END AS category,
  CAST(RAND() * 100 + 1 AS INT) AS priority_score
FROM (SELECT EXPLODE(SEQUENCE(1, 5000)) AS id);
```

**bronze/bronze_transactions.sql:**
```sql
CREATE OR REFRESH MATERIALIZED VIEW bronze_transactions AS
SELECT
  CONCAT('txn_', LPAD(CAST(id AS STRING), 7, '0')) AS transaction_id,
  CONCAT('entity_', LPAD(CAST(MOD(id, 500) + 1 AS STRING), 4, '0')) AS entity_id,
  TIMESTAMP_SECONDS(
    UNIX_TIMESTAMP(CURRENT_DATE()) - CAST(RAND() * 30 * 24 * 3600 AS BIGINT)
  ) AS transaction_timestamp,
  CASE
    WHEN MOD(id, 3) = 0 THEN 9.99
    WHEN MOD(id, 3) = 1 THEN 19.99
    ELSE 4.99
  END AS amount,
  CASE
    WHEN MOD(id, 3) = 0 THEN 'type_a'
    WHEN MOD(id, 3) = 1 THEN 'type_b'
    ELSE 'type_c'
  END AS transaction_type
FROM (SELECT EXPLODE(SEQUENCE(1, 5000)) AS id);
```

**silver/silver_entity_metrics.sql:**
```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_entity_metrics AS
WITH event_stats AS (
  SELECT
    entity_id,
    COUNT(*) AS total_events,
    SUM(CASE WHEN event_status = 'success' THEN 1 ELSE 0 END) AS successful_events,
    AVG(duration_seconds) AS avg_duration,
    MAX(event_timestamp) AS last_event_date,
    COUNT(DISTINCT DATE(event_timestamp)) AS days_active,
    MAX(priority_score) AS max_priority_score,
    MODE(category) AS primary_category
  FROM bronze_events
  GROUP BY entity_id
),
transaction_stats AS (
  SELECT
    entity_id,
    SUM(amount) AS total_amount
  FROM bronze_transactions
  GROUP BY entity_id
)
SELECT
  COALESCE(e.entity_id, t.entity_id) AS entity_id,
  COALESCE(e.total_events, 0) AS total_events,
  CASE
    WHEN e.total_events > 0 THEN ROUND(e.successful_events * 100.0 / e.total_events, 2)
    ELSE 0.0
  END AS success_rate,
  COALESCE(t.total_amount, 0.0) AS total_amount,
  COALESCE(e.days_active, 0) AS days_active,
  COALESCE(e.avg_duration, 0.0) AS avg_duration,
  COALESCE(e.primary_category, 'Unknown') AS primary_category,
  COALESCE(e.max_priority_score, 0) AS max_priority_score,
  e.last_event_date
FROM event_stats e
FULL OUTER JOIN transaction_stats t ON e.entity_id = t.entity_id;
```

**gold/gold_entity_segments.sql:**
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_entity_segments AS
SELECT
  entity_id,
  CASE
    WHEN total_amount > 50 AND total_events > 20 THEN 'High_Value'
    WHEN total_amount = 0 AND total_events > 30 THEN 'Engaged_Free'
    WHEN total_amount > 0 AND total_events < 20 THEN 'Low_Engagement'
    WHEN DATEDIFF(CURRENT_DATE(), DATE(last_event_date)) > 7 THEN 'At_Risk'
    ELSE 'Other'
  END AS segment_name,
  total_events,
  total_amount,
  success_rate,
  DATEDIFF(CURRENT_DATE(), DATE(last_event_date)) AS days_since_last_event
FROM silver_entity_metrics
WHERE entity_id IS NOT NULL
ORDER BY total_amount DESC, total_events DESC;
```

**Pipeline Creation (Python using MCP):**
```python
# Step 1: Create SQL files locally in transformations/ folder structure
# Step 2: Upload to Databricks workspace
upload_folder(
    local_folder="/path/to/example_pipeline",
    workspace_folder="/Workspace/Users/user@example.com/example_pipeline"
)

# Step 3: Create pipeline with glob pattern
create_or_update_pipeline(
    name="Data Analytics Pipeline",
    root_path="/Workspace/Users/user@example.com/example_pipeline",
    catalog="my_catalog",
    schema="my_schema",
    workspace_file_paths=[],  # Empty - using glob instead
    start_run=True,
    wait_for_completion=True,
    full_refresh=True,
    extra_settings={
        "development": False,  # Production mode
        "continuous": False,   # Batch mode
        "serverless": True,    # Use serverless compute
        "libraries": [
            {"glob": {"include": "/transformations/**/*.sql"}}  # Auto-discover all SQL files
        ]
    }
)
```

---

## Troubleshooting: Glob Patterns vs Explicit File Paths

### ⚠️ Known Issue: Glob Pattern Syntax Errors

When using **MCP tools** or certain **API clients**, glob patterns may fail with errors:
- `Special characters *?\ are reserved and should not be used`
- `Path doesn't start with '/'`
- `LIBRARY_FILE_FETCH_INTERNAL_ERROR`

**Root Cause:** Glob pattern syntax in `extra_settings["libraries"]` may not be fully supported by all API clients, even though it works in Databricks UI.

### ✅ Solution 1: Try Different Glob Syntax

```python
# Try these variations in order:
extra_settings={
    "libraries": [
        {"glob": {"include": "transformations/**"}}  # Without leading /
    ]
}

# Or absolute path from root:
extra_settings={
    "libraries": [
        {"glob": {"include": "/transformations/**/*.sql"}}  # With extension
    ]
}
```

### ✅ Solution 2: Use Explicit File Paths (Fallback)

If glob patterns continue to fail, use explicit `workspace_file_paths`:

```python
workspace_file_paths=[
    "/Workspace/Users/user@example.com/my_pipeline/transformations/bronze/table1.sql",
    "/Workspace/Users/user@example.com/my_pipeline/transformations/bronze/table2.sql",
    "/Workspace/Users/user@example.com/my_pipeline/transformations/silver/table3.sql",
    "/Workspace/Users/user@example.com/my_pipeline/transformations/gold/table4.sql"
]
```

**Trade-offs:**
- ✅ Works reliably across all tools
- ❌ Must update list when adding files
- ✅ Explicit about what's included

### ⚠️ Clean Up Tables Before Re-running Failed Pipelines

If a pipeline fails mid-execution, tables may be partially created. Drop them before re-running:

```sql
DROP TABLE IF EXISTS catalog.schema.bronze_table1;
DROP TABLE IF EXISTS catalog.schema.bronze_table2;
DROP TABLE IF EXISTS catalog.schema.silver_table1;
-- etc.
```

**Error if not cleaned:** `Could not materialize because a MANAGED table already exists with that name`

---

## Quick Checklist Before Creating Pipeline

- [ ] Using glob pattern OR explicit file paths (try glob first, fall back to explicit if errors)
- [ ] Folder structure: `transformations/bronze/`, `transformations/silver/`, `transformations/gold/`
- [ ] One table per file: `{table_name}.sql`
- [ ] Bronze tables use `MATERIALIZED VIEW` for inline data OR `STREAMING TABLE` for file sources
- [ ] Silver/Gold use `MATERIALIZED VIEW` for batch transformations
- [ ] NO circular dependencies (tables reading from themselves)
- [ ] Inline data generation uses `SEQUENCE`, `EXPLODE`, `RAND()`
- [ ] NO external Python scripts for data generation in Bronze
- [ ] **Dropped existing tables if re-running after failure**

---

## Summary of Key Changes

| What I Did Wrong | What to Do Instead |
|------------------|-------------------|
| Used individual file paths | Use glob pattern: `{"glob": {"include": "/**"}}` |
| Put all SQL in 3 files | One file per table in layer folders |
| Used STREAMING TABLE everywhere | MATERIALIZED VIEW for batch, STREAMING for streams |
| Tried to read from same table | Generate inline with SEQUENCE/RAND |
| Ran Python script separately | Generate data inline using SQL |
| Created circular dependencies | Bronze reads external/inline, Silver reads Bronze, Gold reads Silver |

**This approach ensures:**
✅ No circular dependencies
✅ Clean folder organization
✅ Easy to maintain and extend
✅ Works in one go without failures
✅ Production-ready from day one
