import streamlit as st
import json
import random
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# LTK Data Analyst Technical Assessment - Study & Practice App
# Simulates a Coderbyte-style assessment: ETL, SQL, MySQL, Coding
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="LTK Data Analyst Assessment Prep",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .correct { background-color: #d4edda; padding: 10px; border-radius: 5px; border-left: 4px solid #28a745; margin: 5px 0; }
    .incorrect { background-color: #f8d7da; padding: 10px; border-radius: 5px; border-left: 4px solid #dc3545; margin: 5px 0; }
    .hint { background-color: #fff3cd; padding: 10px; border-radius: 5px; border-left: 4px solid #ffc107; margin: 5px 0; }
    .info-box { background-color: #d1ecf1; padding: 15px; border-radius: 5px; border-left: 4px solid #17a2b8; margin: 10px 0; }
    .section-header { border-bottom: 2px solid #007bff; padding-bottom: 5px; margin-bottom: 15px; }
    .score-box { background-color: #e8f5e9; padding: 20px; border-radius: 10px; text-align: center; font-size: 24px; }
    pre { background-color: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; overflow-x: auto; }
    code { color: #e83e8c; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA: All questions organized by section
# ═══════════════════════════════════════════════════════════════════════════════

ETL_QUESTIONS = [
    {
        "id": "etl_1",
        "question": "What does ETL stand for?",
        "options": [
            "Extract, Transform, Load",
            "Export, Transfer, Log",
            "Extract, Transfer, Load",
            "Export, Transform, Link",
        ],
        "answer": 0,
        "explanation": "ETL stands for Extract, Transform, Load — the process of extracting data from source systems, transforming it into a usable format, and loading it into a data warehouse or target system.",
    },
    {
        "id": "etl_2",
        "question": "In an ETL pipeline, which step is responsible for cleaning, deduplicating, and standardizing data?",
        "options": [
            "Extract",
            "Transform",
            "Load",
            "Validate",
        ],
        "answer": 1,
        "explanation": "The Transform step handles data cleaning, deduplication, standardization, aggregation, and any business logic before loading.",
    },
    {
        "id": "etl_3",
        "question": "What is the key difference between ETL and ELT?",
        "options": [
            "ELT loads raw data first and transforms in the warehouse; ETL transforms before loading",
            "ETL is faster than ELT",
            "ELT can only work with NoSQL databases",
            "There is no difference, they are the same",
        ],
        "answer": 0,
        "explanation": "In ELT, raw data is loaded into the data warehouse first, then transformations are performed using the warehouse's compute power. This is common with modern cloud warehouses like BigQuery, Snowflake, and Redshift.",
    },
    {
        "id": "etl_4",
        "question": "Which of the following is a common challenge during the 'Extract' phase of ETL?",
        "options": [
            "Data type mismatches between source and target",
            "Handling schema changes in source systems",
            "Slow dashboard rendering",
            "User authentication issues in BI tools",
        ],
        "answer": 1,
        "explanation": "Schema changes (new columns, renamed fields, type changes) in source systems are a common extraction challenge that can break pipelines.",
    },
    {
        "id": "etl_5",
        "question": "What is 'incremental loading' in the context of ETL?",
        "options": [
            "Loading all data from scratch every time",
            "Loading only new or changed data since the last extraction",
            "Loading data in alphabetical order",
            "Loading data into multiple tables simultaneously",
        ],
        "answer": 1,
        "explanation": "Incremental loading only processes new or modified records (often using timestamps or change flags), making the pipeline more efficient than full reloads.",
    },
    {
        "id": "etl_6",
        "question": "What is a 'staging area' in an ETL pipeline?",
        "options": [
            "The final production database",
            "A temporary storage area where raw extracted data is held before transformation",
            "A backup of the source system",
            "The BI dashboard layer",
        ],
        "answer": 1,
        "explanation": "A staging area is intermediate storage used to hold raw extracted data before transformations are applied. It helps decouple extraction from transformation.",
    },
    {
        "id": "etl_7",
        "question": "Which of the following best describes a 'data warehouse'?",
        "options": [
            "A transactional database optimized for INSERT/UPDATE operations",
            "A centralized repository optimized for analytical queries and reporting",
            "A real-time streaming platform",
            "A file storage system like S3",
        ],
        "answer": 1,
        "explanation": "A data warehouse is a centralized repository designed for analytical queries, reporting, and BI. It's optimized for read-heavy, complex queries (OLAP) rather than transactional workloads (OLTP).",
    },
    {
        "id": "etl_8",
        "question": "In ETL, what is a 'slowly changing dimension' (SCD)?",
        "options": [
            "A dimension table that rarely gets new rows",
            "A technique for tracking historical changes in dimension attributes over time",
            "A table that only changes once a year",
            "A fact table with slowly increasing row counts",
        ],
        "answer": 1,
        "explanation": "SCD is a methodology for managing and tracking changes in dimension data over time. SCD Type 1 overwrites old values, Type 2 creates new rows with version history, and Type 3 adds columns for previous values.",
    },
    {
        "id": "etl_9",
        "question": "What is data lineage?",
        "options": [
            "The order in which data was entered",
            "The ability to trace data from its origin through all transformations to its final destination",
            "The relationship between primary and foreign keys",
            "The versioning of database schemas",
        ],
        "answer": 1,
        "explanation": "Data lineage tracks the flow of data from source to destination, including all transformations, aggregations, and joins applied. It's critical for debugging, compliance, and trust in data.",
    },
    {
        "id": "etl_10",
        "question": "Which tool is commonly used for orchestrating ETL/ELT workflows?",
        "options": [
            "Tableau",
            "Apache Airflow",
            "Microsoft Word",
            "Git",
        ],
        "answer": 1,
        "explanation": "Apache Airflow is a widely-used workflow orchestration tool for scheduling, monitoring, and managing complex data pipelines. Other tools include dbt, Prefect, Dagster, and Luigi.",
    },
    {
        "id": "etl_11",
        "question": "What is idempotency in the context of ETL pipelines?",
        "options": [
            "The ability to run a pipeline multiple times and get the same result",
            "The speed of data processing",
            "The number of data sources a pipeline can handle",
            "The ability to process data in parallel",
        ],
        "answer": 0,
        "explanation": "An idempotent pipeline produces the same output regardless of how many times it's run with the same input. This is crucial for reliability — if a pipeline fails midway and is retried, idempotency ensures no duplicate or corrupted data.",
    },
    {
        "id": "etl_12",
        "question": "What does MERGE (UPSERT) do in the context of ETL loading?",
        "options": [
            "Deletes all existing data before inserting new data",
            "Inserts new records and updates existing records in a single operation",
            "Only inserts new records, ignoring existing ones",
            "Creates a new table for each load",
        ],
        "answer": 1,
        "explanation": "MERGE (or UPSERT) combines INSERT and UPDATE logic: if a record already exists (matched by key), it updates it; if it's new, it inserts it. This is fundamental for incremental loading in ETL.",
    },
]

SQL_QUESTIONS = [
    {
        "id": "sql_1",
        "question": "Write a SQL query to find the top 5 creators by total revenue generated in the last 30 days.",
        "context": """Tables:
- creators (creator_id, name, category, join_date)
- transactions (transaction_id, creator_id, amount, transaction_date, status)

Only consider transactions with status = 'completed'.""",
        "hint": "Use JOIN, WHERE for date filter and status, GROUP BY, ORDER BY, LIMIT",
        "solution": """SELECT
    c.creator_id,
    c.name,
    SUM(t.amount) AS total_revenue
FROM creators c
JOIN transactions t ON c.creator_id = t.creator_id
WHERE t.status = 'completed'
  AND t.transaction_date >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY c.creator_id, c.name
ORDER BY total_revenue DESC
LIMIT 5;""",
        "key_concepts": ["JOIN", "WHERE with date filter", "GROUP BY", "ORDER BY DESC", "LIMIT", "SUM aggregation"],
    },
    {
        "id": "sql_2",
        "question": "Write a query to calculate the month-over-month revenue growth rate.",
        "context": """Table:
- transactions (transaction_id, amount, transaction_date, status)

Show each month's revenue and the percentage change from the previous month.
Only include completed transactions.""",
        "hint": "Use DATE_FORMAT or EXTRACT for month, LAG() window function for previous month comparison",
        "solution": """WITH monthly_revenue AS (
    SELECT
        DATE_FORMAT(transaction_date, '%Y-%m') AS month,
        SUM(amount) AS revenue
    FROM transactions
    WHERE status = 'completed'
    GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY month))
        / LAG(revenue) OVER (ORDER BY month) * 100,
        2
    ) AS growth_rate_pct
FROM monthly_revenue
ORDER BY month;""",
        "key_concepts": ["CTE (WITH clause)", "DATE_FORMAT", "LAG() window function", "Percentage calculation", "SUM + GROUP BY"],
    },
    {
        "id": "sql_3",
        "question": "Write a query to build a conversion funnel showing how many users go from 'page_view' → 'add_to_cart' → 'purchase'.",
        "context": """Table:
- events (event_id, user_id, event_type, event_timestamp)

event_type values: 'page_view', 'add_to_cart', 'purchase'
Count distinct users at each stage.""",
        "hint": "Use COUNT(DISTINCT) with CASE WHEN or separate subqueries for each funnel step",
        "solution": """SELECT
    COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) AS page_views,
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS add_to_cart,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchases,
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END), 0),
        2
    ) AS view_to_cart_pct,
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END), 0),
        2
    ) AS cart_to_purchase_pct
FROM events;""",
        "key_concepts": ["COUNT(DISTINCT CASE WHEN ...)", "NULLIF to avoid division by zero", "Conversion rates", "Funnel analysis"],
    },
    {
        "id": "sql_4",
        "question": "Find creators who have been active (made at least one sale) every month for the past 6 months.",
        "context": """Tables:
- creators (creator_id, name, category, join_date)
- transactions (transaction_id, creator_id, amount, transaction_date, status)""",
        "hint": "Use COUNT(DISTINCT month) in a HAVING clause",
        "solution": """SELECT
    c.creator_id,
    c.name,
    COUNT(DISTINCT DATE_FORMAT(t.transaction_date, '%Y-%m')) AS active_months
FROM creators c
JOIN transactions t ON c.creator_id = t.creator_id
WHERE t.status = 'completed'
  AND t.transaction_date >= CURRENT_DATE - INTERVAL 6 MONTH
GROUP BY c.creator_id, c.name
HAVING COUNT(DISTINCT DATE_FORMAT(t.transaction_date, '%Y-%m')) = 6;""",
        "key_concepts": ["HAVING clause", "COUNT(DISTINCT) on derived values", "DATE_FORMAT", "INTERVAL for date math"],
    },
    {
        "id": "sql_5",
        "question": "Write a query to find the running total of revenue per creator, ordered by transaction date.",
        "context": """Table:
- transactions (transaction_id, creator_id, amount, transaction_date, status)""",
        "hint": "Use SUM() as a window function with PARTITION BY and ORDER BY",
        "solution": """SELECT
    creator_id,
    transaction_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY creator_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM transactions
WHERE status = 'completed'
ORDER BY creator_id, transaction_date;""",
        "key_concepts": ["Window function SUM() OVER", "PARTITION BY", "ORDER BY in window", "ROWS BETWEEN ... AND CURRENT ROW"],
    },
    {
        "id": "sql_6",
        "question": "Write a query to find duplicate transactions (same creator_id, amount, and transaction_date).",
        "context": """Table:
- transactions (transaction_id, creator_id, amount, transaction_date, status)

Return all duplicate groups with their count.""",
        "hint": "Use GROUP BY and HAVING COUNT(*) > 1",
        "solution": """SELECT
    creator_id,
    amount,
    transaction_date,
    COUNT(*) AS duplicate_count
FROM transactions
GROUP BY creator_id, amount, transaction_date
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;""",
        "key_concepts": ["GROUP BY", "HAVING COUNT(*) > 1", "Duplicate detection", "Data quality check"],
    },
    {
        "id": "sql_7",
        "question": "Calculate the retention rate: what percentage of creators who made their first sale in January 2026 also made a sale in February 2026?",
        "context": """Table:
- transactions (transaction_id, creator_id, amount, transaction_date, status)""",
        "hint": "Use a CTE to find first-month creators, then LEFT JOIN to find who returned in the second month",
        "solution": """WITH jan_creators AS (
    SELECT DISTINCT creator_id
    FROM transactions
    WHERE status = 'completed'
      AND DATE_FORMAT(transaction_date, '%Y-%m') = '2026-01'
      AND creator_id NOT IN (
          SELECT DISTINCT creator_id
          FROM transactions
          WHERE status = 'completed'
            AND transaction_date < '2026-01-01'
      )
),
feb_activity AS (
    SELECT DISTINCT creator_id
    FROM transactions
    WHERE status = 'completed'
      AND DATE_FORMAT(transaction_date, '%Y-%m') = '2026-02'
)
SELECT
    COUNT(j.creator_id) AS jan_cohort_size,
    COUNT(f.creator_id) AS retained_in_feb,
    ROUND(COUNT(f.creator_id) * 100.0 / COUNT(j.creator_id), 2) AS retention_rate_pct
FROM jan_creators j
LEFT JOIN feb_activity f ON j.creator_id = f.creator_id;""",
        "key_concepts": ["Cohort analysis", "LEFT JOIN for retention", "Multiple CTEs", "NOT IN subquery", "Retention rate calculation"],
    },
    {
        "id": "sql_8",
        "question": "Rank creators within each category by their total revenue. Show the top 3 per category.",
        "context": """Tables:
- creators (creator_id, name, category)
- transactions (transaction_id, creator_id, amount, transaction_date, status)""",
        "hint": "Use ROW_NUMBER() or DENSE_RANK() with PARTITION BY category",
        "solution": """WITH creator_revenue AS (
    SELECT
        c.creator_id,
        c.name,
        c.category,
        SUM(t.amount) AS total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY c.category
            ORDER BY SUM(t.amount) DESC
        ) AS rank_in_category
    FROM creators c
    JOIN transactions t ON c.creator_id = t.creator_id
    WHERE t.status = 'completed'
    GROUP BY c.creator_id, c.name, c.category
)
SELECT *
FROM creator_revenue
WHERE rank_in_category <= 3
ORDER BY category, rank_in_category;""",
        "key_concepts": ["ROW_NUMBER() / DENSE_RANK()", "PARTITION BY", "Filtering window results with outer WHERE", "CTE pattern for ranked queries"],
    },
    {
        "id": "sql_9",
        "question": "Write a query to calculate the average time between a creator's first and second transaction.",
        "context": """Table:
- transactions (transaction_id, creator_id, transaction_date, status)""",
        "hint": "Use ROW_NUMBER() to identify the 1st and 2nd transactions, then use DATEDIFF",
        "solution": """WITH ranked_txns AS (
    SELECT
        creator_id,
        transaction_date,
        ROW_NUMBER() OVER (
            PARTITION BY creator_id
            ORDER BY transaction_date
        ) AS txn_rank
    FROM transactions
    WHERE status = 'completed'
),
time_to_second AS (
    SELECT
        r1.creator_id,
        DATEDIFF(r2.transaction_date, r1.transaction_date) AS days_to_second
    FROM ranked_txns r1
    JOIN ranked_txns r2
        ON r1.creator_id = r2.creator_id
       AND r1.txn_rank = 1
       AND r2.txn_rank = 2
)
SELECT
    ROUND(AVG(days_to_second), 1) AS avg_days_to_second_txn,
    MIN(days_to_second) AS min_days,
    MAX(days_to_second) AS max_days
FROM time_to_second;""",
        "key_concepts": ["ROW_NUMBER() for ordering", "Self-JOIN on ranked data", "DATEDIFF", "AVG/MIN/MAX aggregation"],
    },
    {
        "id": "sql_10",
        "question": "Write a query to identify creators whose revenue dropped more than 50% from one month to the next.",
        "context": """Tables:
- creators (creator_id, name)
- transactions (transaction_id, creator_id, amount, transaction_date, status)""",
        "hint": "Use LAG() window function to compare current month with previous month per creator",
        "solution": """WITH monthly AS (
    SELECT
        creator_id,
        DATE_FORMAT(transaction_date, '%Y-%m') AS month,
        SUM(amount) AS revenue
    FROM transactions
    WHERE status = 'completed'
    GROUP BY creator_id, DATE_FORMAT(transaction_date, '%Y-%m')
),
with_prev AS (
    SELECT
        creator_id,
        month,
        revenue,
        LAG(revenue) OVER (PARTITION BY creator_id ORDER BY month) AS prev_revenue
    FROM monthly
)
SELECT
    w.creator_id,
    c.name,
    w.month,
    w.revenue,
    w.prev_revenue,
    ROUND((w.prev_revenue - w.revenue) / w.prev_revenue * 100, 2) AS drop_pct
FROM with_prev w
JOIN creators c ON w.creator_id = c.creator_id
WHERE w.prev_revenue IS NOT NULL
  AND w.revenue < w.prev_revenue * 0.5
ORDER BY drop_pct DESC;""",
        "key_concepts": ["LAG() with PARTITION BY", "Month-over-month comparison", "Percentage drop calculation", "Filtering on window results"],
    },
]

MYSQL_QUESTIONS = [
    {
        "id": "mysql_1",
        "question": "What is the difference between CHAR and VARCHAR in MySQL?",
        "options": [
            "CHAR is fixed-length and pads with spaces; VARCHAR is variable-length and only uses needed space",
            "CHAR is for numbers, VARCHAR is for text",
            "VARCHAR is faster than CHAR in all cases",
            "There is no difference, they are aliases",
        ],
        "answer": 0,
        "explanation": "CHAR(n) always stores exactly n characters (right-padded with spaces). VARCHAR(n) stores only the actual characters plus 1-2 bytes for length. CHAR is faster for fixed-width data (e.g., country codes), VARCHAR is better for variable-length data.",
    },
    {
        "id": "mysql_2",
        "question": "What does the MySQL function IFNULL(expr1, expr2) do?",
        "options": [
            "Returns expr1 if it is NOT NULL, otherwise returns expr2",
            "Returns NULL if both expressions are NULL",
            "Checks if a column exists in a table",
            "Converts NULL to an empty string",
        ],
        "answer": 0,
        "explanation": "IFNULL(expr1, expr2) returns expr1 if it's not NULL, otherwise returns expr2. It's MySQL's shorthand for COALESCE with two arguments. Example: IFNULL(revenue, 0) returns 0 when revenue is NULL.",
    },
    {
        "id": "mysql_3",
        "question": "In MySQL, what is the difference between DELETE, TRUNCATE, and DROP?",
        "options": [
            "They all do the same thing",
            "DELETE removes rows (can be filtered, logged); TRUNCATE removes all rows (faster, minimal logging); DROP removes the entire table",
            "TRUNCATE is the same as DELETE WHERE 1=1",
            "DROP only removes the table structure, not the data",
        ],
        "answer": 1,
        "explanation": "DELETE: removes specific rows, supports WHERE, logs each row, can be rolled back. TRUNCATE: removes ALL rows, resets auto-increment, minimal logging, faster. DROP: removes the entire table (structure + data).",
    },
    {
        "id": "mysql_4",
        "question": "What is a MySQL INDEX and when should you use one?",
        "options": [
            "An index is a backup copy of a table",
            "An index speeds up data retrieval by creating a data structure (B-Tree) that allows faster lookups on indexed columns",
            "An index is required on every column",
            "Indexes only work on VARCHAR columns",
        ],
        "answer": 1,
        "explanation": "Indexes create B-Tree (or Hash) structures for faster lookups. Use them on columns frequently used in WHERE, JOIN, ORDER BY. Trade-off: they speed up reads but slow down writes (INSERT/UPDATE/DELETE) and use storage.",
    },
    {
        "id": "mysql_5",
        "question": "What is the difference between INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN in MySQL?",
        "options": [
            "They all return the same results",
            "INNER: matching rows only; LEFT: all from left + matches from right; RIGHT: all from right + matches from left; FULL OUTER: all rows from both (MySQL uses UNION for this)",
            "LEFT JOIN is faster than INNER JOIN",
            "RIGHT JOIN returns no NULL values",
        ],
        "answer": 1,
        "explanation": "INNER JOIN: only rows with matches in both tables. LEFT JOIN: all rows from left table + matching from right (NULLs where no match). RIGHT JOIN: inverse. MySQL doesn't natively support FULL OUTER JOIN — you simulate it with LEFT JOIN UNION RIGHT JOIN.",
    },
    {
        "id": "mysql_6",
        "question": "What is the MySQL GROUP_CONCAT() function used for?",
        "options": [
            "Concatenates all values in a group into a single string",
            "Groups tables together",
            "Concatenates two columns",
            "Counts the number of groups",
        ],
        "answer": 0,
        "explanation": "GROUP_CONCAT() concatenates values from multiple rows into a single string within each group. Example: SELECT category, GROUP_CONCAT(name SEPARATOR ', ') FROM creators GROUP BY category; returns all creator names per category as a comma-separated list.",
    },
    {
        "id": "mysql_7",
        "question": "What does EXPLAIN do in MySQL?",
        "options": [
            "Runs the query and shows the results",
            "Shows the execution plan for a query, including which indexes are used and the estimated row counts",
            "Explains the table structure (same as DESCRIBE)",
            "Exports the query results to a file",
        ],
        "answer": 1,
        "explanation": "EXPLAIN prefixed before a SELECT shows the query execution plan: which indexes are used, join types, estimated rows scanned, and order of operations. Essential for query optimization.",
    },
    {
        "id": "mysql_8",
        "question": "What is a MySQL VIEW?",
        "options": [
            "A physical copy of a table",
            "A stored virtual table based on a SQL query that can be queried like a regular table",
            "A type of index",
            "A backup of the database",
        ],
        "answer": 1,
        "explanation": "A VIEW is a virtual table defined by a SQL query. It doesn't store data itself but provides a named, reusable query interface. Views simplify complex queries, add security layers, and abstract underlying table structures.",
    },
    {
        "id": "mysql_9",
        "question": "How do you handle NULL values when comparing in MySQL?",
        "options": [
            "Use = NULL and != NULL",
            "Use IS NULL and IS NOT NULL (because NULL = NULL evaluates to NULL, not TRUE)",
            "NULLs are automatically converted to 0",
            "NULL comparisons are not supported in MySQL",
        ],
        "answer": 1,
        "explanation": "In MySQL, NULL is not equal to anything, not even itself. NULL = NULL returns NULL (falsy). Always use IS NULL / IS NOT NULL for NULL checks. COALESCE() and IFNULL() are useful for providing default values.",
    },
    {
        "id": "mysql_10",
        "question": "What are MySQL window functions and which version introduced them?",
        "options": [
            "Functions for GUI window management, available since MySQL 3.0",
            "Analytical functions (ROW_NUMBER, RANK, LAG, LEAD, SUM OVER) introduced in MySQL 8.0",
            "Functions for creating temporary tables, available since MySQL 5.0",
            "They are the same as aggregate functions, available in all versions",
        ],
        "answer": 1,
        "explanation": "Window functions (ROW_NUMBER(), RANK(), DENSE_RANK(), LAG(), LEAD(), SUM() OVER, etc.) were introduced in MySQL 8.0 (2018). They perform calculations across related rows without collapsing the result set like GROUP BY does.",
    },
    {
        "id": "mysql_11",
        "question": "What is the difference between WHERE and HAVING in MySQL?",
        "options": [
            "There is no difference",
            "WHERE filters rows before aggregation; HAVING filters groups after aggregation",
            "HAVING is used only with JOINs",
            "WHERE can use aggregate functions but HAVING cannot",
        ],
        "answer": 1,
        "explanation": "WHERE filters individual rows BEFORE GROUP BY and aggregation. HAVING filters groups AFTER aggregation. You can't use SUM(), COUNT() etc. in WHERE — use HAVING for conditions on aggregated values.",
    },
    {
        "id": "mysql_12",
        "question": "What is a Common Table Expression (CTE) in MySQL?",
        "options": [
            "A type of stored procedure",
            "A named temporary result set defined with WITH keyword that exists only during query execution",
            "A permanent table created in memory",
            "A MySQL-specific JOIN type",
        ],
        "answer": 1,
        "explanation": "CTEs (WITH ... AS) create named temporary result sets within a query. They improve readability, allow recursion, and can be referenced multiple times. Available since MySQL 8.0. Example: WITH sales AS (SELECT ...) SELECT * FROM sales;",
    },
]

CODING_QUESTIONS = [
    {
        "id": "code_1",
        "title": "Revenue Summary by Category",
        "question": """Write a Python function that takes a list of transactions (dicts with 'creator', 'category', 'amount', 'status')
and returns a dictionary with the total revenue per category (only counting 'completed' transactions), sorted by revenue descending.""",
        "example_input": """transactions = [
    {"creator": "Alice", "category": "Fashion", "amount": 150.0, "status": "completed"},
    {"creator": "Bob", "category": "Beauty", "amount": 200.0, "status": "completed"},
    {"creator": "Alice", "category": "Fashion", "amount": 100.0, "status": "pending"},
    {"creator": "Carol", "category": "Fashion", "amount": 250.0, "status": "completed"},
    {"creator": "Dave", "category": "Beauty", "amount": 75.0, "status": "completed"},
]""",
        "example_output": '{"Fashion": 400.0, "Beauty": 275.0}  # Note: pending excluded',
        "hint": "Filter by status, use a dictionary to accumulate totals, sort with sorted() and a lambda",
        "solution": """def revenue_by_category(transactions):
    totals = {}
    for t in transactions:
        if t["status"] == "completed":
            cat = t["category"]
            totals[cat] = totals.get(cat, 0) + t["amount"]
    # Sort by revenue descending
    return dict(sorted(totals.items(), key=lambda x: x[1], reverse=True))""",
        "key_concepts": ["Dictionary accumulation", "Filtering", "sorted() with lambda", "dict.get() with default"],
    },
    {
        "id": "code_2",
        "title": "Moving Average Calculator",
        "question": """Write a Python function that calculates the N-day moving average for a list of daily revenue values.
Return a list of floats (rounded to 2 decimal places). For the first N-1 days where there isn't enough data, return None.""",
        "example_input": """revenues = [100, 200, 150, 300, 250, 400]
window = 3""",
        "example_output": "[None, None, 150.0, 216.67, 233.33, 316.67]",
        "hint": "Use a sliding window approach. For index i >= window-1, compute the average of the previous `window` elements.",
        "solution": """def moving_average(revenues, window):
    result = []
    for i in range(len(revenues)):
        if i < window - 1:
            result.append(None)
        else:
            avg = sum(revenues[i - window + 1:i + 1]) / window
            result.append(round(avg, 2))
    return result""",
        "key_concepts": ["Sliding window", "List slicing", "round()", "Edge case handling"],
    },
    {
        "id": "code_3",
        "title": "Funnel Drop-off Analysis",
        "question": """Write a Python function that takes a list of events (dicts with 'user_id' and 'event_type') and
a list of funnel steps (ordered), and returns a list of dicts showing the count and drop-off percentage at each step.""",
        "example_input": """events = [
    {"user_id": 1, "event_type": "page_view"},
    {"user_id": 2, "event_type": "page_view"},
    {"user_id": 3, "event_type": "page_view"},
    {"user_id": 1, "event_type": "add_to_cart"},
    {"user_id": 2, "event_type": "add_to_cart"},
    {"user_id": 1, "event_type": "purchase"},
]
funnel_steps = ["page_view", "add_to_cart", "purchase"]""",
        "example_output": """[
    {"step": "page_view", "users": 3, "drop_off_pct": 0},
    {"step": "add_to_cart", "users": 2, "drop_off_pct": 33.33},
    {"step": "purchase", "users": 1, "drop_off_pct": 50.0},
]""",
        "hint": "Use sets to count distinct users per event type. Drop-off = (prev - current) / prev * 100",
        "solution": """def funnel_analysis(events, funnel_steps):
    # Count distinct users per event type
    users_per_step = {}
    for e in events:
        step = e["event_type"]
        if step not in users_per_step:
            users_per_step[step] = set()
        users_per_step[step].add(e["user_id"])

    result = []
    prev_count = None
    for step in funnel_steps:
        count = len(users_per_step.get(step, set()))
        if prev_count is None or prev_count == 0:
            drop_off = 0
        else:
            drop_off = round((prev_count - count) / prev_count * 100, 2)
        result.append({
            "step": step,
            "users": count,
            "drop_off_pct": drop_off,
        })
        prev_count = count
    return result""",
        "key_concepts": ["Set for distinct counting", "Sequential funnel logic", "Percentage calculation", "dict.get() with default"],
    },
    {
        "id": "code_4",
        "title": "Detect Anomalous Revenue Days",
        "question": """Write a Python function that identifies anomalous days where revenue is more than 2 standard deviations
from the mean. Input: list of dicts with 'date' and 'revenue'. Return: list of anomalous records with an added 'z_score' field.""",
        "example_input": """daily_revenue = [
    {"date": "2026-01-01", "revenue": 1000},
    {"date": "2026-01-02", "revenue": 1100},
    {"date": "2026-01-03", "revenue": 950},
    {"date": "2026-01-04", "revenue": 5000},  # anomaly
    {"date": "2026-01-05", "revenue": 1050},
]""",
        "example_output": '[{"date": "2026-01-04", "revenue": 5000, "z_score": 2.43}]',
        "hint": "Calculate mean and standard deviation manually (or with statistics module). Z-score = (value - mean) / std_dev",
        "solution": """def detect_anomalies(daily_revenue, threshold=2):
    revenues = [d["revenue"] for d in daily_revenue]
    n = len(revenues)
    mean = sum(revenues) / n
    variance = sum((x - mean) ** 2 for x in revenues) / n
    std_dev = variance ** 0.5

    if std_dev == 0:
        return []

    anomalies = []
    for d in daily_revenue:
        z = (d["revenue"] - mean) / std_dev
        if abs(z) > threshold:
            anomalies.append({
                "date": d["date"],
                "revenue": d["revenue"],
                "z_score": round(z, 2),
            })
    return anomalies""",
        "key_concepts": ["Statistical analysis (mean, std dev, z-score)", "List comprehension", "Edge case (std_dev=0)", "Dictionary construction"],
    },
    {
        "id": "code_5",
        "title": "Parse and Clean CSV-like Data",
        "question": """Write a Python function that takes a CSV string (with header row) and returns a list of cleaned dictionaries.
Cleaning rules: strip whitespace, convert numeric strings to floats, convert 'NULL'/'None'/'' to None.""",
        "example_input": """csv_data = \"\"\"name, amount, category
Alice , 150.50, Fashion
Bob, NULL, Beauty
Carol, 200,
\"\"\"
""",
        "example_output": """[
    {"name": "Alice", "amount": 150.5, "category": "Fashion"},
    {"name": "Bob", "amount": None, "category": "Beauty"},
    {"name": "Carol", "amount": 200.0, "category": None},
]""",
        "hint": "Split by newlines, then by commas. Strip each value. Try float() conversion in a try/except.",
        "solution": """def parse_csv(csv_data):
    lines = [l.strip() for l in csv_data.strip().split("\\n") if l.strip()]
    headers = [h.strip() for h in lines[0].split(",")]
    result = []
    for line in lines[1:]:
        values = [v.strip() for v in line.split(",")]
        row = {}
        for h, v in zip(headers, values):
            if v in ("NULL", "None", ""):
                row[h] = None
            else:
                try:
                    row[h] = float(v)
                except ValueError:
                    row[h] = v
        result.append(row)
    return result""",
        "key_concepts": ["String parsing", "try/except for type conversion", "zip() for pairing headers/values", "Data cleaning patterns"],
    },
]

LTK_COMPANY_INFO = {
    "name": "LTK (formerly rewardStyle & LIKEtoKNOW.it)",
    "founded": "2011",
    "hq": "Dallas, Texas",
    "employees": "650+",
    "description": "Global technology platform that empowers lifestyle creators to monetize content. Connects brands, creators, and shoppers via a shopping app and tools.",
    "key_metrics": [
        "$5B+ annual retail sales driven by creators",
        "40M+ monthly consumers on LTK shopping platform",
        "Creators in 160+ countries",
        "Three-sided marketplace: Creators, Brands, Shoppers",
    ],
    "tech_stack": {
        "Database": "PostgreSQL",
        "BI/Analytics": "Looker, Hex, Wisdom",
        "Web Analytics": "Google Analytics",
        "Query Language": "SQL",
        "Commerce": "ShareASale, HCL Commerce",
    },
    "role_focus": [
        "Write SQL, build dashboards, run analyses",
        "Support Product, Finance, and Operations teams",
        "Analyze experiments and A/B tests",
        "Monitor funnels, platform health, business KPIs",
        "Automate reporting with SQL, BI tools, or scripting",
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# APP LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

def init_session_state():
    if "scores" not in st.session_state:
        st.session_state.scores = {
            "etl": {"correct": 0, "total": 0, "answered": set()},
            "sql": {"reviewed": set()},
            "mysql": {"correct": 0, "total": 0, "answered": set()},
            "coding": {"reviewed": set()},
        }
    if "show_solutions" not in st.session_state:
        st.session_state.show_solutions = {}
    if "show_hints" not in st.session_state:
        st.session_state.show_hints = {}


def render_company_info():
    st.markdown("## Sobre LTK")
    info = LTK_COMPANY_INFO

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Empresa:** {info['name']}")
        st.markdown(f"**Fundada:** {info['founded']}")
        st.markdown(f"**HQ:** {info['hq']}")
        st.markdown(f"**Empleados:** {info['employees']}")
        st.markdown(f"\n**Descripcion:** {info['description']}")

    with col2:
        st.markdown("**Metricas clave:**")
        for m in info["key_metrics"]:
            st.markdown(f"- {m}")

    st.markdown("### Tech Stack")
    for cat, tools in info["tech_stack"].items():
        st.markdown(f"- **{cat}:** {tools}")

    st.markdown("### Lo que haras como Data Analyst en LTK")
    for item in info["role_focus"]:
        st.markdown(f"- {item}")


def render_etl_section():
    st.markdown("## Seccion 1: ETL (Extract, Transform, Load)")
    st.markdown("""
<div class='info-box'>
    <strong>Que esperar:</strong> Preguntas de opcion multiple sobre conceptos de ETL,
    data warehousing, pipelines de datos y mejores practicas. Tipicamente 5-8 preguntas en Coderbyte.
</div>
    """, unsafe_allow_html=True)

    scores = st.session_state.scores["etl"]

    for i, q in enumerate(ETL_QUESTIONS):
        with st.expander(f"Pregunta {i+1}: {q['question']}", expanded=(i < 3)):
            key = f"etl_{q['id']}"
            selected = st.radio(
                "Selecciona tu respuesta:",
                q["options"],
                key=key,
                index=None,
            )

            if selected is not None and q["id"] not in scores["answered"]:
                scores["answered"].add(q["id"])
                scores["total"] += 1
                selected_idx = q["options"].index(selected)
                if selected_idx == q["answer"]:
                    scores["correct"] += 1

            if selected is not None:
                selected_idx = q["options"].index(selected)
                if selected_idx == q["answer"]:
                    st.markdown(f"<div class='correct'>✅ Correcto! {q['explanation']}</div>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<div class='incorrect'>❌ Incorrecto. La respuesta correcta es: <strong>{q['options'][q['answer']]}</strong><br>{q['explanation']}</div>", unsafe_allow_html=True)

    if scores["total"] > 0:
        st.markdown(f"### Score ETL: {scores['correct']}/{scores['total']} ({scores['correct']/scores['total']*100:.0f}%)")


def render_sql_section():
    st.markdown("## Seccion 2: SQL Queries")
    st.markdown("""
<div class='info-box'>
    <strong>Que esperar:</strong> Te daran tablas y te pediran escribir queries SQL.
    En Coderbyte, tu query debe producir un output que coincida con la tabla esperada.
    Los temas clave: JOINs, window functions, CTEs, aggregations, funnels, retention.
    <br><br>
    <strong>Consejo LTK:</strong> Piensa en el contexto de e-commerce / influencer marketing:
    creators, transactions, brands, shoppers, conversion funnels, revenue analysis.
</div>
    """, unsafe_allow_html=True)

    for i, q in enumerate(SQL_QUESTIONS):
        with st.expander(f"Ejercicio SQL {i+1}: {q['question']}", expanded=(i < 2)):
            st.markdown("**Contexto:**")
            st.code(q["context"], language="text")

            # User's workspace
            user_answer = st.text_area(
                "Escribe tu query SQL aqui:",
                height=200,
                key=f"sql_answer_{q['id']}",
                placeholder="SELECT ...",
            )

            col1, col2 = st.columns(2)
            with col1:
                if st.button("💡 Ver Hint", key=f"hint_sql_{q['id']}"):
                    st.session_state.show_hints[q['id']] = True
            with col2:
                if st.button("✅ Ver Solucion", key=f"sol_sql_{q['id']}"):
                    st.session_state.show_solutions[q['id']] = True
                    st.session_state.scores["sql"]["reviewed"].add(q['id'])

            if st.session_state.show_hints.get(q['id']):
                st.markdown(f"<div class='hint'>💡 <strong>Hint:</strong> {q['hint']}</div>", unsafe_allow_html=True)

            if st.session_state.show_solutions.get(q['id']):
                st.markdown("**Solucion:**")
                st.code(q["solution"], language="sql")
                st.markdown("**Conceptos clave:**")
                for concept in q["key_concepts"]:
                    st.markdown(f"- `{concept}`")


def render_mysql_section():
    st.markdown("## Seccion 3: MySQL Conceptual")
    st.markdown("""
<div class='info-box'>
    <strong>Que esperar:</strong> Preguntas sobre funcionalidades especificas de MySQL:
    tipos de datos, funciones, indices, JOINs, optimizacion, window functions (MySQL 8.0+).
    <br><br>
    <strong>Nota:</strong> LTK usa PostgreSQL pero el assessment de Coderbyte es en MySQL.
    La mayoria de conceptos SQL son transferibles entre ambos.
</div>
    """, unsafe_allow_html=True)

    scores = st.session_state.scores["mysql"]

    for i, q in enumerate(MYSQL_QUESTIONS):
        with st.expander(f"Pregunta {i+1}: {q['question']}", expanded=(i < 3)):
            key = f"mysql_{q['id']}"
            selected = st.radio(
                "Selecciona tu respuesta:",
                q["options"],
                key=key,
                index=None,
            )

            if selected is not None and q["id"] not in scores["answered"]:
                scores["answered"].add(q["id"])
                scores["total"] += 1
                selected_idx = q["options"].index(selected)
                if selected_idx == q["answer"]:
                    scores["correct"] += 1

            if selected is not None:
                selected_idx = q["options"].index(selected)
                if selected_idx == q["answer"]:
                    st.markdown(f"<div class='correct'>✅ Correcto! {q['explanation']}</div>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<div class='incorrect'>❌ Incorrecto. La respuesta correcta es: <strong>{q['options'][q['answer']]}</strong><br>{q['explanation']}</div>", unsafe_allow_html=True)

    if scores["total"] > 0:
        st.markdown(f"### Score MySQL: {scores['correct']}/{scores['total']} ({scores['correct']/scores['total']*100:.0f}%)")


def render_coding_section():
    st.markdown("## Seccion 4: Coding Challenge (Python)")
    st.markdown("""
<div class='info-box'>
    <strong>Que esperar:</strong> Un problema de programacion que combina logica,
    manipulacion de datos, y pensamiento analitico. Generalmente en Python.
    <br><br>
    <strong>Importante:</strong> Segun Hammad, saltarse la parte de coding NO es aceptable.
    Incluso una solucion parcial es mejor que no intentarlo.
    <br><br>
    <strong>Temas comunes:</strong> Procesamiento de datos, agregaciones, limpieza de datos,
    analisis estadistico basico, parsing de archivos.
</div>
    """, unsafe_allow_html=True)

    for i, q in enumerate(CODING_QUESTIONS):
        with st.expander(f"Challenge {i+1}: {q['title']}", expanded=(i < 2)):
            st.markdown(f"**Problema:** {q['question']}")

            st.markdown("**Input de ejemplo:**")
            st.code(q["example_input"], language="python")

            st.markdown("**Output esperado:**")
            st.code(q["example_output"], language="python")

            user_code = st.text_area(
                "Escribe tu solucion en Python:",
                height=250,
                key=f"code_answer_{q['id']}",
                placeholder="def solution(...):\n    ...",
            )

            col1, col2 = st.columns(2)
            with col1:
                if st.button("💡 Ver Hint", key=f"hint_code_{q['id']}"):
                    st.session_state.show_hints[q['id']] = True
            with col2:
                if st.button("✅ Ver Solucion", key=f"sol_code_{q['id']}"):
                    st.session_state.show_solutions[q['id']] = True
                    st.session_state.scores["coding"]["reviewed"].add(q['id'])

            if st.session_state.show_hints.get(q['id']):
                st.markdown(f"<div class='hint'>💡 <strong>Hint:</strong> {q['hint']}</div>", unsafe_allow_html=True)

            if st.session_state.show_solutions.get(q['id']):
                st.markdown("**Solucion:**")
                st.code(q["solution"], language="python")
                st.markdown("**Conceptos clave:**")
                for concept in q["key_concepts"]:
                    st.markdown(f"- `{concept}`")


def render_cheat_sheet():
    st.markdown("## SQL & MySQL Quick Reference")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Window Functions (MySQL 8.0+)")
        st.code("""-- ROW_NUMBER: unique sequential number
ROW_NUMBER() OVER (PARTITION BY col ORDER BY col2)

-- RANK: same rank for ties, gaps after
RANK() OVER (ORDER BY revenue DESC)

-- DENSE_RANK: same rank for ties, NO gaps
DENSE_RANK() OVER (ORDER BY revenue DESC)

-- LAG: access previous row's value
LAG(revenue, 1) OVER (ORDER BY month)

-- LEAD: access next row's value
LEAD(revenue, 1) OVER (ORDER BY month)

-- Running total
SUM(amount) OVER (
    PARTITION BY creator_id
    ORDER BY txn_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)""", language="sql")

        st.markdown("### CTEs (Common Table Expressions)")
        st.code("""WITH cte_name AS (
    SELECT col1, col2
    FROM table1
    WHERE condition
),
second_cte AS (
    SELECT *
    FROM cte_name
    WHERE another_condition
)
SELECT * FROM second_cte;""", language="sql")

        st.markdown("### Date Functions (MySQL)")
        st.code("""-- Current date/time
CURRENT_DATE, NOW(), CURDATE()

-- Date arithmetic
DATE_ADD(date, INTERVAL 30 DAY)
DATE_SUB(date, INTERVAL 1 MONTH)
DATEDIFF(date1, date2)  -- days between

-- Formatting
DATE_FORMAT(date, '%Y-%m')    -- '2026-03'
DATE_FORMAT(date, '%Y-%m-%d') -- '2026-03-14'
EXTRACT(MONTH FROM date)
YEAR(date), MONTH(date), DAY(date)""", language="sql")

    with col2:
        st.markdown("### Aggregations & Grouping")
        st.code("""-- Basic aggregations
COUNT(*), COUNT(DISTINCT col)
SUM(amount), AVG(amount)
MIN(val), MAX(val)

-- Conditional aggregation
COUNT(DISTINCT CASE WHEN status = 'active'
    THEN user_id END) AS active_users

SUM(CASE WHEN category = 'Fashion'
    THEN amount ELSE 0 END) AS fashion_revenue

-- HAVING (filter after GROUP BY)
SELECT category, SUM(amount) AS total
FROM transactions
GROUP BY category
HAVING SUM(amount) > 1000;""", language="sql")

        st.markdown("### JOINs")
        st.code("""-- INNER: only matching rows
SELECT * FROM a INNER JOIN b ON a.id = b.a_id

-- LEFT: all from left, NULLs for no match
SELECT * FROM a LEFT JOIN b ON a.id = b.a_id

-- Self JOIN (compare rows in same table)
SELECT * FROM t a JOIN t b
    ON a.creator_id = b.creator_id
    AND a.txn_rank = 1 AND b.txn_rank = 2

-- FULL OUTER (MySQL workaround)
SELECT * FROM a LEFT JOIN b ON a.id = b.id
UNION
SELECT * FROM a RIGHT JOIN b ON a.id = b.id;""", language="sql")

        st.markdown("### Useful MySQL Functions")
        st.code("""-- NULL handling
IFNULL(expr, default_value)
COALESCE(val1, val2, val3)  -- first non-NULL
NULLIF(expr1, expr2)  -- NULL if equal

-- String
CONCAT(str1, str2)
GROUP_CONCAT(name SEPARATOR ', ')
SUBSTRING(str, start, length)
TRIM(), UPPER(), LOWER()

-- Numeric
ROUND(val, decimals)
CEIL(), FLOOR()
MOD(a, b)

-- EXPLAIN (query optimization)
EXPLAIN SELECT * FROM big_table WHERE ...;""", language="sql")

    st.markdown("### ETL Concepts Quick Review")
    st.code("""ETL vs ELT:
  ETL: Transform BEFORE loading (traditional)
  ELT: Load raw, transform IN warehouse (modern: BigQuery, Snowflake)

Key Concepts:
  - Staging Area: temp storage for raw extracted data
  - Incremental Load: only new/changed records (vs full reload)
  - Idempotency: same result if run multiple times
  - MERGE/UPSERT: INSERT new + UPDATE existing in one operation
  - SCD (Slowly Changing Dimensions): tracking historical changes
    Type 1: Overwrite | Type 2: New row + version | Type 3: Add column
  - Data Lineage: trace data from source through all transforms
  - Data Quality: completeness, accuracy, consistency, timeliness

Common Tools:
  - Orchestration: Apache Airflow, dbt, Prefect, Dagster
  - Warehouses: Snowflake, BigQuery, Redshift, Databricks
  - Ingestion: Fivetran, Stitch, Airbyte""", language="text")


def render_tips():
    st.markdown("## Tips para el Assessment de Coderbyte")

    st.markdown("""
### Antes del examen
1. **Asegurate de tener buena conexion a internet** - el examen monitorea cambios de tab
2. **No copies/pegues de otras fuentes** - Coderbyte detecta esto
3. **No uses herramientas de AI** - sera detectado como trampa
4. **Lee TODO el enunciado** antes de empezar a escribir

### Durante el examen - SQL
1. **Empieza con un SELECT simple** y ve agregando complejidad
2. **Usa CTEs** (WITH) para organizar queries complejos - mas legible que subqueries anidados
3. **Prueba tu logica mentalmente** con datos de ejemplo
4. **Cuidado con NULLs** - usa IS NULL, COALESCE, IFNULL
5. **No olvides ORDER BY** si el resultado debe estar ordenado
6. **GROUP BY debe incluir** todas las columnas no agregadas del SELECT

### Durante el examen - Coding
1. **Lee bien el problema** - entiende input/output esperado
2. **Empieza con la solucion mas simple** que funcione
3. **Maneja edge cases**: lista vacia, NULLs, division por cero
4. **Es mejor enviar algo parcial** que no enviar nada

### Conceptos clave para LTK
- **Conversion funnels**: page_view → add_to_cart → purchase
- **Retention/Cohort analysis**: % de usuarios que vuelven
- **Revenue metrics**: MoM growth, revenue per creator, unit economics
- **A/B testing**: comparing control vs treatment groups
- **Three-sided marketplace**: Creators, Brands, Shoppers
""")


def render_score_summary():
    st.markdown("## Resumen de Progreso")
    scores = st.session_state.scores

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        etl = scores["etl"]
        if etl["total"] > 0:
            pct = etl["correct"] / etl["total"] * 100
            st.metric("ETL", f"{etl['correct']}/{etl['total']}", f"{pct:.0f}%")
        else:
            st.metric("ETL", "Sin intentar", "0/12")

    with col2:
        sql_rev = len(scores["sql"]["reviewed"])
        st.metric("SQL Ejercicios", f"{sql_rev}/{len(SQL_QUESTIONS)} revisados")

    with col3:
        mysql = scores["mysql"]
        if mysql["total"] > 0:
            pct = mysql["correct"] / mysql["total"] * 100
            st.metric("MySQL", f"{mysql['correct']}/{mysql['total']}", f"{pct:.0f}%")
        else:
            st.metric("MySQL", "Sin intentar", "0/12")

    with col4:
        code_rev = len(scores["coding"]["reviewed"])
        st.metric("Coding", f"{code_rev}/{len(CODING_QUESTIONS)} revisados")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN APP
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    init_session_state()

    st.title("📊 LTK Data Analyst Assessment Prep")
    st.markdown("*Simulacion de evaluacion tecnica tipo Coderbyte para la posicion de Data Analyst en LTK*")

    # Sidebar navigation
    st.sidebar.title("Navegacion")
    page = st.sidebar.radio(
        "Seccion:",
        [
            "🏢 Sobre LTK",
            "📋 ETL (Multiple Choice)",
            "🔍 SQL Queries",
            "🐬 MySQL Conceptual",
            "🐍 Coding (Python)",
            "📝 Cheat Sheet",
            "💡 Tips & Estrategia",
            "📊 Progreso",
        ],
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Info del Assessment")
    st.sidebar.markdown("""
    **Plataforma:** Coderbyte
    **Secciones:** ETL, SQL, MySQL, Coding
    **Empresa:** LTK (rewardStyle)
    **Rol:** Data Analyst
    **Tipo:** Remoto, Contractor
    """)

    st.sidebar.markdown("---")
    render_score_summary()

    # Render selected page
    if page == "🏢 Sobre LTK":
        render_company_info()
    elif page == "📋 ETL (Multiple Choice)":
        render_etl_section()
    elif page == "🔍 SQL Queries":
        render_sql_section()
    elif page == "🐬 MySQL Conceptual":
        render_mysql_section()
    elif page == "🐍 Coding (Python)":
        render_coding_section()
    elif page == "📝 Cheat Sheet":
        render_cheat_sheet()
    elif page == "💡 Tips & Estrategia":
        render_tips()
    elif page == "📊 Progreso":
        render_score_summary()
        st.markdown("---")
        st.markdown("### Distribucion recomendada del tiempo")
        st.markdown("""
        | Seccion | Tiempo sugerido | Prioridad |
        |---------|----------------|-----------|
        | ETL (Multiple Choice) | 10-15 min | Media |
        | SQL Queries | 25-35 min | **Alta** |
        | MySQL Conceptual | 10-15 min | Media |
        | Coding (Python) | 15-20 min | **Alta** |
        """)


if __name__ == "__main__":
    main()
