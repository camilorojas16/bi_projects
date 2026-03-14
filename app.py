import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import textwrap

st.set_page_config(
    page_title="Data Analyst Skills Assessment",
    page_icon="📝",
    layout="wide",
)

# ---------------------------------------------------------------------------
# In-memory SQLite database with e-commerce sample data
# ---------------------------------------------------------------------------
@st.cache_resource
def init_db():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            signup_date TEXT,
            country TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TEXT,
            status TEXT,
            channel TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    cur.execute("""
        CREATE TABLE order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_name TEXT,
            category TEXT,
            quantity INTEGER,
            unit_price REAL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        )
    """)

    np.random.seed(42)
    countries = ["US", "UK", "CA", "MX", "BR", "CO", "DE", "FR"]
    channels = ["Influencer Link", "Organic Search", "Paid Social", "Email", "Direct"]
    statuses = ["completed", "completed", "completed", "cancelled", "returned"]
    categories = ["Fashion", "Beauty", "Home", "Fitness", "Electronics"]
    products = {
        "Fashion": ["Dress", "Jacket", "Sneakers", "Handbag", "Sunglasses"],
        "Beauty": ["Lipstick", "Moisturizer", "Perfume", "Foundation", "Serum"],
        "Home": ["Candle", "Throw Pillow", "Vase", "Lamp", "Rug"],
        "Fitness": ["Yoga Mat", "Dumbbells", "Resistance Bands", "Water Bottle", "Leggings"],
        "Electronics": ["Earbuds", "Phone Case", "Charger", "Smartwatch", "Speaker"],
    }

    # 200 customers
    for i in range(1, 201):
        signup = f"2024-{np.random.randint(1,13):02d}-{np.random.randint(1,29):02d}"
        cur.execute(
            "INSERT INTO customers VALUES (?, ?, ?, ?, ?)",
            (i, f"Customer_{i}", f"cust{i}@email.com", signup, np.random.choice(countries)),
        )

    # 1000 orders
    for i in range(1, 1001):
        cid = np.random.randint(1, 201)
        odate = f"2025-{np.random.randint(1,13):02d}-{np.random.randint(1,29):02d}"
        cur.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
            (i, cid, odate, np.random.choice(statuses), np.random.choice(channels)),
        )

    # 2500 order items
    for i in range(1, 2501):
        oid = np.random.randint(1, 1001)
        cat = np.random.choice(categories)
        prod = np.random.choice(products[cat])
        qty = np.random.randint(1, 5)
        price = round(np.random.uniform(10, 200), 2)
        cur.execute(
            "INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?)",
            (i, oid, prod, cat, qty, price),
        )

    conn.commit()
    return conn


conn = init_db()

# ---------------------------------------------------------------------------
# App header
# ---------------------------------------------------------------------------
st.title("Data Analyst - Skills Assessment Practice")
st.markdown(
    "Prepare for the **Coderbyte assessment** (ETL, SQL, MySQL, coding). "
    "Practice with interactive questions using a live SQLite database."
)
st.divider()

# ---------------------------------------------------------------------------
# Show schema
# ---------------------------------------------------------------------------
with st.expander("Database Schema (click to expand)"):
    st.code(textwrap.dedent("""\
        TABLE customers (
            customer_id  INTEGER PRIMARY KEY,
            name         TEXT,
            email        TEXT,
            signup_date  TEXT,       -- e.g. '2024-06-15'
            country      TEXT        -- e.g. 'US', 'CO', 'MX'
        )

        TABLE orders (
            order_id     INTEGER PRIMARY KEY,
            customer_id  INTEGER,    -- FK -> customers
            order_date   TEXT,       -- e.g. '2025-03-10'
            status       TEXT,       -- 'completed', 'cancelled', 'returned'
            channel      TEXT        -- 'Influencer Link', 'Organic Search', 'Paid Social', 'Email', 'Direct'
        )

        TABLE order_items (
            item_id      INTEGER PRIMARY KEY,
            order_id     INTEGER,    -- FK -> orders
            product_name TEXT,
            category     TEXT,       -- 'Fashion', 'Beauty', 'Home', 'Fitness', 'Electronics'
            quantity     INTEGER,
            unit_price   REAL
        )
    """), language="sql")

    st.markdown("**Sample data:**")
    for table in ["customers", "orders", "order_items"]:
        st.caption(table)
        st.dataframe(pd.read_sql(f"SELECT * FROM {table} LIMIT 5", conn), hide_index=True)

# ---------------------------------------------------------------------------
# Section selector
# ---------------------------------------------------------------------------
section = st.sidebar.radio(
    "Section",
    ["SQL Practice", "ETL Concepts", "Python Coding", "SQL Sandbox"],
)

# ========================== SQL PRACTICE ==================================
if section == "SQL Practice":
    st.header("SQL Practice")

    sql_questions = [
        {
            "id": "sql1",
            "title": "1. Total revenue per channel",
            "prompt": (
                "Write a query to find the **total revenue** (quantity * unit_price) "
                "for each **channel**, only for **completed** orders. "
                "Order by revenue descending."
            ),
            "answer": textwrap.dedent("""\
                SELECT o.channel,
                       ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_revenue
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                WHERE o.status = 'completed'
                GROUP BY o.channel
                ORDER BY total_revenue DESC;
            """),
            "hint": "JOIN orders with order_items, filter by status, GROUP BY channel.",
        },
        {
            "id": "sql2",
            "title": "2. Top 5 customers by number of orders",
            "prompt": (
                "Find the **top 5 customers** (name, country) with the most orders. "
                "Include the order count. Order by count descending."
            ),
            "answer": textwrap.dedent("""\
                SELECT c.name, c.country, COUNT(o.order_id) AS order_count
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.name, c.country
                ORDER BY order_count DESC
                LIMIT 5;
            """),
            "hint": "JOIN customers with orders, GROUP BY customer, use LIMIT 5.",
        },
        {
            "id": "sql3",
            "title": "3. Month-over-month revenue growth",
            "prompt": (
                "Calculate the **total revenue per month** (completed orders only) "
                "and the **percentage change** from the previous month. "
                "Use a window function (LAG)."
            ),
            "answer": textwrap.dedent("""\
                WITH monthly AS (
                    SELECT SUBSTR(o.order_date, 1, 7) AS month,
                           ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue
                    FROM orders o
                    JOIN order_items oi ON o.order_id = oi.order_id
                    WHERE o.status = 'completed'
                    GROUP BY month
                )
                SELECT month,
                       revenue,
                       LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
                       ROUND(
                           (revenue - LAG(revenue) OVER (ORDER BY month))
                           / LAG(revenue) OVER (ORDER BY month) * 100, 2
                       ) AS pct_change
                FROM monthly
                ORDER BY month;
            """),
            "hint": "Use a CTE for monthly totals, then LAG() window function.",
        },
        {
            "id": "sql4",
            "title": "4. Conversion rate by channel",
            "prompt": (
                "For each channel, calculate the **total orders**, "
                "**completed orders**, and the **conversion rate** "
                "(completed / total * 100). Round to 2 decimals."
            ),
            "answer": textwrap.dedent("""\
                SELECT channel,
                       COUNT(*) AS total_orders,
                       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                       ROUND(
                           SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 100.0
                           / COUNT(*), 2
                       ) AS conversion_rate
                FROM orders
                GROUP BY channel
                ORDER BY conversion_rate DESC;
            """),
            "hint": "Use CASE WHEN inside SUM for conditional counting.",
        },
        {
            "id": "sql5",
            "title": "5. Customers with no orders",
            "prompt": (
                "Find all customers who have **never placed an order**. "
                "Show their name, email, and signup_date."
            ),
            "answer": textwrap.dedent("""\
                SELECT c.name, c.email, c.signup_date
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                WHERE o.order_id IS NULL;
            """),
            "hint": "Use LEFT JOIN and filter WHERE the order side IS NULL.",
        },
        {
            "id": "sql6",
            "title": "6. Running total of revenue",
            "prompt": (
                "Write a query that shows each completed order with its revenue "
                "and a **running total** of revenue ordered by order_date."
            ),
            "answer": textwrap.dedent("""\
                SELECT o.order_id,
                       o.order_date,
                       ROUND(SUM(oi.quantity * oi.unit_price), 2) AS order_revenue,
                       ROUND(SUM(SUM(oi.quantity * oi.unit_price))
                           OVER (ORDER BY o.order_date, o.order_id), 2
                       ) AS running_total
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                WHERE o.status = 'completed'
                GROUP BY o.order_id, o.order_date
                ORDER BY o.order_date, o.order_id;
            """),
            "hint": "Use SUM() OVER(ORDER BY ...) as a window function for running total.",
        },
        {
            "id": "sql7",
            "title": "7. Category ranking per channel",
            "prompt": (
                "Rank product **categories by revenue within each channel** "
                "(completed orders). Use RANK() or ROW_NUMBER(). "
                "Show channel, category, revenue, and rank."
            ),
            "answer": textwrap.dedent("""\
                SELECT channel, category, revenue,
                       RANK() OVER (PARTITION BY channel ORDER BY revenue DESC) AS rnk
                FROM (
                    SELECT o.channel,
                           oi.category,
                           ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue
                    FROM orders o
                    JOIN order_items oi ON o.order_id = oi.order_id
                    WHERE o.status = 'completed'
                    GROUP BY o.channel, oi.category
                ) sub
                ORDER BY channel, rnk;
            """),
            "hint": "Subquery for channel+category revenue, then RANK() OVER(PARTITION BY channel).",
        },
        {
            "id": "sql8",
            "title": "8. Average order value by country",
            "prompt": (
                "Find the **average order value** (total revenue per order) "
                "by customer country, for completed orders only. "
                "Only show countries with AOV > 100."
            ),
            "answer": textwrap.dedent("""\
                SELECT c.country,
                       ROUND(AVG(order_total), 2) AS avg_order_value
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                JOIN (
                    SELECT order_id, SUM(quantity * unit_price) AS order_total
                    FROM order_items
                    GROUP BY order_id
                ) oi ON o.order_id = oi.order_id
                WHERE o.status = 'completed'
                GROUP BY c.country
                HAVING AVG(order_total) > 100
                ORDER BY avg_order_value DESC;
            """),
            "hint": "Calculate order totals in a subquery, JOIN with customers, use HAVING.",
        },
    ]

    for q in sql_questions:
        st.subheader(q["title"])
        st.markdown(q["prompt"])

        with st.expander("Hint"):
            st.info(q["hint"])

        user_sql = st.text_area("Your SQL:", key=f"input_{q['id']}", height=120)

        col_run, col_show = st.columns(2)

        with col_run:
            if st.button("Run my query", key=f"run_{q['id']}"):
                if user_sql.strip():
                    try:
                        result = pd.read_sql(user_sql, conn)
                        st.success(f"Query returned {len(result)} rows")
                        st.dataframe(result, hide_index=True, use_container_width=True)
                    except Exception as e:
                        st.error(f"SQL Error: {e}")
                else:
                    st.warning("Write your query first.")

        with col_show:
            if st.button("Show answer", key=f"ans_{q['id']}"):
                st.code(q["answer"], language="sql")
                try:
                    result = pd.read_sql(q["answer"], conn)
                    st.dataframe(result, hide_index=True, use_container_width=True)
                except Exception as e:
                    st.error(f"Error in reference answer: {e}")

        st.divider()

# ========================== ETL CONCEPTS ==================================
elif section == "ETL Concepts":
    st.header("ETL Concepts")

    etl_questions = [
        {
            "id": "etl1",
            "question": "What does ETL stand for?",
            "options": [
                "Extract, Transform, Load",
                "Export, Transfer, Link",
                "Extract, Transfer, Log",
                "Export, Transform, Load",
            ],
            "correct": 0,
            "explanation": (
                "**Extract, Transform, Load** - Extract data from sources, "
                "transform it (clean, aggregate, join), and load it into a destination "
                "(data warehouse, database)."
            ),
        },
        {
            "id": "etl2",
            "question": "Which step handles data cleaning, deduplication, and type casting?",
            "options": ["Extract", "Transform", "Load", "All three equally"],
            "correct": 1,
            "explanation": (
                "The **Transform** step is where data cleaning, deduplication, "
                "type conversions, and business logic are applied."
            ),
        },
        {
            "id": "etl3",
            "question": "What is the main difference between ETL and ELT?",
            "options": [
                "ELT loads raw data first, then transforms in the warehouse",
                "ELT is faster because it skips transformation",
                "ETL is only for batch processing",
                "There is no difference",
            ],
            "correct": 0,
            "explanation": (
                "**ELT** loads raw data into the warehouse first, then uses the "
                "warehouse's compute power to transform. Common with modern cloud "
                "warehouses (BigQuery, Snowflake, Redshift)."
            ),
        },
        {
            "id": "etl4",
            "question": "You have duplicate rows in a staging table. Which SQL approach removes them?",
            "options": [
                "ROW_NUMBER() OVER(PARTITION BY ...) and keep rn = 1",
                "SELECT DISTINCT on all columns",
                "GROUP BY with HAVING COUNT(*) > 1",
                "Both A and B depending on the case",
            ],
            "correct": 3,
            "explanation": (
                "**DISTINCT** works for exact duplicates. **ROW_NUMBER()** with "
                "PARTITION BY is better when you need to deduplicate based on specific "
                "keys while keeping the most recent or relevant row."
            ),
        },
        {
            "id": "etl5",
            "question": "What is an idempotent ETL pipeline?",
            "options": [
                "A pipeline that produces the same result regardless of how many times it runs",
                "A pipeline that runs only once",
                "A pipeline that deletes old data before loading",
                "A pipeline that never fails",
            ],
            "correct": 0,
            "explanation": (
                "**Idempotent** means running the pipeline multiple times with the same "
                "input always produces the same output. This is critical for reliability "
                "and retry handling in production pipelines."
            ),
        },
        {
            "id": "etl6",
            "question": "What is a slowly changing dimension (SCD Type 2)?",
            "options": [
                "Tracks history by adding new rows with effective dates",
                "Overwrites the old value with the new value",
                "Adds a new column for each change",
                "Deletes the old record and inserts a new one",
            ],
            "correct": 0,
            "explanation": (
                "**SCD Type 2** preserves history by inserting a new row with "
                "effective_from/effective_to dates. Type 1 overwrites. "
                "This is a common data warehousing interview topic."
            ),
        },
        {
            "id": "etl7",
            "question": "Which is a best practice when loading data into a warehouse?",
            "options": [
                "Use UPSERT (MERGE) for incremental loads",
                "Always truncate and full reload",
                "Load directly into production tables without staging",
                "Skip data validation to improve speed",
            ],
            "correct": 0,
            "explanation": (
                "**UPSERT/MERGE** allows incremental loading - inserting new records "
                "and updating existing ones. Full reloads are wasteful for large datasets. "
                "Always use staging tables and validate before promoting to production."
            ),
        },
    ]

    score = 0
    total = len(etl_questions)

    for q in etl_questions:
        st.subheader(q["question"])
        answer = st.radio(
            "Select your answer:",
            q["options"],
            key=f"etl_{q['id']}",
            index=None,
        )

        if answer is not None:
            idx = q["options"].index(answer)
            if idx == q["correct"]:
                st.success("Correct!")
                score += 1
            else:
                st.error(f"Incorrect. Correct answer: **{q['options'][q['correct']]}**")
            st.info(q["explanation"])

        st.divider()

    if st.button("Show score"):
        st.metric("Score", f"{score} / {total}")

# ========================== PYTHON CODING =================================
elif section == "Python Coding":
    st.header("Python Coding Practice")

    coding_questions = [
        {
            "id": "py1",
            "title": "1. Find duplicate emails",
            "prompt": (
                "Write a function `find_duplicates(emails: list) -> list` "
                "that returns a sorted list of emails that appear more than once."
            ),
            "starter": textwrap.dedent("""\
                def find_duplicates(emails: list) -> list:
                    # Your code here
                    pass
            """),
            "test_code": textwrap.dedent("""\
                emails = ["a@test.com", "b@test.com", "a@test.com", "c@test.com", "b@test.com", "d@test.com"]
                result = find_duplicates(emails)
                assert result == ["a@test.com", "b@test.com"], f"Expected ['a@test.com', 'b@test.com'], got {result}"
                print("PASSED: find_duplicates")
            """),
            "answer": textwrap.dedent("""\
                def find_duplicates(emails: list) -> list:
                    from collections import Counter
                    counts = Counter(emails)
                    return sorted([e for e, c in counts.items() if c > 1])
            """),
        },
        {
            "id": "py2",
            "title": "2. Calculate conversion funnel",
            "prompt": (
                "Write a function `conversion_funnel(stages: dict) -> dict` "
                "that takes `{'sessions': 1000, 'views': 600, 'cart': 150, 'purchase': 45}` "
                "and returns the **drop-off rate** between each consecutive stage as a dict. "
                "E.g. `{'sessions_to_views': 40.0, 'views_to_cart': 75.0, 'cart_to_purchase': 70.0}`"
            ),
            "starter": textwrap.dedent("""\
                def conversion_funnel(stages: dict) -> dict:
                    # Your code here
                    pass
            """),
            "test_code": textwrap.dedent("""\
                stages = {'sessions': 1000, 'views': 600, 'cart': 150, 'purchase': 45}
                result = conversion_funnel(stages)
                assert result == {'sessions_to_views': 40.0, 'views_to_cart': 75.0, 'cart_to_purchase': 70.0}, f"Got {result}"
                print("PASSED: conversion_funnel")
            """),
            "answer": textwrap.dedent("""\
                def conversion_funnel(stages: dict) -> dict:
                    keys = list(stages.keys())
                    vals = list(stages.values())
                    result = {}
                    for i in range(len(keys) - 1):
                        drop = round((1 - vals[i+1] / vals[i]) * 100, 1)
                        result[f"{keys[i]}_to_{keys[i+1]}"] = drop
                    return result
            """),
        },
        {
            "id": "py3",
            "title": "3. Clean and transform CSV data",
            "prompt": (
                "Write a function `clean_revenue_data(data: list[dict]) -> list[dict]` that:\n"
                "- Removes rows where `revenue` is None or negative\n"
                "- Converts `date` string ('YYYY-MM-DD') to just the month ('YYYY-MM')\n"
                "- Rounds `revenue` to 2 decimal places\n"
                "- Returns the cleaned list sorted by date"
            ),
            "starter": textwrap.dedent("""\
                def clean_revenue_data(data: list) -> list:
                    # Your code here
                    pass
            """),
            "test_code": textwrap.dedent("""\
                data = [
                    {"date": "2025-03-15", "revenue": 150.567},
                    {"date": "2025-01-10", "revenue": None},
                    {"date": "2025-02-20", "revenue": -50},
                    {"date": "2025-01-05", "revenue": 200.123},
                ]
                result = clean_revenue_data(data)
                expected = [
                    {"date": "2025-01", "revenue": 200.12},
                    {"date": "2025-03", "revenue": 150.57},
                ]
                assert result == expected, f"Got {result}"
                print("PASSED: clean_revenue_data")
            """),
            "answer": textwrap.dedent("""\
                def clean_revenue_data(data: list) -> list:
                    cleaned = []
                    for row in data:
                        if row["revenue"] is None or row["revenue"] < 0:
                            continue
                        cleaned.append({
                            "date": row["date"][:7],
                            "revenue": round(row["revenue"], 2),
                        })
                    return sorted(cleaned, key=lambda x: x["date"])
            """),
        },
        {
            "id": "py4",
            "title": "4. Group by and aggregate",
            "prompt": (
                "Write a function `revenue_by_category(items: list[dict]) -> dict` that "
                "takes a list of dicts with keys `category`, `quantity`, `price` and returns "
                "a dict of `{category: total_revenue}` where revenue = quantity * price, "
                "rounded to 2 decimals, sorted by revenue descending."
            ),
            "starter": textwrap.dedent("""\
                def revenue_by_category(items: list) -> dict:
                    # Your code here
                    pass
            """),
            "test_code": textwrap.dedent("""\
                items = [
                    {"category": "Fashion", "quantity": 2, "price": 50.0},
                    {"category": "Beauty", "quantity": 3, "price": 30.0},
                    {"category": "Fashion", "quantity": 1, "price": 75.0},
                    {"category": "Beauty", "quantity": 2, "price": 25.0},
                ]
                result = revenue_by_category(items)
                expected = {"Fashion": 175.0, "Beauty": 140.0}
                assert result == expected, f"Got {result}"
                print("PASSED: revenue_by_category")
            """),
            "answer": textwrap.dedent("""\
                def revenue_by_category(items: list) -> dict:
                    totals = {}
                    for item in items:
                        cat = item["category"]
                        rev = item["quantity"] * item["price"]
                        totals[cat] = totals.get(cat, 0) + rev
                    totals = {k: round(v, 2) for k, v in totals.items()}
                    return dict(sorted(totals.items(), key=lambda x: x[1], reverse=True))
            """),
        },
    ]

    for q in coding_questions:
        st.subheader(q["title"])
        st.markdown(q["prompt"])

        user_code = st.text_area("Your code:", value=q["starter"], key=f"code_{q['id']}", height=150)

        col_test, col_ans = st.columns(2)

        with col_test:
            if st.button("Run tests", key=f"test_{q['id']}"):
                try:
                    exec_globals = {}
                    exec(user_code, exec_globals)
                    exec(q["test_code"], exec_globals)
                    st.success("All tests passed!")
                except AssertionError as e:
                    st.error(f"Test failed: {e}")
                except Exception as e:
                    st.error(f"Error: {e}")

        with col_ans:
            if st.button("Show answer", key=f"ans_{q['id']}"):
                st.code(q["answer"], language="python")

        st.divider()

# ========================== SQL SANDBOX ===================================
elif section == "SQL Sandbox":
    st.header("SQL Sandbox")
    st.markdown("Write any SQL query against the database. Use the schema reference above.")

    free_sql = st.text_area("Your SQL query:", height=200, key="sandbox_sql")

    if st.button("Run query", key="sandbox_run"):
        if free_sql.strip():
            try:
                result = pd.read_sql(free_sql, conn)
                st.success(f"Returned {len(result)} rows")
                st.dataframe(result, hide_index=True, use_container_width=True)
            except Exception as e:
                st.error(f"SQL Error: {e}")
        else:
            st.warning("Write a query first.")

st.divider()
st.caption("Data Analyst Skills Assessment | Camilo Rojas")
