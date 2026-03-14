import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)

# --- Seed for reproducibility ---
np.random.seed(42)

# --- Generate synthetic e-commerce data ---
@st.cache_data
def generate_data():
    dates = pd.date_range("2025-01-01", "2025-12-31", freq="D")
    n = len(dates)

    channels = ["Influencer Link", "Organic Search", "Paid Social", "Email", "Direct"]
    categories = ["Fashion", "Beauty", "Home", "Fitness", "Electronics"]

    rows = []
    for date in dates:
        for channel in channels:
            base_sessions = np.random.randint(800, 5000)
            # Seasonal boost for Q4
            if date.month >= 10:
                base_sessions = int(base_sessions * 1.4)

            view_rate = np.random.uniform(0.30, 0.70)
            cart_rate = np.random.uniform(0.08, 0.25)
            purchase_rate = np.random.uniform(0.02, 0.08)

            sessions = base_sessions
            product_views = int(sessions * view_rate)
            add_to_cart = int(sessions * cart_rate)
            purchases = int(sessions * purchase_rate)
            revenue = round(purchases * np.random.uniform(25, 120), 2)

            category = np.random.choice(categories)
            rows.append({
                "date": date,
                "channel": channel,
                "category": category,
                "sessions": sessions,
                "product_views": product_views,
                "add_to_cart": add_to_cart,
                "purchases": purchases,
                "revenue": revenue,
            })

    return pd.DataFrame(rows)


df = generate_data()

# --- Sidebar filters ---
st.sidebar.header("Filters")

date_range = st.sidebar.date_input(
    "Date range",
    value=(df["date"].min(), df["date"].max()),
    min_value=df["date"].min(),
    max_value=df["date"].max(),
)

selected_channels = st.sidebar.multiselect(
    "Channel", df["channel"].unique().tolist(), default=df["channel"].unique().tolist()
)

selected_categories = st.sidebar.multiselect(
    "Category", df["category"].unique().tolist(), default=df["category"].unique().tolist()
)

# Apply filters
if len(date_range) == 2:
    mask = (
        (df["date"] >= pd.Timestamp(date_range[0]))
        & (df["date"] <= pd.Timestamp(date_range[1]))
        & (df["channel"].isin(selected_channels))
        & (df["category"].isin(selected_categories))
    )
else:
    mask = (df["channel"].isin(selected_channels)) & (df["category"].isin(selected_categories))

filtered = df[mask]

# --- Header ---
st.title("E-Commerce Analytics Dashboard")
st.markdown(
    "Interactive analysis of conversion funnels, revenue drivers, and channel "
    "performance for an influencer-driven e-commerce platform."
)
st.divider()

# --- KPI row ---
total_sessions = filtered["sessions"].sum()
total_purchases = filtered["purchases"].sum()
total_revenue = filtered["revenue"].sum()
overall_conversion = (total_purchases / total_sessions * 100) if total_sessions > 0 else 0
avg_order_value = (total_revenue / total_purchases) if total_purchases > 0 else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Sessions", f"{total_sessions:,.0f}")
k2.metric("Purchases", f"{total_purchases:,.0f}")
k3.metric("Revenue", f"${total_revenue:,.0f}")
k4.metric("Conversion Rate", f"{overall_conversion:.2f}%")
k5.metric("Avg Order Value", f"${avg_order_value:.2f}")

st.divider()

# --- Conversion Funnel ---
st.subheader("Conversion Funnel")

funnel_data = pd.DataFrame({
    "Stage": ["Sessions", "Product Views", "Add to Cart", "Purchases"],
    "Count": [
        filtered["sessions"].sum(),
        filtered["product_views"].sum(),
        filtered["add_to_cart"].sum(),
        filtered["purchases"].sum(),
    ],
})
funnel_data["Drop-off %"] = (
    (1 - funnel_data["Count"] / funnel_data["Count"].shift(1)) * 100
).fillna(0).round(1)

col_funnel, col_table = st.columns([2, 1])
with col_funnel:
    st.bar_chart(funnel_data.set_index("Stage")["Count"])
with col_table:
    st.dataframe(funnel_data, hide_index=True, use_container_width=True)

st.divider()

# --- Revenue over time ---
st.subheader("Revenue Trend")

daily_revenue = filtered.groupby("date")["revenue"].sum().reset_index()
st.line_chart(daily_revenue.set_index("date")["revenue"])

# --- Channel and Category analysis side by side ---
st.divider()
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Revenue by Channel")
    channel_metrics = (
        filtered.groupby("channel")
        .agg(
            sessions=("sessions", "sum"),
            purchases=("purchases", "sum"),
            revenue=("revenue", "sum"),
        )
        .reset_index()
    )
    channel_metrics["conversion_rate"] = (
        (channel_metrics["purchases"] / channel_metrics["sessions"]) * 100
    ).round(2)
    channel_metrics["avg_order_value"] = (
        channel_metrics["revenue"] / channel_metrics["purchases"]
    ).round(2)
    channel_metrics = channel_metrics.sort_values("revenue", ascending=False)
    st.bar_chart(channel_metrics.set_index("channel")["revenue"])
    st.dataframe(
        channel_metrics.rename(columns={
            "channel": "Channel",
            "sessions": "Sessions",
            "purchases": "Purchases",
            "revenue": "Revenue ($)",
            "conversion_rate": "Conv. Rate (%)",
            "avg_order_value": "AOV ($)",
        }),
        hide_index=True,
        use_container_width=True,
    )

with col_right:
    st.subheader("Revenue by Category")
    cat_metrics = (
        filtered.groupby("category")
        .agg(
            sessions=("sessions", "sum"),
            purchases=("purchases", "sum"),
            revenue=("revenue", "sum"),
        )
        .reset_index()
    )
    cat_metrics["conversion_rate"] = (
        (cat_metrics["purchases"] / cat_metrics["sessions"]) * 100
    ).round(2)
    cat_metrics["avg_order_value"] = (
        cat_metrics["revenue"] / cat_metrics["purchases"]
    ).round(2)
    cat_metrics = cat_metrics.sort_values("revenue", ascending=False)
    st.bar_chart(cat_metrics.set_index("category")["revenue"])
    st.dataframe(
        cat_metrics.rename(columns={
            "category": "Category",
            "sessions": "Sessions",
            "purchases": "Purchases",
            "revenue": "Revenue ($)",
            "conversion_rate": "Conv. Rate (%)",
            "avg_order_value": "AOV ($)",
        }),
        hide_index=True,
        use_container_width=True,
    )

# --- Monthly cohort / unit economics ---
st.divider()
st.subheader("Monthly Performance")

monthly = filtered.copy()
monthly["month"] = monthly["date"].dt.to_period("M").astype(str)
monthly_agg = (
    monthly.groupby("month")
    .agg(
        sessions=("sessions", "sum"),
        purchases=("purchases", "sum"),
        revenue=("revenue", "sum"),
    )
    .reset_index()
)
monthly_agg["conversion_rate"] = ((monthly_agg["purchases"] / monthly_agg["sessions"]) * 100).round(2)
monthly_agg["revenue_per_session"] = (monthly_agg["revenue"] / monthly_agg["sessions"]).round(4)
monthly_agg["avg_order_value"] = (monthly_agg["revenue"] / monthly_agg["purchases"]).round(2)

st.dataframe(
    monthly_agg.rename(columns={
        "month": "Month",
        "sessions": "Sessions",
        "purchases": "Purchases",
        "revenue": "Revenue ($)",
        "conversion_rate": "Conv. Rate (%)",
        "revenue_per_session": "Rev/Session ($)",
        "avg_order_value": "AOV ($)",
    }),
    hide_index=True,
    use_container_width=True,
)

st.divider()
st.caption("Built with Streamlit & Python | Camilo Rojas - Data Analyst")
