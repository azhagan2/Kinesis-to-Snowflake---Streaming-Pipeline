import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import time
from snowflake.snowpark.context import get_active_session


# Get the current credentials
session = get_active_session()
# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Retail POS Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🛒 Retail POS Dashboard - Last 15 Minutes")

# -----------------------------
# Sidebar Filters
# -----------------------------
with st.sidebar:
    st.header("🔍 Filters")
    city_filter = st.text_input("City filter (optional)")
    store_filter = st.text_input("Store ID filter (optional)")

# -----------------------------
# Auto-refresh countdown
# -----------------------------
refresh_interval = 120  # seconds
countdown = st.empty()

# -----------------------------
# Query Function
# -----------------------------
def load_data():
    query = """
SELECT
  store_id AS "store_id",
  city AS "city",
  COUNT(DISTINCT transaction_id) AS "transactions",
  SUM(CASE 
        WHEN is_canceled THEN 0
        WHEN is_refunded THEN (price * quantity) - refund_amount
        ELSE price * quantity
      END) AS "net_sales_amount",
  SUM(quantity) AS "total_units",
  SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END) AS "refunds",
  SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) AS "cancels",
  MIN(timestamp) AS "window_start",
  MAX(timestamp) AS "window_end"
FROM retail_pos_processed

    """

    if city_filter:
        query += f" AND city ILIKE '%{city_filter}%'"
    if store_filter:
        query += f" AND store_id = '{store_filter}'"

    #query += " GROUP BY store_id, city ORDER BY net_sales_amount1 DESC"
    query += " GROUP BY store_id, city "
    
    df = session.sql(query).to_pandas()
    df.columns = [col.lower() for col in df.columns]  # Normalize casing
    return df

# -----------------------------
# Main Loop: Auto-refresh
# -----------------------------
placeholder = st.empty()

while True:
    with placeholder.container():
        df = load_data()
        print(df)

        if df.empty:
            st.warning("No data found for the last 15 minutes.")
        else:
            # Create tabs
            tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "💸 Refunds", "❌ Cancellations"])

            # ----------------- Dashboard Tab -----------------
            with tab1:
                st.subheader("💰 Net Sales Summary")
                st.dataframe(df, use_container_width=True)

                st.subheader("🏬 Top Stores by Net Sales")
                if "store_id" in df.columns and "net_sales_amount" in df.columns and not df.empty:
                    chart_data = df.set_index("store_id")["net_sales_amount"]
                    st.bar_chart(chart_data)
                else:
                    st.warning("Net Sales chart could not be displayed.")
                    st.write("Available columns:", df.columns.tolist())

            # ----------------- Refunds Tab -----------------
            with tab2:
                st.subheader("💸 Refunds by Store")

                if all(col in df.columns for col in ["store_id", "refunds", "transactions"]) and not df.empty:
                    refund_df = df[["store_id", "refunds", "transactions"]].copy()
                    refund_df["refund_rate"] = (refund_df["refunds"] / refund_df["transactions"]) * 100
                    refund_df = refund_df.sort_values(by="refund_rate", ascending=False)

                    st.dataframe(refund_df, use_container_width=True)

                    if not refund_df.empty:
                        st.bar_chart(refund_df.set_index("store_id")["refund_rate"])
                    else:
                        st.warning("No refund data to display.")
                else:
                    st.warning("Refund chart could not be displayed.")
                    st.write("Available columns:", df.columns.tolist())

            # ----------------- Cancellations Tab -----------------
            with tab3:
                st.subheader("❌ Cancellations by Store")

                if all(col in df.columns for col in ["store_id", "cancels", "transactions"]) and not df.empty:
                    cancel_df = df[["store_id", "cancels", "transactions"]].copy()
                    cancel_df["cancel_rate"] = (cancel_df["cancels"] / cancel_df["transactions"]) * 100
                    cancel_df = cancel_df.sort_values(by="cancel_rate", ascending=False)

                    st.dataframe(cancel_df, use_container_width=True)

                    if not cancel_df.empty:
                        st.bar_chart(cancel_df.set_index("store_id")["cancel_rate"])
                    else:
                        st.warning("No cancellation data to display.")
                else:
                    st.warning("Cancellation chart could not be displayed.")
                    st.write("Available columns:", df.columns.tolist())

        # -----------------------------
        # Countdown refresh
        # -----------------------------
        for seconds_left in range(refresh_interval, 0, -1):
            countdown.metric("⏱️ Auto-refresh in", f"{seconds_left}s")
            time.sleep(1)