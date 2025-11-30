from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import altair as alt
import pandas as pd
import streamlit as st

# -------------------------------
# Connection and page settings
# -------------------------------
session: Session = get_active_session()
st.set_page_config(layout="wide")

# -------------------------------
# Utils
# -------------------------------
def load_product_data() -> pd.DataFrame:
    """Loads the inventory data from the silver layer in retail_analytics_db."""

    data = session.sql(
        """SELECT
               ID,
               IMAGE,
               ITEM_NAME,
               PRICE,
               UNITS_SOLD,
               UNITS_LEFT,
               COST_PRICE,
               REORDER_POINT,
               DESCRIPTION,
               LAST_UPDATED
           FROM
               retail_analytics_db.silver.inventory"""
    ).toPandas()

    data.rename(
        columns={
            "ID": "ID",
            "IMAGE": "Image",
            "ITEM_NAME": "Item Name",
            "PRICE": "Price",
            "UNITS_SOLD": "Units Sold",
            "UNITS_LEFT": "Units Left",
            "COST_PRICE": "Cost Price",
            "REORDER_POINT": "Reorder Point",
            "DESCRIPTION": "Description",
            "LAST_UPDATED": "Last Updated"
        },
        inplace=True,
    )

    return data

# -----------------------------------------------------------------------------
# Draw the actual page, starting with the inventory table.
# -----------------------------------------------------------------------------

st.title("Inventory Tracker 📊")

product_data = load_product_data()

st.dataframe(product_data, use_container_width=True)

# -----------------------------------------------------------------------------
# Now some cool charts
# -----------------------------------------------------------------------------

st.subheader("Stocks left in Store", divider="green")

need_to_reorder = product_data[
    product_data["Units Left"] <= product_data["Reorder Point"]
].loc[:, "Item Name"]

if len(need_to_reorder) > 0:
    items = "\n".join(f"* {name}" for name in need_to_reorder)
    st.warning(f"We're running dangerously low on the items below:\n {items}")

st.altair_chart(
    alt.Chart(product_data)
    .mark_bar(orient="horizontal", color="#52c234")
    .encode(
        x="Units Sold:Q",
        y=alt.Y("Item Name:N", sort="-x")
    )
    + alt.Chart(product_data)
    .mark_point(
        shape="diamond",
        filled=True,
        size=50,
        color="#061700",
        opacity=1,
    )
    .encode(
        x="Reorder Point:Q",
        y="Item Name:N",
    ),
    use_container_width=True,
)

st.caption("NOTE: The :diamonds: location shows the reorder point.")

st.subheader("Selling faster", divider="green")

st.altair_chart(
    alt.Chart(product_data)
    .mark_bar(orient="horizontal", color="#1D976C")
    .encode(
        x="Units Sold:Q",
        y=alt.Y("Item Name:N").sort("-x"),
    ),
    use_container_width=True,
)

st.subheader("Top Profitable Products", divider="green")

# Calculate total profit for each item
product_data["Total Profit"] = (product_data["Price"] - product_data["Cost Price"]) * product_data["Units Sold"]

# Create a bar chart for top 10 profitable products
top_profit_chart = (
    alt.Chart(product_data.sort_values("Total Profit", ascending=False).head(10))
    .mark_bar(color="#FF5733")
    .encode(
        x="Total Profit:Q",
        y=alt.Y("Item Name:N", sort="-x"),
        tooltip=["Item Name", "Total Profit", "Units Sold"]
    )
)

st.altair_chart(top_profit_chart, use_container_width=True)

st.caption("NOTE: This chart shows the products generating the highest total profit.")


@st.cache_data
def load_store_sales_data() -> pd.DataFrame:
    """Join dim_store with POS data and calculate aggregated sales."""
    query = """
        SELECT
            ds.REGION,
            ds.CITY,
            ds.STORE_NAME,
            ds.IS_ACTIVE,
            SUM(rpp.TOTAL_AMOUNT) AS TOTAL_SALES,
            COUNT(DISTINCT rpp.TRANSACTION_ID) AS NUM_TRANSACTIONS
        FROM retail_analytics_db.SILVER.DIM_STORE ds
        JOIN retail_analytics_db.SILVER.RETAIL_POS_PROCESSED rpp
            ON ds.STORE_ID = rpp.STORE_ID
        WHERE rpp.IS_CANCELED = FALSE
        GROUP BY ds.REGION, ds.CITY, ds.STORE_NAME, ds.IS_ACTIVE
    """
    return session.sql(query).toPandas()


st.subheader("Store-Level Sales Insights", divider="green")

store_sales = load_store_sales_data()

st.dataframe(store_sales, use_container_width=True)

# Region-wise Total Sales
region_chart = (
    alt.Chart(store_sales)
    .mark_bar(color="#FFB347")
    .encode(
        x=alt.X("TOTAL_SALES:Q", title="Total Sales ($)", scale=alt.Scale(zero=True)),
        y=alt.Y("REGION:N", sort="-x"),
        tooltip=["REGION", "TOTAL_SALES"]
    )
)

st.altair_chart(region_chart, use_container_width=True)

# City-wise Transactions
city_chart = (
    alt.Chart(store_sales)
    .mark_bar(color="#87CEEB")
    .encode(
        x=alt.X("NUM_TRANSACTIONS:Q", title="Number of Transactions"),
        y=alt.Y("CITY:N", sort="-x"),
        tooltip=["CITY", "NUM_TRANSACTIONS"]
    )
)

st.altair_chart(city_chart, use_container_width=True)

