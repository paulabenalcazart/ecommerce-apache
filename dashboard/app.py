import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col

# ----------------------------------------
# Spark session (batch, no streaming)
# ----------------------------------------
spark = (
    SparkSession.builder
    .appName("EcommerceDashboard")
    .master("local[*]")
    .getOrCreate()
)

st.set_page_config(page_title="E-Commerce Analytics", layout="wide")
st.title("📊 E-Commerce Analytics Dashboard")

# ----------------------------------------
# Ventas por minuto
# ----------------------------------------
st.header("Ventas por minuto")

sales_df = spark.read.parquet(
    "hdfs://localhost:9000/ecommerce/sales_per_minute/"
)

sales_df = sales_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_sales"),
    col("total_orders")
)


sales_pd = sales_df.toPandas()

st.line_chart(
    sales_pd,
    x="window_start",
    y="total_sales"
)

# ----------------------------------------
# Promedio de ticket por usuario
# ----------------------------------------
st.header("Promedio de ticket por usuario")

avg_ticket_df = spark.read.parquet(
    "hdfs://localhost:9000/ecommerce/orders_avg_ticket/"
)

avg_pd = avg_ticket_df.toPandas()

st.bar_chart(
    avg_pd,
    x="user_id",
    y="avg_ticket"
)

# ----------------------------------------
# Vistas por categoria
# ----------------------------------------
st.header("Vistas por categoria")

views_df = spark.read.parquet(
    "hdfs://localhost:9000/ecommerce/views_by_category/"
)

views_pd = views_df.toPandas()

st.bar_chart(
    views_pd,
    x="category",
    y="views_count"
)
