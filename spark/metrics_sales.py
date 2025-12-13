from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    window,
    sum as sum_,
    avg,
    count
)

# --------------------------------------------------
# Filtrar solo eventos de compra
# --------------------------------------------------
def filter_purchases(events_df: DataFrame) -> DataFrame:
    return events_df.filter(col("event_type") == "purchase")


# --------------------------------------------------
# Total de ventas por minuto
# --------------------------------------------------
def sales_per_minute(purchases_df: DataFrame) -> DataFrame:
    return (
        purchases_df
	.withWatermark("timestamp", "2 minutes")
        .groupBy(
            window(col("timestamp"), "1 minute")
        )
        .agg(
            sum_("price").alias("total_sales"),
            count("*").alias("total_orders")
        )
    )


# --------------------------------------------------
# Promedio de ticket por usuario
# --------------------------------------------------
def avg_ticket_per_user(purchases_df: DataFrame) -> DataFrame:
    return (
        purchases_df
        .groupBy("user_id")
        .agg(
            avg("price").alias("avg_ticket")
        )
    )


# --------------------------------------------------
# Ventas por producto (base para picos de demanda)
# --------------------------------------------------
def sales_by_product(purchases_df: DataFrame) -> DataFrame:
    return (
        purchases_df
        .groupBy(
            "product_id",
            window(col("timestamp"), "1 minute")
        )
        .agg(
            count("*").alias("orders_count"),
            sum_("price").alias("product_revenue")
        )
    )
