from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    desc,
    expr
)

# ==================================================
# FILTRADO DE EVENTOS
# ==================================================

def filter_views(events_df: DataFrame) -> DataFrame:
    return events_df.filter(col("event_type") == "page_view")


def filter_cart_events(events_df: DataFrame) -> DataFrame:
    return events_df.filter(col("event_type") == "add_to_cart")


def filter_purchases(events_df: DataFrame) -> DataFrame:
    return events_df.filter(col("event_type") == "purchase")


# ==================================================
# CONTEO DE VISITAS POR CATEGORÍA
# ==================================================

def views_by_category(views_df: DataFrame) -> DataFrame:
    return (
        views_df
        .groupBy("category")
        .agg(count("*").alias("views_count"))
    )


# ==================================================
# TOP 5 PRODUCTOS MÁS VISTOS
# ⚠️ Usar SOLO en consola con outputMode("complete")
# ==================================================

def top_products_by_views(views_df: DataFrame) -> DataFrame:
    return (
        views_df
        .groupBy("product_id")
        .agg(count("*").alias("views_count"))
        .orderBy(desc("views_count"))
        .limit(5)
    )


# ==================================================
# CONVERSION FUNNEL
# JOIN ENTRE STREAMS (carrito + compra)
# ==================================================
# Requisito:
# - stream–stream join
# - watermark
# - condición temporal
# - outputMode("append")
# ==================================================

def conversion_funnel(events_df: DataFrame) -> DataFrame:
    carts = (
        events_df
        .filter(col("event_type") == "add_to_cart")
        .select("user_id", "product_id", "timestamp")
        .withWatermark("timestamp", "30 minutes")
    )

    purchases = (
        events_df
        .filter(col("event_type") == "purchase")
        .select("user_id", "product_id", "timestamp")
        .withWatermark("timestamp", "30 minutes")
    )

    funnel = (
        carts.alias("c")
        .join(
            purchases.alias("p"),
            expr("""
                c.user_id = p.user_id AND
                c.product_id = p.product_id AND
                p.timestamp BETWEEN c.timestamp
                                AND c.timestamp + interval 20 minutes
            """),
            "inner"
        )
    )

    return funnel
