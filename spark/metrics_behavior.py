from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc

# --------------------------------------------------
# Filtrar eventos por tipo
# --------------------------------------------------
def filter_views(events_df: DataFrame) -> DataFrame:
    return events_df.filter(col("event_type") == "view")


def filter_cart_events(events_df: DataFrame) -> DataFrame:
    return events_df.filter(col("event_type") == "add_to_cart")


# --------------------------------------------------
# Conteo de visitas por categoria
# --------------------------------------------------
def views_by_category(views_df: DataFrame) -> DataFrame:
    return (
        views_df
        .groupBy("category")
        .agg(count("*").alias("views_count"))
    )


# --------------------------------------------------
# Top 5 productos mas vistos
# --------------------------------------------------
def top_products_by_views(views_df: DataFrame) -> DataFrame:
    return (
        views_df
        .groupBy("product_id")
        .agg(count("*").alias("views_count"))
        .orderBy(desc("views_count"))
        .limit(5)
    )


# --------------------------------------------------
# Conversion funnel: vista → carrito → compra
# --------------------------------------------------
def conversion_funnel(events_df: DataFrame) -> DataFrame:
    views = events_df.filter(col("event_type") == "view").select("user_id", "product_id")
    carts = events_df.filter(col("event_type") == "add_to_cart").select("user_id", "product_id")
    purchases = events_df.filter(col("event_type") == "purchase").select("user_id", "product_id")

    funnel = (
        views
        .join(carts, ["user_id", "product_id"], "left")
        .join(purchases, ["user_id", "product_id"], "left")
    )

    return funnel
