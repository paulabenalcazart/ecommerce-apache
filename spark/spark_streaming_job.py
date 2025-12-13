from pyspark.sql import SparkSession

from pyspark.sql.functions import to_date, col

from kafka_reader import read_kafka_events

from metrics_sales import (
    filter_purchases,
    sales_per_minute,
    avg_ticket_per_user,
    sales_by_product
)

from metrics_behavior import (
    filter_views,
    filter_cart_events,
    views_by_category,
    top_products_by_views,
    conversion_funnel
)

from sinks import (
    write_to_console,
    write_raw_to_hdfs,
    write_agg_to_hdfs
)

# ==================================================
# INICIALIZAR SPARK
# ==================================================

spark = (
    SparkSession.builder
    .appName("EcommerceStreaming")
    .master("local[*]")  # tu compañero puede cambiar a spark://host:7077
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==================================================
# LEER STREAM DESDE KAFKA
# ==================================================

events_df = read_kafka_events(
    spark,
    bootstrap_servers="localhost:9092",
    topics="orders,cart_events,page_views"
)

# ==================================================
# FILTRADO DE EVENTOS
# ==================================================

purchases_df = filter_purchases(events_df)
views_df = filter_views(events_df)
cart_df = filter_cart_events(events_df)

purchases_df = purchases_df.withColumn("fecha", to_date(col("timestamp")))
views_df = views_df.withColumn("fecha", to_date(col("timestamp")))
cart_df = cart_df.withColumn("fecha", to_date(col("timestamp")))

# ==================================================
# MÉTRICAS DE VENTAS
# ==================================================

sales_per_minute_df = sales_per_minute(purchases_df)
avg_ticket_df = avg_ticket_per_user(purchases_df)
sales_by_product_df = sales_by_product(purchases_df)

# ==================================================
# MÉTRICAS DE COMPORTAMIENTO
# ==================================================

views_by_category_df = views_by_category(views_df)
top_products_df = top_products_by_views(views_df)

# JOIN ENTRE STREAMS (carrito + compra)
conversion_df = conversion_funnel(events_df)

# ==================================================
# SALIDAS A CONSOLA (DEBUG / VISUALIZACIÓN)
# ==================================================

queries = []

queries.append(
    write_to_console(
        sales_per_minute_df,
        query_name="SalesPerMinute",
        output_mode="update"
    )
)

queries.append(
    write_to_console(
        avg_ticket_df,
        query_name="AvgTicketPerUser",
        output_mode="update"
    )
)

queries.append(
    write_to_console(
        views_by_category_df,
        query_name="ViewsByCategory",
        output_mode="update"
    )
)

queries.append(
    write_to_console(
        top_products_df,
        query_name="TopProducts",
        output_mode="complete"
    )
)

queries.append(
    write_to_console(
        conversion_df,
        query_name="ConversionFunnel",
        output_mode="append"
    )
)

# ==================================================
# SALIDAS A HDFS
# ==================================================

# -------- STREAM CRUDO (append) --------
queries.append(
    write_raw_to_hdfs(
        purchases_df,
        "/ecommerce/orders/",
        "/ecommerce/checkpoints/orders"
    )
)

queries.append(
    write_raw_to_hdfs(
	views_df,
	"/ecommerce/views/",
	"/ecommerce/checkpoints/view"
    )
)

queries.append(
    write_raw_to_hdfs(
	cart_df,
	"/ecommerce/cart",
	"/ecommerce/checkpoints/cart"
    )
)

# -------- AGREGACIONES (foreachBatch) --------
queries.append(
    write_agg_to_hdfs(
        sales_per_minute_df,
        "/ecommerce/sales_per_minute/",
        "/ecommerce/checkpoints/sales_per_minute"
    )
)

queries.append(
    write_agg_to_hdfs(
        avg_ticket_df,
        "/ecommerce/orders_avg_ticket/",
        "/ecommerce/checkpoints/orders_avg_ticket"
    )
)

queries.append(
    write_agg_to_hdfs(
        views_by_category_df,
        "/ecommerce/views_by_category/",
        "/ecommerce/checkpoints/views_by_category"
    )
)

# ==================================================
# EJECUTAR STREAMING
# ==================================================

for q in queries:
    q.awaitTermination()
