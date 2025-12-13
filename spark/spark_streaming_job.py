from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from schemas import ecommerce_schema
from kafka_reader import read_kafka_stream
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
from sinks import write_to_console, write_to_hdfs

# --------------------------------------------------
# Inicializar Spark
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .master("local[*]") \   # reemplaza por "spark://<master>:7077" si es standalone cluster
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Leer stream desde Kafka
# --------------------------------------------------
events_df = read_kafka_stream(
    spark,
    topic="orders,cart_events,page_views",
    schema=ecommerce_schema
)

# --------------------------------------------------
# METRICAS DE VENTAS
# --------------------------------------------------
purchases_df = filter_purchases(events_df)

sales_per_minute_df = sales_per_minute(purchases_df)
avg_ticket_df = avg_ticket_per_user(purchases_df)
sales_by_product_df = sales_by_product(purchases_df)

# --------------------------------------------------
# METRICAS DE COMPORTAMIENTO
# --------------------------------------------------
views_df = filter_views(events_df)
cart_df = filter_cart_events(events_df)

views_by_category_df = views_by_category(views_df)
top_products_df = top_products_by_views(views_df)
conversion_df = conversion_funnel(events_df)

# --------------------------------------------------
# SALIDAS (consola + HDFS)
# --------------------------------------------------
queries = []

# Consola (debug)
queries.append(write_to_console(sales_per_minute_df, "SalesPerMinute"))
queries.append(write_to_console(avg_ticket_df, "AvgTicketPerUser"))
queries.append(write_to_console(views_by_category_df, "ViewsByCategory"))
queries.append(write_to_console(top_products_df, "TopProducts"))
queries.append(write_to_console(conversion_df, "ConversionFunnel"))

# HDFS (persistencia)
queries.append(write_to_hdfs(sales_per_minute_df, "/ecommerce/orders/", "/ecommerce/checkpoints/orders"))
queries.append(write_to_hdfs(avg_ticket_df, "/ecommerce/orders_avg_ticket/", "/ecommerce/checkpoints/orders_avg_ticket"))
queries.append(write_to_hdfs(views_by_category_df, "/ecommerce/views/", "/ecommerce/checkpoints/views"))
queries.append(write_to_hdfs(top_products_df, "/ecommerce/top_products/", "/ecommerce/checkpoints/top_products"))
queries.append(write_to_hdfs(conversion_df, "/ecommerce/conversion/", "/ecommerce/checkpoints/conversion"))

# --------------------------------------------------
# Ejecutar streaming
# --------------------------------------------------
for q in queries:
    q.awaitTermination()

