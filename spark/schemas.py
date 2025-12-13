from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

# Schema principal de eventos de e-commerce
event_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("event_type", StringType(), True),  # view | add_to_cart | purchase
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True),

    # Informacion de la tienda (opcional)
    StructField("store_id", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("province", StringType(), True)
])
