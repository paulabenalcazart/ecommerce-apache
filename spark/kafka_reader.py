from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql import DataFrame
from schemas import event_schema


def read_kafka_events(
    spark,
    bootstrap_servers: str,
    topics: str
) -> DataFrame:
    """
    Lee eventos desde Kafka y devuelve un DataFrame estructurado
    """

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topics)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .select(
            from_json(col("value").cast("string"), event_schema).alias("data")
        )
        .select("data.*")
	.withColumn("timestamp", to_timestamp(col("timestamp")))
    )

    # Filtrado basico de eventos validos
    filtered_df = (
        parsed_df
        .filter(col("event_type").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("timestamp").isNotNull())
    )

    return filtered_df
