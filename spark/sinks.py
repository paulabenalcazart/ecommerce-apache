from pyspark.sql import DataFrame

# --------------------------------------------------
# Escribir stream a consola (debug)
# --------------------------------------------------
def write_to_console(df: DataFrame, query_name: str):
    return (
        df.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .queryName(query_name)
        .start()
    )


# --------------------------------------------------
# Escribir stream a HDFS en formato Parquet
# --------------------------------------------------
def write_to_hdfs(
    df: DataFrame,
    path: str,
    checkpoint_path: str
):
    return (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
    )
