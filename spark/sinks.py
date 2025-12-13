from pyspark.sql import DataFrame

# --------------------------------------------------
# Escribir stream a consola (debug)
# --------------------------------------------------
def write_to_console(df: DataFrame, query_name: str):
    output_mode = "complete" if ds.isStreaming and df.columns else "append"
    return (
        df.writeStream
        .outputMode(output_mode)
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
