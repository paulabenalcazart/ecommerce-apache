from pyspark.sql import DataFrame

# ==================================================
# ESCRIBIR STREAM A CONSOLA (DEBUG)
# ==================================================

def write_to_console(
    df: DataFrame,
    query_name: str,
    output_mode: str = "update"
):
    return (
        df.writeStream
          .format("console")
          .outputMode(output_mode)
          .option("truncate", False)
          .queryName(query_name)
          .start()
    )


# ==================================================
# ESCRIBIR STREAM CRUDO A HDFS (append)
# SOLO para datos SIN agregación
# ==================================================

def write_raw_to_hdfs(
    df: DataFrame,
    path: str,
    checkpoint_path: str
):
    return (
        df.writeStream
          .format("parquet")
          .outputMode("append")
          .option("path", path)
          .option("checkpointLocation", checkpoint_path)
          .start()
    )


# ==================================================
# ESCRIBIR AGREGACIONES A HDFS (foreachBatch)
# ==================================================

def write_agg_to_hdfs(
    df: DataFrame,
    path: str,
    checkpoint_path: str,
    mode: str = "overwrite"  # overwrite es lo normal para métricas
):
    return (
        df.writeStream
          .outputMode("update")  # o "complete"
          .option("checkpointLocation", checkpoint_path)
          .foreachBatch(
              lambda batch_df, batch_id:
                  batch_df.write.mode(mode).parquet(path)
          )
          .start()
    )
