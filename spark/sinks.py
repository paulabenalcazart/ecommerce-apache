
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc

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
          .partitionBy("fecha")
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
def write_top_products_to_hdfs(
    df: DataFrame,
    path: str,
    checkpoint_path: str,
    top_n: int = 5
):
    def foreach_batch(batch_df, batch_id):
        (
            batch_df
            .orderBy(desc("views_count"))  
            .limit(top_n)
            .write
            .mode("overwrite")           
            .parquet(path)
        )

    return (
        df.writeStream
          .outputMode("complete")         
          .option("checkpointLocation", checkpoint_path)
          .foreachBatch(foreach_batch)
          .start()
    )
