from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    get_json_object,
    to_date,
    to_timestamp,
)

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    RAW_CHECKPOINT_PATH,
    RAW_DELIVERY_EVENT_PATH,
)


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaToRawDeliveryEvents")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    raw_df = (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("event_id", get_json_object(col("raw_payload"), "$.event_id"))
        .withColumn("event_timestamp_str", get_json_object(col("raw_payload"), "$.event_timestamp"))
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp_str")))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("ingested_at", current_timestamp())
        .select(
            "event_id",
            "event_date",
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "raw_payload",
            "ingested_at",
        )
    )

    query = (
        raw_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", RAW_DELIVERY_EVENT_PATH)
        .option("checkpointLocation", RAW_CHECKPOINT_PATH)
        .partitionBy("event_date")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()