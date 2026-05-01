from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    first,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    unix_timestamp,
    when,
)

from config import (
    POSTGRES_PASSWORD,
    POSTGRES_URL,
    POSTGRES_USER,
    PROCESSED_DELIVERY_EVENT_PATH,
)


def write_to_postgres_overwrite(df, table_name):
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def main():
    spark = (
        SparkSession.builder
        .appName("BuildShipmentMetrics")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(PROCESSED_DELIVERY_EVENT_PATH)

    # 이벤트 스트림 순서 보장
    df = df.orderBy("shipment_id", "event_sequence", "event_timestamp")

    metrics_df = (
        df.groupBy("shipment_id")
        .agg(
            first("dispatch_id", ignorenulls=True).alias("dispatch_id"),
            first("driver_id", ignorenulls=True).alias("driver_id"),
            first("vehicle_id", ignorenulls=True).alias("vehicle_id"),
            first("origin_region_id", ignorenulls=True).alias("origin_region_id"),
            first("destination_region_id", ignorenulls=True).alias("destination_region_id"),
            first("origin_hub_id", ignorenulls=True).alias("origin_hub_id"),
            first("destination_hub_id", ignorenulls=True).alias("destination_hub_id"),

            spark_min("event_timestamp").alias("first_event_time"),
            spark_max(
                when(col("event_status") == "DELIVERED", col("event_timestamp"))
            ).alias("delivered_time"),

            first("promised_delivery_at", ignorenulls=True).alias("promised_delivery_at"),

            count("*").alias("event_count"),
            spark_max("event_sequence").alias("final_event_sequence"),

            avg("delay_probability").alias("avg_delay_probability"),
            spark_max("delay_probability").alias("max_delay_probability"),
            avg("traffic_congestion_level").alias("avg_traffic_congestion_level"),
            avg("weather_severity").alias("avg_weather_severity"),
            avg("hub_congestion_level").alias("avg_hub_congestion_level"),

            spark_sum(
                when(col("exception_type").isNotNull(), 1).otherwise(0)
            ).alias("exception_count"),

            spark_max("risk_classification").alias("final_risk_classification"),
        )
        .withColumn(
            "total_delivery_minutes",
            when(
                col("delivered_time").isNotNull(),
                (unix_timestamp(col("delivered_time")) - unix_timestamp(col("first_event_time"))) / 60,
            ).otherwise(None),
        )
        .withColumn(
            "delay_minutes",
            when(
                col("delivered_time").isNotNull(),
                (unix_timestamp(col("delivered_time")) - unix_timestamp(col("promised_delivery_at"))) / 60,
            ).otherwise(None),
        )
        .withColumn(
            "is_delayed",
            when(col("delay_minutes") > 0, True).otherwise(False),
        )
        .withColumn("created_at", current_timestamp())
        .select(
            "shipment_id",
            "dispatch_id",
            "driver_id",
            "vehicle_id",
            "origin_region_id",
            "destination_region_id",
            "origin_hub_id",
            "destination_hub_id",
            "first_event_time",
            "delivered_time",
            "total_delivery_minutes",
            "promised_delivery_at",
            "delay_minutes",
            "is_delayed",
            "event_count",
            "final_event_sequence",
            "avg_delay_probability",
            "max_delay_probability",
            "avg_traffic_congestion_level",
            "avg_weather_severity",
            "avg_hub_congestion_level",
            "exception_count",
            "final_risk_classification",
            "created_at",
        )
    )

    write_to_postgres_overwrite(metrics_df, "shipment_metrics")

    print("shipment_metrics 생성 완료")
    spark.stop()


if __name__ == "__main__":
    main()