from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    round as spark_round,
    sum as spark_sum,
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
        .appName("BuildDeliveryEventDataMart")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(PROCESSED_DELIVERY_EVENT_PATH)

    region_delay_mart = (
        df.groupBy("event_date", "destination_region_id")
        .agg(
            count("*").alias("total_event_count"),
            spark_sum(
                when(col("is_event_delayed") == True, 1).otherwise(0)
            ).alias("delayed_event_count"),
            avg("delay_probability").alias("avg_delay_probability"),
            avg("eta_variation_minutes").alias("avg_eta_variation_minutes"),
            spark_sum(
                when(col("risk_classification") == "HIGH", 1).otherwise(0)
            ).alias("high_risk_count"),
        )
        .withColumn(
            "delay_rate",
            spark_round(col("delayed_event_count") / col("total_event_count"), 4),
        )
        .withColumn("created_at", current_timestamp())
        .select(
            "event_date",
            "destination_region_id",
            "total_event_count",
            "delayed_event_count",
            "delay_rate",
            "avg_delay_probability",
            "avg_eta_variation_minutes",
            "high_risk_count",
            "created_at",
        )
    )

    hub_performance_mart = (
        df.groupBy("event_date", "destination_hub_id")
        .agg(
            count("*").alias("total_event_count"),
            spark_sum(
                when(col("is_event_delayed") == True, 1).otherwise(0)
            ).alias("delayed_event_count"),
            avg("traffic_congestion_level").alias("avg_traffic_congestion_level"),
            avg("hub_congestion_level").alias("avg_hub_congestion_level"),
            avg("delay_probability").alias("avg_delay_probability"),
        )
        .withColumn(
            "delay_rate",
            spark_round(col("delayed_event_count") / col("total_event_count"), 4),
        )
        .withColumn("created_at", current_timestamp())
        .select(
            "event_date",
            "destination_hub_id",
            "total_event_count",
            "delayed_event_count",
            "delay_rate",
            "avg_traffic_congestion_level",
            "avg_hub_congestion_level",
            "avg_delay_probability",
            "created_at",
        )
    )

    risk_summary_mart = (
        df.groupBy("event_date", "risk_classification")
        .agg(
            count("*").alias("event_count"),
            avg("delay_probability").alias("avg_delay_probability"),
            avg("weather_severity").alias("avg_weather_severity"),
            avg("traffic_congestion_level").alias("avg_traffic_congestion_level"),
        )
        .withColumn("created_at", current_timestamp())
        .select(
            "event_date",
            "risk_classification",
            "event_count",
            "avg_delay_probability",
            "avg_weather_severity",
            "avg_traffic_congestion_level",
            "created_at",
        )
    )

    write_to_postgres_overwrite(region_delay_mart, "mart_region_delay")
    write_to_postgres_overwrite(hub_performance_mart, "mart_hub_performance")
    write_to_postgres_overwrite(risk_summary_mart, "mart_risk_summary")

    print("Data Mart 생성 완료")
    spark.stop()


if __name__ == "__main__":
    main()