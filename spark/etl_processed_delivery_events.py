from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    hour,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config import PROCESSED_DELIVERY_EVENT_PATH, RAW_DELIVERY_EVENT_PATH

schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("shipment_id", StringType(), True),
        StructField("dispatch_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("origin_region_id", StringType(), True),
        StructField("destination_region_id", StringType(), True),
        StructField("origin_hub_id", StringType(), True),
        StructField("destination_hub_id", StringType(), True),
        StructField("current_hub_id", StringType(), True),
        StructField("next_hub_id", StringType(), True),
        StructField("cargo_type", StringType(), True),
        StructField("cargo_weight_kg", DoubleType(), True),
        StructField("shipping_costs", DoubleType(), True),
        StructField("lead_time_days", DoubleType(), True),
        StructField("promised_delivery_at", StringType(), True),
        StructField("event_status", StringType(), True),
        StructField("event_sequence", IntegerType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("processing_timestamp", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("route_step", IntegerType(), True),
        StructField("route_total_steps", IntegerType(), True),
        StructField("traffic_congestion_level", DoubleType(), True),
        StructField("weather_severity", DoubleType(), True),
        StructField("hub_congestion_level", DoubleType(), True),
        StructField("eta_variation_minutes", DoubleType(), True),
        StructField("delay_probability", DoubleType(), True),
        StructField("risk_classification", StringType(), True),
        StructField("exception_type", StringType(), True),
    ]
)


def main():
    spark = (
        SparkSession.builder
        .appName("RawToProcessedDeliveryEvents")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.read.parquet(RAW_DELIVERY_EVENT_PATH)

    processed_df = (
        raw_df
        .select(from_json(col("raw_payload"), schema).alias("data"))
        .select("data.*")
        .filter(col("event_id").isNotNull())
        .filter(col("shipment_id").isNotNull())
        .filter(col("event_status").isNotNull())
        .dropDuplicates(["event_id"])
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("processing_timestamp", to_timestamp(col("processing_timestamp")))
        .withColumn("promised_delivery_at", to_timestamp(col("promised_delivery_at")))
        .filter(col("event_timestamp").isNotNull())
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("is_created", when(col("event_status") == "CREATED", True).otherwise(False))
        .withColumn("is_assigned", when(col("event_status") == "ASSIGNED", True).otherwise(False))
        .withColumn("is_pickup", when(col("event_status") == "PICKUP", True).otherwise(False))
        .withColumn("is_in_transit", when(col("event_status") == "IN_TRANSIT", True).otherwise(False))
        .withColumn("is_arrived_hub", when(col("event_status") == "ARRIVED_HUB", True).otherwise(False))
        .withColumn("is_out_for_delivery", when(col("event_status") == "OUT_FOR_DELIVERY", True).otherwise(False))
        .withColumn("is_delivered", when(col("event_status") == "DELIVERED", True).otherwise(False))
        .withColumn("is_exception", when(col("exception_type").isNotNull(), True).otherwise(False))
        .withColumn(
            "is_event_delayed",
            when(
                (col("delay_probability") >= 0.5)
                | (col("eta_variation_minutes") >= 30)
                | (col("exception_type").isNotNull()),
                True,
            ).otherwise(False),
        )
        .withColumn("processed_at", current_timestamp())
        .orderBy("shipment_id", "event_sequence", "event_timestamp")
    )

    (
        processed_df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(PROCESSED_DELIVERY_EVENT_PATH)
    )

    print("Raw delivery_event → Processed delivery_event 완료")
    spark.stop()


if __name__ == "__main__":
    main()