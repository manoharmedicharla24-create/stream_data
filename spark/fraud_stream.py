import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    lit,
    concat,
    count
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from fraud_detection.fraud_rules import calculate_fraud_score


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("FraudDetectionStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# TRANSACTION SCHEMA
# ============================================================

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("merchant", StringType(), True),
    StructField("location", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transaction_type", StringType(), True)
])


# ============================================================
# KAFKA STREAM
# ============================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "transactions"
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)


# ============================================================
# PARSE KAFKA JSON
# ============================================================

json_df = kafka_df.select(
    col("value").cast("string").alias("json")
)


transactions_df = (
    json_df
    .select(
        from_json(
            col("json"),
            transaction_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# CONVERT TIMESTAMP
# ============================================================

transactions_with_timestamp = (
    transactions_df
    .withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )
)


# ============================================================
# FRAUD SCORE
# ============================================================

fraud_df = calculate_fraud_score(
    transactions_with_timestamp
)


# ============================================================
# POSTGRES CONFIGURATION
# ============================================================

POSTGRES_URL = (
    "jdbc:postgresql://localhost:5434/fraud_detection"
)

POSTGRES_PROPERTIES = {
    "user": "fraud_user",
    "password": "fraud_password",
    "driver": "org.postgresql.Driver"
}


# ============================================================
# QUERY 1
# WRITE NORMAL TRANSACTIONS
# ============================================================

def write_to_postgres(batch_df, batch_id):

    print(
        f"\n========== TRANSACTION BATCH {batch_id} =========="
    )

    output_df = batch_df.select(
        col("transaction_id"),
        col("user_id"),
        col("amount"),
        col("merchant"),
        col("location"),
        col("device_id"),
        col("event_time")
        .alias("transaction_timestamp"),
        col("transaction_type"),
        col("fraud_score"),
        col("risk_level")
    )

    # --------------------------------------------------------
    # WRITE ALL TRANSACTIONS
    # --------------------------------------------------------

    output_df.write.jdbc(
        url=POSTGRES_URL,
        table="transactions",
        mode="append",
        properties=POSTGRES_PROPERTIES
    )

    print(
        f"Batch {batch_id} written to transactions"
    )

    # --------------------------------------------------------
    # HIGH-RISK TRANSACTIONS
    # --------------------------------------------------------

    high_risk_df = output_df.filter(
        col("risk_level") == "HIGH"
    )

    if not high_risk_df.rdd.isEmpty():

        alerts_df = high_risk_df.select(
            col("transaction_id"),
            col("user_id"),
            col("amount"),
            col("fraud_score"),
            col("risk_level"),
            col("transaction_type").alias("reason")
        )

        alerts_df.write.jdbc(
            url=POSTGRES_URL,
            table="fraud_alerts",
            mode="append",
            properties=POSTGRES_PROPERTIES
        )

        print(
            f"🚨 HIGH-RISK ALERTS CREATED: "
            f"{alerts_df.count()}"
        )

    else:

        print(
            "No HIGH-risk fraud in this batch"
        )


# ============================================================
# QUERY 1
# START NORMAL FRAUD PIPELINE
# ============================================================

transaction_query = (
    fraud_df
    .writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/tmp/fraud_detection_checkpoint"
    )
    .start()
)


# ============================================================
# VELOCITY DETECTION
# ============================================================

velocity_df = (
    transactions_with_timestamp
    .withWatermark(
        "event_time",
        "2 minutes"
    )
    .groupBy(
        window(
            col("event_time"),
            "1 minute"
        ),
        col("user_id")
    )
    .agg(
        count("*").alias("transaction_count")
    )
    .filter(
        col("transaction_count") > 3
    )
)


# ============================================================
# VELOCITY ALERT WRITER
# ============================================================

def write_velocity_alerts(batch_df, batch_id):

    print(
        f"\n========== VELOCITY BATCH {batch_id} =========="
    )

    if batch_df.rdd.isEmpty():

        print(
            "No suspicious transaction velocity detected"
        )

        return

    velocity_alerts = batch_df.select(

        lit(None)
        .cast("string")
        .alias("transaction_id"),

        col("user_id"),

        lit(None)
        .cast("double")
        .alias("amount"),

        lit(25)
        .cast("int")
        .alias("fraud_score"),

        lit("HIGH")
        .alias("risk_level"),

        concat(
            lit("VELOCITY_FRAUD: "),
            col("transaction_count"),
            lit(" transactions within 1 minute")
        ).alias("reason")
    )

    velocity_alerts.write.jdbc(
        url=POSTGRES_URL,
        table="fraud_alerts",
        mode="append",
        properties=POSTGRES_PROPERTIES
    )

    print(
        "🚨 VELOCITY FRAUD ALERT CREATED"
    )

    velocity_alerts.show(
        truncate=False
    )


# ============================================================
# QUERY 2
# START VELOCITY DETECTION
# ============================================================

velocity_query = (
    velocity_df
    .writeStream
    .foreachBatch(write_velocity_alerts)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/tmp/fraud_velocity_checkpoint"
    )
    .start()
)


# ============================================================
# WAIT FOR BOTH STREAMS
# ============================================================

spark.streams.awaitAnyTermination()