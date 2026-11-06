from pyspark.sql.functions import col, when, lit


def calculate_fraud_score(df):

    # ============================================================
    # START FRAUD SCORE AT 0
    # ============================================================

    df = df.withColumn(
        "fraud_score",
        lit(0)
    )

    # ============================================================
    # RULE 1: TRANSACTION AMOUNT
    # ============================================================

    df = df.withColumn(
        "fraud_score",
        col("fraud_score")
        + when(col("amount") > 300000, 80)
        .when(col("amount") > 100000, 40)
        .when(col("amount") > 50000, 20)
        .otherwise(0)
    )

    # ============================================================
    # RULE 2: SUSPICIOUS LOCATION
    # ============================================================

    df = df.withColumn(
        "fraud_score",
        col("fraud_score")
        + when(
            col("location").isin("Delhi", "Mumbai"),
            15
        ).otherwise(0)
    )

    # ============================================================
    # RULE 3: SUSPICIOUS MERCHANT
    # ============================================================

    df = df.withColumn(
        "fraud_score",
        col("fraud_score")
        + when(
            col("merchant").isin("Netflix", "Uber"),
            5
        ).otherwise(0)
    )

    # ============================================================
    # MAXIMUM SCORE = 100
    # ============================================================

    df = df.withColumn(
        "fraud_score",
        when(col("fraud_score") > 100, 100)
        .otherwise(col("fraud_score"))
    )

    # ============================================================
    # RISK LEVEL
    # ============================================================

    df = df.withColumn(
        "risk_level",
        when(col("fraud_score") >= 70, "HIGH")
        .when(col("fraud_score") >= 40, "MEDIUM")
        .otherwise("LOW")
    )

    return df