import json
import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

LANDING = "data/landing"
OUTPUT = "data/output/windowed"
CHECKPOINT = "data/checkpoints/windowed"
SUMMARY = "data/output/summary.json"

WINDOW = "30 seconds"
WATERMARK = "10 seconds"
KEEP_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "IssueCommentEvent",
    "WatchEvent",
]


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("l17-streaming-homework")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def event_schema() -> StructType:
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("public", BooleanType(), True),
            StructField(
                "actor",
                StructType(
                    [
                        StructField("login", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "repo",
                StructType(
                    [
                        StructField("name", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )


def read_stream(spark: SparkSession) -> DataFrame:
    return spark.readStream.schema(event_schema()).json(LANDING)


def clean_events(stream_df: DataFrame) -> DataFrame:
    df = stream_df.filter(F.col("type").isin(KEEP_TYPES))
    df = df.filter(F.col("public") == F.lit(True))

    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("created_at")),
    )

    return df.select(
        F.col("id"),
        F.col("type").alias("event_type"),
        F.col("event_time"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.name").alias("repo_name"),
    )


def windowed_counts(clean_df: DataFrame) -> DataFrame:
    df = clean_df.withWatermark("event_time", WATERMARK)

    return df.groupBy(
        F.window("event_time", WINDOW),
        "event_type",
    ).count()


def write_windows(spark: SparkSession) -> None:
    shutil.rmtree(OUTPUT, ignore_errors=True)
    shutil.rmtree(CHECKPOINT, ignore_errors=True)

    clean = clean_events(read_stream(spark))

    def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
        agg = windowed_counts(batch_df)

        output_df = agg.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("event_type"),
            F.col("count").alias("event_count"),
        )

        output_df.write.mode("append").parquet(OUTPUT)

    query = (
        clean.writeStream.foreachBatch(upsert_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()


def build_summary(spark: SparkSession) -> dict:
    df = spark.read.parquet(OUTPUT)

    total_events = df.agg(F.sum("event_count").alias("total")).first()["total"]

    n_windows = df.select("window_start").distinct().count()

    by_type_rows = (
        df.groupBy("event_type").agg(F.sum("event_count").alias("count")).collect()
    )

    by_type = {row["event_type"]: int(row["count"]) for row in by_type_rows}

    summary = {
        "total_events": int(total_events),
        "n_windows": int(n_windows),
        "window_seconds": 30,
        "by_type": by_type,
    }

    os.makedirs(os.path.dirname(SUMMARY), exist_ok=True)

    with open(SUMMARY, "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, sort_keys=True)

    return summary


def main() -> None:
    spark = build_spark()

    try:
        write_windows(spark)
        summary = build_summary(spark)
        print("SUMMARY:", json.dumps(summary, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
