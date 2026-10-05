from __future__ import annotations

import logging
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType
from pyspark.sql.window import Window

LANDING_GLOB = "data/landing/*.json.gz"
OUTPUT_DIR = "data/output"
TARGET_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
]
SUMMARY_DIMENSIONS = ["event_type", "repo_owner", "actor_login", "hour"]
TOP_N = 5
BOT_SUFFIX = "[bot]"
log = logging.getLogger(__name__)


def event_schema() -> StructType:
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField(
                "actor",
                StructType([StructField("login", StringType(), True)]),
                True,
            ),
            StructField(
                "repo",
                StructType([StructField("name", StringType(), True)]),
                True,
            ),
            StructField("public", BooleanType(), True),
            StructField("created_at", StringType(), True),
        ]
    )


def read_raw(spark: SparkSession) -> DataFrame:
    return spark.read.schema(event_schema()).json(LANDING_GLOB)


def flatten(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.name").alias("repo_name"),
        F.col("public"),
        F.to_timestamp("created_at").alias("created_at"),
    )


def clean(events: DataFrame) -> DataFrame:
    return (
        events.filter(F.col("event_type").isin(TARGET_EVENT_TYPES))
        .filter(F.col("public") == True)
        .dropna(subset=["event_id", "repo_name", "created_at"])
        .dropDuplicates(["event_id"])
    )


def with_derived(events: DataFrame) -> DataFrame:
    return (
        events.withColumn(
            "repo_owner",
            F.split(F.col("repo_name"), "/").getItem(0),
        )
        .withColumn(
            "is_bot",
            F.coalesce(
                F.col("actor_login").endswith(BOT_SUFFIX),
                F.lit(False),
            ),
        )
        .withColumn("hour", F.date_trunc("hour", F.col("created_at")))
    )


def owner_totals(events: DataFrame) -> DataFrame:
    return events.groupBy("repo_owner").agg(
        F.count("*").alias("owner_events"),
        F.countDistinct("repo_name").alias("owner_repos"),
        F.sum(F.when(F.col("is_bot"), 1).otherwise(0)).alias("owner_bot_events"),
    )


def top_repos_per_type(events: DataFrame, n: int) -> DataFrame:
    counts = events.groupBy("event_type", "repo_name").agg(
        F.count("*").alias("repo_event_count")
    )
    window = Window.partitionBy("event_type").orderBy(
        F.col("repo_event_count").desc(),
        F.col("repo_name").asc(),
    )
    return counts.withColumn(
        "rank",
        F.row_number().over(window),
    ).filter(F.col("rank") <= n)


def enrich_top_repos(top_repos: DataFrame, owners: DataFrame) -> DataFrame:
    top = top_repos.withColumn(
        "repo_owner",
        F.split(F.col("repo_name"), "/").getItem(0),
    )
    joined = top.join(
        F.broadcast(owners),
        on="repo_owner",
        how="left",
    )
    return (
        joined.withColumn(
            "owner_events",
            F.coalesce(F.col("owner_events"), F.lit(0)),
        )
        .withColumn(
            "owner_repos",
            F.coalesce(F.col("owner_repos"), F.lit(0)),
        )
        .withColumn(
            "owner_share",
            F.when(
                F.col("owner_events") > 0,
                F.round(
                    F.col("repo_event_count") / F.col("owner_events"),
                    4,
                ),
            ),
        )
        .select(
            "event_type",
            "repo_name",
            "repo_owner",
            "repo_event_count",
            "rank",
            "owner_events",
            "owner_repos",
            "owner_share",
        )
    )


def summary_slice(events: DataFrame, dimension: str) -> DataFrame:
    return (
        events.groupBy(F.col(dimension))
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("repo_name").alias("distinct_repos"),
        )
        .select(
            F.lit(dimension).alias("dimension"),
            F.col(dimension).cast("string").alias("dimension_value"),
            "events",
            "distinct_repos",
        )
    )


def build_summary(events: DataFrame, dimensions: list[str]) -> DataFrame:
    slices = [summary_slice(events, dimension) for dimension in dimensions]
    result = slices[0]
    for df in slices[1:]:
        result = result.unionByName(df)
    return result


def write_outputs(outputs: dict[str, tuple[DataFrame, str | None]]) -> None:
    for name, (df, partition_col) in outputs.items():
        path = f"{OUTPUT_DIR}/{name}"
        if partition_col is None:
            df.coalesce(1).write.mode("overwrite").parquet(path)
        else:
            (
                df.repartition(partition_col)
                .write.mode("overwrite")
                .partitionBy(partition_col)
                .parquet(path)
            )


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = build_spark("l12-github")
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    events = with_derived(clean(flatten(read_raw(spark)))).cache()
    owners = owner_totals(events)
    top_repos = enrich_top_repos(top_repos_per_type(events, TOP_N), owners)
    summary = build_summary(events, SUMMARY_DIMENSIONS)
    marts: dict[str, tuple[DataFrame, str | None]] = {
        "events": (events, "event_type"),
        "owner_totals": (owners, None),
        "top_repos": (top_repos, None),
        "summary": (summary, None),
    }
    write_outputs(marts)
    for name, (df, _) in marts.items():
        log.info("%-13s %d", f"{name}:", df.count())
    events.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
