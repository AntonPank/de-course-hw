"""Bronze: landing-зона -> bronze.raw_events у Postgres.

Spark у local mode читає NDJSON із landing і дописує рядки у сховище через JDBC.

Контракт шару Bronze:
* читаємо з явною схемою;
* `payload` лишається сирим JSON-рядком;
* файл, який уже завантажено, вдруге не потрапляє в Bronze;
* append атомарний: або записуються всі нові рядки, або жодного.
"""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common import config
from common.spark import build_spark

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("ride_id", StringType(), True),
        StructField("occurred_at", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)


def read_landing(spark: SparkSession, landing_dir: str) -> DataFrame:
    """Читає завершені landing-файли та додає ingestion metadata."""
    root_uri = Path(landing_dir).resolve().as_uri().rstrip("/") + "/"

    df = (
        spark.read.schema(EVENT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.ndjson")
        .json(landing_dir)
    )

    df = df.withColumn(
        "_source_file",
        F.regexp_replace(
            F.input_file_name(),
            f"^{re.escape(root_uri)}",
            "",
        ),
    )

    df = df.withColumn(
        "_ingested_at",
        F.current_timestamp(),
    )

    return df.select(
        "event_id",
        "event_type",
        "ride_id",
        "occurred_at",
        "source",
        "payload",
        "_source_file",
        "_ingested_at",
    )


def loaded_files(spark: SparkSession) -> set[str]:
    """Повертає файли, які вже завантажені в Bronze."""
    query = f"(SELECT DISTINCT _source_file FROM {config.BRONZE_TABLE}) AS t"
    rows = spark.read.jdbc(
        config.JDBC_URL,
        query,
        properties=config.JDBC_PROPERTIES,
    ).collect()
    return {r["_source_file"] for r in rows}


def select_new(df: DataFrame, already_loaded: set[str]) -> DataFrame:
    """Лишає рядки з landing-файлів, яких ще немає в Bronze."""
    if not already_loaded:
        return df

    return df.filter(~F.col("_source_file").isin(sorted(already_loaded)))


def write_bronze(df: DataFrame) -> None:
    """Дописує нові Bronze-рядки однією JDBC-транзакцією."""
    (
        df.coalesce(1)
        .write.mode("append")
        .jdbc(
            config.JDBC_URL,
            config.BRONZE_TABLE,
            properties=config.JDBC_PROPERTIES,
        )
    )


def main() -> None:
    spark = build_spark("bronze-job")

    try:
        raw = read_landing(spark, str(config.LANDING_DIR))
        new = select_new(raw, loaded_files(spark)).cache()

        n_new = new.count()

        if n_new == 0:
            print("Bronze: нових файлів немає — пропускаю запис (ідемпотентно).")
        else:
            n_files = new.select("_source_file").distinct().count()
            write_bronze(new)
            print(f"Bronze: додано {n_new} подій із {n_files} файлів.")

        total = spark.read.jdbc(
            config.JDBC_URL,
            f"(SELECT count(*) AS n FROM {config.BRONZE_TABLE}) AS t",
            properties=config.JDBC_PROPERTIES,
        ).collect()[0]["n"]

        print(f"Bronze total: {total}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
