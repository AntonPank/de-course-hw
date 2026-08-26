from __future__ import annotations

import polars as pl

from . import config


def build_bronze() -> pl.DataFrame:
    lf = pl.scan_ndjson(
        config.LANDING_FILE,
        schema=config.LANDING_SCHEMA,
    )

    bronze = lf.select(
        [
            pl.col("id").alias("event_id"),
            pl.col("type").alias("event_type"),
            pl.col("actor").struct.field("id").alias("actor_id"),
            pl.col("actor").struct.field("login").alias("actor_login"),
            pl.col("repo").struct.field("id").alias("repo_id"),
            pl.col("repo").struct.field("name").alias("repo_name"),
            pl.col("created_at")
            .str.to_datetime(
                "%Y-%m-%dT%H:%M:%SZ",
                time_zone="UTC",
            )
            .alias("created_at"),
            pl.col("public"),
            pl.col("payload").struct.field("action").alias("action"),
            (
                pl.col("payload")
                .struct.field("commits")
                .list.len()
                .fill_null(0)
                .cast(pl.Int64)
                .alias("commit_count")
            ),
        ]
    )

    df = bronze.collect()

    df.write_parquet(config.BRONZE_FILE, mkdir=True)

    return df
