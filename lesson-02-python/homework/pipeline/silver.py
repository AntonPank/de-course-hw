from __future__ import annotations

import polars as pl

from . import config


def build_silver(bronze: pl.DataFrame) -> pl.DataFrame:
    silver = (
        bronze.filter(pl.col("event_type").is_in(config.TARGET_EVENT_TYPES))
        .filter(
            pl.col("repo_name").is_not_null()
            & (pl.col("repo_name") != "")
            & pl.col("event_id").is_not_null()
            & pl.col("created_at").is_not_null()
        )
        .unique(subset=["event_id"])
    )

    silver.write_parquet(config.SILVER_FILE, mkdir=True)

    return silver


def write_silver_partitioned(silver: pl.DataFrame) -> None:

    silver.write_parquet(
        config.SILVER_PARTITIONED_DIR,
        partition_by="event_type",
        mkdir=True,
    )
