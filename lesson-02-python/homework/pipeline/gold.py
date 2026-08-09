from __future__ import annotations

import polars as pl

from . import config

from pathlib import Path


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:

    repo_activity = (
        silver
        .group_by("repo_name")
        .agg(
            [
                pl.len()
                .cast(pl.Int64)
                .alias("event_count"),

                pl.col("event_type")
                .n_unique()
                .cast(pl.Int64)
                .alias("distinct_event_types"),

            ]
        )
        .sort(
            "event_count",
            descending=True,
        )

    )

    print(repo_activity.height)

    print(
        repo_activity["event_count"].sum()
    )

    

    Path(config.GOLD_REPO_ACTIVITY).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    repo_activity.write_parquet(
        config.GOLD_REPO_ACTIVITY
    )

    return repo_activity



def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    activity = (
        silver
        .group_by(
            pl.col("created_at")
                .dt.truncate("1m")
                .alias("minute")

        )

        .agg(
            pl.len()
                .cast(pl.Int64)
                .alias("event_count")
        )

        .sort("minute")
    )

    print(activity.height)

    print(
        activity["event_count"].sum()
    )


    Path(config.GOLD_ACTIVITY_PER_MINUTE).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    activity.write_parquet(
        config.GOLD_ACTIVITY_PER_MINUTE
    )

    return activity




def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    push = (
        silver
        .filter(
            pl.col("event_type") == "PushEvent"
        )

        .group_by("repo_name")

        .agg(
            [
              pl.len()
              .cast(pl.Int64)
              .alias("push_events"),

              pl.col("commit_count")
              .sum()
              .cast(pl.Int64)
              .alias("total_commits"),
            ]
        )
        .sort(
            "total_commits",
            descending=True,
        )

    )

    print(push.height)
    print(push["total_commits"].sum())

    Path(config.GOLD_PUSH_COMMITS).parent.mkdir(
        parents=True,
        exist_ok=True,
    )


    push.write_parquet(config.GOLD_PUSH_COMMITS)

    return push