"""Silver stage — clean, filter and de-duplicate the bronze events.

TODO (Завдання 2 і 3): реалізуйте build_silver() і write_silver_partitioned().
Контракт: див. CONTRACTS.md → "silver" і "silver partitioned".

build_silver():
  * залиште тільки типи з config.TARGET_EVENT_TYPES
  * приберіть рядки з порожнім/відсутнім repo_name, відсутнім event_id чи created_at
  * гарантуйте унікальність по event_id (.unique(subset=["event_id"]))
  * запишіть у config.SILVER_FILE і поверніть DataFrame

write_silver_partitioned():
  * запишіть silver як Hive-партиціонований датасет за event_type
  * директорія: config.SILVER_PARTITIONED_DIR
  * підказка: df.write_parquet(dir, partition_by="event_type")
"""

from __future__ import annotations

import polars as pl

from . import config

from pathlib import Path


def build_silver(bronze: pl.DataFrame) -> pl.DataFrame:
    silver = bronze # опрацьовуємо Silver, використовуючи Bronze як джерело вхідних даних

    # опрацьовуємо тільки ті типи, що вказані в контракті
    silver = silver.filter(
        pl.col("event_type").is_in(config.TARGET_EVENT_TYPES)
    )


    # видаляємо невалідні рядки
    silver = silver.filter(
        pl.col("repo_name").is_not_null()
        & (pl.col("repo_name") != "")
        & pl.col("event_id").is_not_null()
        & pl.col("created_at").is_not_null()
    )

    # видаляємо дублікати
    silver = silver.unique(
        subset=["event_id"]
    )


    print(
        silver
        .group_by("event_type")
        .len()
        .sort("event_type")
    )


    # створення шляху та зберігання результату
    Path(config.SILVER_FILE).parent.mkdir(parents=True, exist_ok=True)

    silver.write_parquet(config.SILVER_FILE)

    return silver



def write_silver_partitioned(silver: pl.DataFrame) -> None:

  Path(config.SILVER_PARTITIONED_DIR).mkdir(
    parents=True,
    exist_ok=True,
  )
  silver.write_parquet(
      config.SILVER_PARTITIONED_DIR,
      partition_by="event_type",
  )

    

