from __future__ import annotations

import requests
from airflow.sensors.base import BaseSensorOperator


class GHArchiveSensor(BaseSensorOperator):
    def __init__(self, hour: int = 14, **kwargs) -> None:
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        ds = context["ds"]

        url = f"https://data.gharchive.org/{ds}-{self.hour:02d}.json.gz"

        try:
            response = requests.head(url)
            return response.status_code == 200
        except requests.RequestException:
            return False
