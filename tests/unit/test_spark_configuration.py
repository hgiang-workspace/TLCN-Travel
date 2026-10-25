from __future__ import annotations

from travel_intelligence.common.spark import configure_spark_builder


class Builder:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def master(self, value: str) -> "Builder":
        self.values["spark.master"] = value
        return self

    def config(self, key: str, value: str) -> "Builder":
        self.values[key] = value
        return self


def test_local_master_is_the_default(monkeypatch) -> None:
    monkeypatch.delenv("SPARK_MASTER_URL", raising=False)
    builder = configure_spark_builder(Builder())
    assert builder.values == {"spark.master": "local[2]"}


def test_standalone_configures_reachable_driver(monkeypatch) -> None:
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    monkeypatch.setenv("SPARK_DRIVER_HOST", "spark")
    builder = configure_spark_builder(Builder())
    assert builder.values["spark.master"] == "spark://spark-master:7077"
    assert builder.values["spark.driver.bindAddress"] == "0.0.0.0"
    assert builder.values["spark.driver.host"] == "spark"
    assert builder.values["spark.driver.port"] == "37001"
    assert builder.values["spark.blockManager.port"] == "37002"
    assert builder.values["spark.executor.instances"] == "1"
