"""
session.py — Spark session factory and configuration loader.

Centralizes Spark session creation so every job gets the same base config
(Delta Lake extensions, adaptive query execution, serializer settings).
Think of this as the "power plant" that every machine in the factory plugs into.
"""

import yaml
from pathlib import Path
from pyspark.sql import SparkSession


def load_config(config_path: str = None, env: str = "production") -> dict:
    """Load pipeline config with environment-specific overrides.

    Config resolution order (later values win):
      1. Base config (pipeline_config.yaml)
      2. Environment overlay (config/{env}.yaml)
      3. Explicit overrides passed at runtime

    Args:
        config_path: Path to base config. Defaults to config/pipeline_config.yaml.
        env: Environment name for overlay resolution.

    Returns:
        Merged configuration dictionary.
    """
    if config_path is None:
        config_path = str(Path(__file__).parent.parent / "config" / "pipeline_config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Load environment overlay if it exists
    env_path = Path(config_path).parent / f"{env}.yaml"
    if env_path.exists():
        with open(env_path, "r") as f:
            env_config = yaml.safe_load(f)
            # BUG: shallow merge — nested keys in env_config overwrite entire
            # sections instead of merging them. E.g., setting one spark.config
            # value in dev.yaml wipes ALL production spark.config values.
            config.update(env_config)

    return config


def create_spark_session(config: dict) -> SparkSession:
    """Create a configured SparkSession with Delta Lake support.

    Args:
        config: Pipeline configuration dictionary.

    Returns:
        Configured SparkSession instance.
    """
    spark_config = config.get("spark", {})

    builder = SparkSession.builder.appName(
        spark_config.get("app_name", "acme-lakehouse")
    )

    master = spark_config.get("master")
    if master:
        builder = builder.master(master)

    # Apply all Spark configurations
    for key, value in spark_config.get("config", {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Set log level — hardcoded, not configurable
    spark.sparkContext.setLogLevel("WARN")

    return spark
