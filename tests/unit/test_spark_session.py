import pytest
from pyspark.sql import SparkSession

from src.common.spark_session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    """
    Provides a SparkSession for testing and tears it down after all tests.
    """
    spark = (
        SparkSession.builder.master("local[*]").appName("pytest-spark").getOrCreate()
    )
    yield spark
    spark.stop()


def test_get_spark_session_with_app_name_and_config(tmp_path):
    configs = {"spark.sql.shuffle.partitions": "2", "spark.executor.memory": "1g"}

    spark = get_spark_session("TestApp", configs)

    # Assert app name is set
    assert spark.sparkContext.appName == "TestApp"

    # Assert configs are applied
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
    assert spark.conf.get("spark.executor.memory") == "1g"

    spark.stop()


def test_get_spark_session_without_config():
    spark = get_spark_session("NoConfigApp", {})

    assert spark.sparkContext.appName == "NoConfigApp"

    # Default shuffle partitions (Spark usually defaults to 200, but let's just check it's numeric)
    partitions = spark.conf.get("spark.sql.shuffle.partitions")
    assert partitions.isdigit()

    spark.stop()


def test_get_spark_session_reuses_existing_session():
    configs = {"spark.sql.autoBroadcastJoinThreshold": "-1"}

    spark1 = get_spark_session("ReuseApp", configs)
    spark2 = get_spark_session("ReuseApp", {})

    # Spark should reuse the same session
    assert spark1 is spark2
    assert spark2.conf.get("spark.sql.autoBroadcastJoinThreshold") == "-1"

    spark1.stop()
