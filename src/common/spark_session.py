from pyspark.sql import SparkSession


def get_spark_session(app_name: str,conf:dict) -> SparkSession:
    """
    Get or create a Spark session for the application.

    Args:
        app_name: The name of the Spark application.

    Returns:
        A SparkSession object.
    """
    spark_session_builder = SparkSession.builder.appName(app_name)
    print(conf)
    for key,value in conf.items():
        spark_session_builder = spark_session_builder.config(key,value)
    spark = spark_session_builder.getOrCreate()
    return spark
