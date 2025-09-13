from pyspark.sql import SparkSession


def get_spark_session(app_name: str, configs: dict) -> SparkSession:
    """
     Get or create a Spark session for the application, applying any provided configurations.

    Args:
        app_name: The name of the Spark application.
        configs: A dictionary of Spark configurations.

    Returns:
        A SparkSession object.
    """
    builder = SparkSession.builder.appName(app_name)
    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)
    spark = builder.getOrCreate()

    return spark
