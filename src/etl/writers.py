from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from src.etl.readers import read_iceberg


def write_to_path(spark_df: DataFrame, path: str, file_formats: str, mode):
    spark_df.write.format(file_formats).mode(mode).save(path)


def write_to_iceberg(spark, spark_df: DataFrame, catalog, database, table_name):
    try:
        read_iceberg(spark, catalog, database, table_name)
    except AnalysisException as e:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS rest")
        x = spark.sql("SHOW CATALOGS;")
        print(x.show())
        x = spark.sql("SHOW SCHEMAS;")
        print(x.show())

        # spark.sql(f"""
        # CREATE DATABASE IF NOT EXISTS {catalog}.{database}
        # LOCATION 's3a://warehouse/'
        # """)
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print("Creating table since it does not exist...")
            spark_df.writeTo(f"{catalog}.{database}.{table_name}").using(
                "iceberg"
            ).create()
        else:
            raise
    spark_df.writeTo(f"{catalog}.{database}.{table_name}").overwritePartitions()
