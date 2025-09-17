from pyspark.sql import DataFrame

from src.services.ConfigReader import ConfigReader
from src.services.readers import read_iceberg


# def write_to_path(spark_df: DataFrame, path: str, file_formats: str, mode):
#     spark_df.write.format(file_formats).mode(mode).save(path)


class IcebergWriter:
    def __init__(self, config_reader: ConfigReader):
        self.config_reader = config_reader

    def write_to_iceberg(self, spark, spark_df: DataFrame):
        catalog = self.config_reader.get_output_catalog()
        database = self.config_reader.get_output_database()
        table_name = self.config_reader.get_output_table_name()
        try:
            read_iceberg(spark, catalog, database, table_name)
        except Exception as e:
            # spark.sql("CREATE NAMESPACE IF NOT EXISTS rest")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                print("Creating table since it does not exist...")
                spark_df.writeTo(f"{catalog}.{database}.{table_name}").create()
            else:
                raise
        spark_df.writeTo(f"{catalog}.{database}.{table_name}").overwritePartitions()
