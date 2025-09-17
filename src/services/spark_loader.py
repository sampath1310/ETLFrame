from pyspark.sql import SparkSession
from pyspark.sql.functions import DataFrame

from src.core.IETL import ITransformer, ILoader
from src.services.ConfigReader import ConfigReader
from src.services.writers import IcebergWriter


class SparkLoader(ILoader):
    def __init__(
        self,
        transformer: ITransformer,
        config_reader: ConfigReader,
        spark: SparkSession,
    ):
        self.transformer = transformer
        self.config_reader = config_reader
        self.spark = spark

    def load(
        self,
    ):
        transformer_dataframe: DataFrame = self.transformer.transform()
        iceberg_writer = IcebergWriter(config_reader=self.config_reader)
        iceberg_writer.write_to_iceberg(self.spark, transformer_dataframe)
