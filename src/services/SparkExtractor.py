import pprint

from pyspark.sql import SparkSession

from src.core.IETL import IExtractor
from src.services.ConfigReader import ConfigReader


class SparkFileExtractor(IExtractor):
    def __init__(
        self,
        spark_session: SparkSession,
        config_reader: ConfigReader,
    ):
        self.spark_session = spark_session
        self.file_format = config_reader.get_format_config()
        self.config_reader = config_reader
        self.options = self.config_reader.get_options_config()
        self.file_path = self.config_reader.get_data_file_path()
        pprint.pprint(self.options)

    def read(self):
        self.options["format"] = self.file_format
        reader_dataframe = self.spark_session.read.load(self.file_path, **self.options)
        return reader_dataframe
