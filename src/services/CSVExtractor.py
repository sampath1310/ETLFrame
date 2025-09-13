import pprint
from typing import Dict, Any

from pyspark.sql import SparkSession

from src.core.IETL import IExtractor
from src.services.ConfigReader import ConfigReader


class SparkFileExtractor(IExtractor):
    def __init__(
        self, spark_session: SparkSession, file_format: str, options: Dict[str, Any]
    ):
        self.spark_session = spark_session
        self.file_format = file_format
        self.config_reader = ConfigReader()
        self.options = self.config_reader.get_options_config()
        self.file_path = self.config_reader.get_data_file_path()
        pprint.pprint(self.options)

    def read(self):
        self.options["format"] = self.file_format
        reader_dataframe = self.spark_session.read.load(self.file_path, **self.options)

        return reader_dataframe
