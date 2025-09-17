# from common.spark_session import get_spark_session
from pathlib import Path

from src.common.constants import CONFIG_PATH, JOB_CONFIG_FILE_PATH
from src.common.spark_session import get_spark_session
from src.services.ConfigReader import ConfigReader
from src.services.SparkExtractor import SparkFileExtractor
from src.services.SparkTransformer import SparkTransformer
from src.services.spark_loader import SparkLoader

if __name__ == "__main__":
    file_path = (
        Path(__file__).resolve().parent  # .parent.parent
        / CONFIG_PATH
        / JOB_CONFIG_FILE_PATH
    )
    config_reader = ConfigReader(file_path)
    spark_config = config_reader.get_key("sparkConf")
    spark = get_spark_session("TestJob", spark_config)
    print("File format", config_reader.get_format_config())
    spark_extractor = SparkFileExtractor(spark, config_reader)
    spark_transformer = SparkTransformer(spark_extractor)
    spark_loader = SparkLoader(spark_transformer, config_reader, spark)
    spark_loader.load()
    spark.stop()
