# from common.spark_session import get_spark_session
# from services.CSVExtractor import SparkExtractor
# from services.ConfigReader import ConfigReader
#
# if __name__ == "__main__":
#     config_reader = ConfigReader("/home/iceberg/project_code/conf/job/job_config.yaml")
#     spark_config = config_reader.get_key("SparkConf")
#     spark = get_spark_session("SalesETLJob", spark_config)
#     spark_extractor = SparkExtractor(spark)
#     spark_extractor.read().show()
