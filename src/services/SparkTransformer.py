from pyspark.sql.functions import DataFrame, current_timestamp

from src.core.IETL import ITransformer, IExtractor


class SparkTransformer(ITransformer):
    def __init__(self, extractor: IExtractor):
        self.extractor = extractor

    def transform(self):
        extractor_dataframe: DataFrame = self.extractor.read()
        extractor_dataframe = extractor_dataframe.withColumn(
            "recorded_at", current_timestamp()
        )
        return extractor_dataframe
