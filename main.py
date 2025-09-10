from src.common import spark_session

if __name__ == '__main__':
    spark = spark_session.get_spark_session("SalesETLJob")