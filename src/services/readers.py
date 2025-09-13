def read_from_path(spark, path, file_format, options):
    """
    Read data from a given path."""
    if file_format == "csv":
        return spark.read.csv(path, **options)
    if file_format == "json":
        return spark.read.json(path, **options)
    if file_format == "parquet":
        return spark.read.parquet(path, **options)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def read_iceberg(spark, catalog, database, table_name):
    df = spark.table(f"{catalog}.{database}.{table_name}")
    df.show()
    return df
