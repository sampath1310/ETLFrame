import argparse
import os
import sys

import yaml

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)


from src.common.spark_session import get_spark_session
from src.services import readers, writers


def run_job(config_path: str):
    """
    Runs the sales ETL job.
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        spark_config = config["sparkConf"]
        config = config["sales_job"]
    spark = get_spark_session("SalesETLJob", spark_config)

    # 1. Read source data
    input_df = readers.read_from_path(
        spark,
        path=config["input"]["path"],
        file_format=config["input"]["format"],
        options=config["input"]["options"],
    )
    if config.get("output").get("format") == "iceberg":
        writers.write_to_iceberg(
            spark,
            input_df,
            config.get("output").get("catalog"),
            config.get("output").get("database"),
            config.get("output").get("table_name"),
        )
    spark.stop()
    print("Sales ETL job completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A Spark Python ETL Job")
    parser.add_argument("--etl_job_config", type=str, help="Path to elt job config")
    args = parser.parse_args()
    run_job(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            f"conf/base/{args.etl_job_config}.yaml",
        )
    )
