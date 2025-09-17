# Project Description

# Project Flow

# Dataset

https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload?selectedyear=2024
This project performs ETL operation on above financial table
using delta lake features and iceberg features.
As the project flows we will

#### Iceberg intro

- .:/home/iceberg/project_code

  extra_hosts:
    - "host.docker.internal:host-gateway"

/opt/spark/bin/spark-submit --master spark://4e4e024504d4:7077 --packages org.apache.hadoop:hadoop-aws:
3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:
1.9.2,org.apache.iceberg:iceberg-aws-bundle:1.7.1 --executor-memory 2G
--num-executors2/home/iceberg/project_code/src/main.py --etl_job_config etl_config_iceberg

References:
https://www.dremio.com/resources/guides/apache-iceberg/
https://mayursurani.medium.com/comprehensive-guide-to-production-grade-pyspark-solutions-6243233c2b3b
https://medium.com/@diehardankush/the-different-types-of-loads-in-etl-40e5f04be8ac

For iceberg configurations in job
https://iceberg.apache.org/spark-quickstart/#writing-data-to-a-table
https://datastrato.ai/blog/implement-rest-iceberg/
https://karlchris.github.io/data-engineering/projects/spark-iceberg/#load-parquet-files-into-iceberg
https://medium.com/@geekfrosty/apache-iceberg-architecture-demystified-e19b5cae9975
https://www.tabular.io/blog/rest-catalog-docker/
https://kevinjqliu.substack.com/p/iceberg-rest-catalog-with-hive-metastore

Project prompt
Hey Jake, you are role is to provide detailed break down of project and help organize project into tasks , sub-tasks
modules. This should Cover all from

1. Development to deployment. All tasks such as development of feature , testing , integrated testing , unit testing.
2. Suggestion/Recommendation of industry coding standards and solid principles and code quality all mentioned as task.
3. Timeline of the project with break down of task and features

User will provide you with project descriptions and features. you have to prepare excel for the suggested tasks and
features with timelines. in case of any change in project requirment
make sure to re-evaluate all the and update tasks based on new changes.
Strictly Keep Project managment as consistent as possible

User:Hey jake, I am creating a spark ETL project for in pyspark using iceberg as metastore and storage as minio for
local development , making it scalable to deploy on any cloud service provider.
Features of this project are

1. Create a Config based ETL project. parameters like s3 path, file name file format , delimiters of file . file format
   includes , csv file, json file, parquet file for reading from S3 bucket
2. DataSource include s3,database, api based
2. Any one who uses this ETL program should not have to write much of code unless there is custom transformation.
   everything is managed with configuration files
3. Data Quality report should be generated after successful run of spark job. I intent to use dequeu for this project.

can you create tasks and help manage project, provide me any suggestion. you can ask me question before providing me the
result

Cloud Deployment Target – You mentioned it should be scalable to any cloud. Do you want me to plan deployment steps for
AWS, Azure, GCP separately,
or just design a cloud-agnostic deployment strategy with minimal adjustments per provider?
Not now later we can plan this
Tech Stack Confirmation – Besides PySpark + Iceberg + MinIO (local), do you want me to include Airflow / Prefect /
Dagster for orchestration? Or should I keep orchestration out of scope?
include airflow
Data Quality Tooling – You mentioned Deequ.
Should I plan tasks for:
Only basic quality checks (null, duplicates, schema drift)
Or also business rule-based checks (custom validations)
Yes only basic quality checks

CI/CD & Testing Strategy – Do you want me to include:

Unit testing (Pytest, Spark testing base)

Integration testing (end-to-end ETL run with test data)

Static checks (linting, mypy, code style, security scans)?
Yes include all
Team & Timeline Constraints – Should I assume:

1–2 developers with ~8 weeks timeline, or

Medium size timeline only 1 developer
