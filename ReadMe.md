
# Project Description
# Project Flow
# Dataset
https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload?selectedyear=2024
This project performs ETL operation on above financial table 
using delta lake features and iceberg features.
As the project flows we will




#### Iceberg intro 


-  .:/home/iceberg/project_code


    extra_hosts:
      - "host.docker.internal:host-gateway"

/opt/spark/bin/spark-submit --master  spark://4e4e024504d4:7077  --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,org.apache.iceberg:iceberg-aws-bundle:1.7.1    --executor-memory 2G --num-executors 2 /home/iceberg/project_code/etl_job/elt_job.py --etl_job_config etl_config_iceberg


    
References:
https://www.dremio.com/resources/guides/apache-iceberg/
https://mayursurani.medium.com/comprehensive-guide-to-production-grade-pyspark-solutions-6243233c2b3b
https://medium.com/@diehardankush/the-different-types-of-loads-in-etl-40e5f04be8ac

For iceberg configurations in job
https://iceberg.apache.org/spark-quickstart/#writing-data-to-a-table
