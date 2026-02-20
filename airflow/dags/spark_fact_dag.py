from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
   "owner": "data-engineer",
}

with DAG(
   dag_id            = "spark_fact_dag",
   start_date        = datetime(2025, 1, 1),
   schedule_interval = "@daily",
   catchup           = False,
   default_args      = default_args,
) as dag:

   spark_fact_job = SparkSubmitOperator(
       task_id          = "spark_fact_job",
       application      = "dags/spark_jobs/fact_transactions.py",
       conn_id          = "spark_default",
       application_args = ["--process_date", "{{ ds }}"],
       conf             = {
           "spark.executor.memory"   : "2g",
           "spark.executor.cores"    : "2",
           "spark.executor.instances": "2"
       },
       execution_timeout=None,
   )

   spark_fact_job


