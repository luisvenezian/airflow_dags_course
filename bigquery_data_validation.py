from airflow import DAG 
from airflow.utils.dates import days_ago

from airflow.operators.bigquery_plugin import (
    BigQueryDataValidationOperator,
    BigQueryDatasetSensor
)

default_arguments = {"owner": "Luis", "start_date": days_ago(1)}

with DAG(
    "bigquery_data_validation",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={"project":"lfav-apache-airflow-pipeline"}) as dag:

    is_table_empty = BigQueryDataValidationOperator(
        task_id="is_table_empty",
        sql="SELECT COUNT(*) FROM `{{project}}.lfavapacheairflowpipeline.history`",
        location="southamerica-east1"
    )

    dataset_exists = BigQueryDatasetSensor(
        task_id="dataset_exists",
        project_id="{{project}}",
        dataset_id="lfavapacheairflowpipeline"
    )


dataset_exists >> is_table_empty