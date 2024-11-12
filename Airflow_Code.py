from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

from datetime import datetime, timedelta

default_args = {
    "owner":"Nasir",
    "depends_on_past":False,
    #datetime(year,month,date)
    "start_date": datetime(2024, 11, 10)
}


dag = DAG(
    dag_id = "Cricket_Data_Pipeline",
    default_args=default_args,
    description="ETL Process For Cricket Data",
    schedule_interval = timedelta(seconds=55),
    catchup=False
)


trigger_extract_lambda = LambdaInvokeFunctionOperator(
    task_id = "Extract_Lambda_Function",
    function_name="Cricket_Data_Extraction",
    aws_conn_id = "aws_connection",
    region="us-east-1",
    dag=dag,
)



trigger_transformation_databricks = DatabricksSubmitRunOperator(
    task_id='Transformation_Layer_Databricks',
    databricks_conn_id='databricks_default',
    json={
         "tasks": [{
            "task_key": "databricks_task",  # Add a unique task key here
            "notebook_task": {
                "notebook_path": "/Workspace/Users/nasirkadri2601@gmail.com/transformation_cric_data_final"
            },
            "existing_cluster_id": "1105-151146-ln8m1ajv"  # Ensure this is a valid, active cluster ID
        }]
    },
    dag=dag
)


trigger_extract_lambda >> trigger_transformation_databricks






