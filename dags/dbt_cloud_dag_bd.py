from airflow.decorators import dag
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudListJobsOperator,
     DbtCloudRunJobOperator,
)
import pendulum

@dag(
    # normal dag parameters
    dag_id="dbt_cloud_dag_bd",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1),
    catchup=False,
     default_args={
         "retries":0,
        "dbt_cloud_conn_id":"dbt_conn",
    },   
 )
def dbt_cloud_dag_bd():
    dbt_cloud_list_jobs = DbtCloudListJobsOperator(
        task_id="dbt_cloud_list_jobs",
        project_id="9141",
     )
    dbt_cloud_run_job = DbtCloudRunJobOperator(
      task_id="dbt_cloud_run_job", job_id=24480
    )

    dbt_cloud_list_jobs >> dbt_cloud_run_job
    
dbt_cloud_dag_bd = dbt_cloud_dag_bd()
