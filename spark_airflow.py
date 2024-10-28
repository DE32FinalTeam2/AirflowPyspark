from datetime import datetime, timedelta
import sys
import os
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        BranchPythonOperator,
        PythonVirtualenvOperator,
)

with DAG(
    'Spark_db',
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=3),
        "execution_timeout": timedelta(hours=2),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='Sparkdb',
    schedule='1 * * * *',
    start_date=datetime(2024, 10, 27),
    catchup=True,
    tags=['java', 'chat', 'log'],
    ) as dag:


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    spark_submit = BashOperator(
            task_id='spark.submit',
            bash_command="""
            $SPARK_HOME/bin/spark-submit /home/ubuntu/airflow/pyspark_db.py
            """
            )

    start >> spark_submit >> end
