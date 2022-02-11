from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator

dag = DAG(
    dag_id="my_dag",
    description="Runs AWS Batch job",
    default_args={"owner": "Airflow"},
    schedule_interval="once",
)

run_job = AwsBatchOperator(
    task_id="run_batch_job",
    dag=dag,
    job_name="yannick-jobrun-airflow",
    job_definition="Yannick-job",
    job_queue="academy-capstone-winter-2022-job-queue",
    overrides={}
)