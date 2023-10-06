from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
import datetime

# use the dag decorator to inform airflow that we are definition a pipeline
@dag(
        # date at which our DAG starts being scheduled
        start_date=datetime.datetime(2023,10,1),
        # define how often the scheduler triggers this DAG. expects cron expression.
        schedule="@daily",
        # allow filtering DAGs on the Airflow UI
        tags=["shikhar"],
        # avoid running non-triggered DAG runs bewteen last execution & current date
        catchup=False,
)
# function name is the unique ID of this DAG
def shikhar_etl():

    # This operator returns a python list with the name of objects 
    # This can be used via XCom in the downstream task.
    # XCom stands for cross communication & is used to pass data between airflow tasks.
    # Using XCom the files found via GCSListObjectsOperator can be passed to
    # later airflow tasks to processes these files on Dataproc
    files_on_gcs = GCSListObjectsOperator(
        task_id='find_files_on_GCS',
        # example file:
        # gs://python-lab-329118-demo-dataproc-pipeline/shikharorder2023-09-27T22:35:15+00:00_07a589.jsonl
        bucket='python-lab-329118-demo-dataproc-pipeline',
        match_glob='*.jsonl',
        # Cloud Composer configures default connections in your environment. 
        # Can use these connections to access resources without configuring them.
        # https://cloud.google.com/composer/docs/composer-2/manage-airflow-connections#preconfigured-connections
        gcp_conn_id="google_cloud_storage_default"
    )

    # The airflow context is a dictionary with info about running DAG & airlfow env
    # ti refers to task instance which facilitates the use of xcom
    # recall xcom is how we fetched the data returned by other tasks, e.g. GCS above
    # https://docs.astronomer.io/learn/airflow-context?tab=traditional#task-decorator-PythonOperator
    # see also other ways to pass data between airflow tasks
    # https://docs.astronomer.io/learn/airflow-passing-data-between-tasks?tab=traditional#example-dag
    def print_found_files(**context):
        ti = context["ti"]
        files = ti.xcom_pull(key="return_value", task_ids="find_files_on_GCS")
        for file in files:
            print(file)

    process_files = PythonOperator(
        task_id="process_files",
        python_callable=print_found_files,
        provide_context=True
    )

    # specify dependencies of airflow tasks
    files_on_gcs >> process_files

# execute the DAG by calling the decorated function
shikhar_etl()