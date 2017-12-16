from datetime import datetime, timedelta
import posixpath as pp

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.models import Variable

CONNECTION_ID = 'google_cloud_default'

config = Variable.get('pipe_template', deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 1),
    'email': ['airflow@globalfishingwatch.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'project_id': config['project_id'],
    'dataset_id': config['pipeline_dataset'],
    'bucket': config['pipeline_bucket'],
    'bigquery_conn_id': CONNECTION_ID,
    'gcp_conn_id': CONNECTION_ID,
    'google_cloud_conn_id': CONNECTION_ID,
    'write_disposition': 'WRITE_TRUNCATE',
    'allow_large_results': True,
}


with DAG('pipe_template', schedule_interval=timedelta(days=1), default_args=default_args) as dag:

    dataflow_test=DataFlowPythonOperator(
        task_id='dataflow-test',
        py_file=Variable.get('DATAFLOW_WRAPPER_STUB'),
        options=dict(
            command='{docker_run} {docker_image} dataflow_test'.format(**config),
            startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'), 'pipe_template/dataflow_test.log'),
            tag_field='{tag_field}'.format(**config),
            tag_value='{tag_value}'.format(**config),
            dest='gs://{pipeline_bucket}/{output_path}'.format(**config),
            project=config['project_id'],
            runner='DataflowRunner',
            temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
            staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
            disk_size_gb="50",
            max_num_workers="4",
            requirements_file='./requirements.txt',
            setup_file='./setup.py',
        ),
        dag=dag
    )

