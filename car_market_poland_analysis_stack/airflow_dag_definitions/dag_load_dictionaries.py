from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

import os

from datetime import timedelta, datetime, date

from scraping.dictionaries import Dictionary, get_dictionary, write_dictionary

DEFAULT_OUTPUT_DIR = "/opt/airflow/data"
DEFAULT_HDFS_BRONZE_DIR = "/cepik/bronze"

def load_dictionary(ti, ds_nodash, dict_name:Dictionary, local_path:str=None):
    local_path = local_path or f"{DEFAULT_OUTPUT_DIR}/{ds_nodash}_dictionary_{dict_name.name.lower()}.json"
    d = get_dictionary(dict_name)
    path = write_dictionary(local_path, d)
    ti.xcom_push(key="local_path", value=path)


def save_dictionary_to_hdfs(ti, hdfs_path:str=None):
    local_path = ti.xcom_pull(task_ids="fetch_raw_voivodeship_data", key="local_path")
    hdfs_path = hdfs_path or f"{DEFAULT_HDFS_BRONZE_DIR}/{os.path.basename(local_path)}"
    if not local_path or not os.path.exists(local_path):
        raise FileNotFoundError(f"Path was not found: {local_path}")
    hook = WebHDFSHook(webhdfs_conn_id="conn_webhdfs")
    client = hook.get_conn()
    client.upload(hdfs_path, local_path, overwrite=True)
    return hdfs_path



default_args = {
    "owner":"pomeran",
    "retries":5,
    "retry_delay":timedelta(minutes=5)
}


with DAG(
    dag_id="load_dictionary_data_v19",
    default_args=default_args,
    start_date=datetime(2025,8,30),
    schedule="@weekly"
) as dag:
    fetch_raw_voivodeship_data = PythonOperator(
        task_id="fetch_raw_voivodeship_data",
        python_callable=load_dictionary,
        op_kwargs = {
            "dict_name":Dictionary.WOJEWODZTWA
        }
    )

    upload_raw_voivodeship_data = PythonOperator(
        task_id="upload_raw_voivodeship_data",
        python_callable=save_dictionary_to_hdfs
    )

    fetch_raw_voivodeship_data >> upload_raw_voivodeship_data