from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import os
import posixpath
import logging
from datetime import timedelta, datetime, date

HDFS_URI = Variable.get("hdfs-data-path")
LOCAL_URI = Variable.get("local-data-path")

from cepik.scraping.dictionaries.get_raw_dictionary import Dictionary, get_dictionary, write_dictionary

logger = logging.getLogger(__name__)


default_dag_args = {
    "owner":"pomeran",
    "retries":5,
    "retry_delay":timedelta(minutes=5)
}

common_spark_env_vars = {"HADOOP_USER_NAME":"airflow"}

common_spark_conf = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.defaultFS": "hdfs://hadoop-namenode:8020",
            "spark.jars.ivy": "/tmp/.ivy2",
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
        }


def load_dictionary(ti, ds_nodash, dict_name:Dictionary):
    local_path = f"{LOCAL_URI}/{ds_nodash}_dictionary_{dict_name.name.lower()}.json"
    d = get_dictionary(dict_name)
    save_path = write_dictionary(local_path, d)
    ti.xcom_push(key="local_save_path_dict_voivodeships", value=save_path)
    logger.info(f"Saved dictionary to local path:{save_path}")

def delete_local_raw_file(ti):
    local_path = ti.xcom_pull(
        task_ids="fetch_raw_voivodeship_data",
        key="local_save_path_dict_voivodeships")
    os.remove(local_path)
    
def save_dictionary_to_hdfs(ti, **context):
    p = context["params"]
    local_file_save_path = ti.xcom_pull(
        task_ids="fetch_raw_voivodeship_data", 
        key="local_save_path_dict_voivodeships")
    hdfs_data_path = p["bronze_hdfs_raw_voivodeships_data_path"]
    remote_fname = os.path.basename(local_file_save_path)
    relative_hdfs_save_path = posixpath.join(hdfs_data_path, remote_fname)
    if not local_file_save_path or not os.path.exists(local_file_save_path):
        raise FileNotFoundError(f"Path was not found: {local_file_save_path}")
    hook = WebHDFSHook(webhdfs_conn_id="conn-webhdfs")
    client = hook.get_conn()
    client.upload(relative_hdfs_save_path, local_file_save_path, overwrite=True)
    fully_qualified_path = posixpath.join(HDFS_URI, relative_hdfs_save_path)
    ti.xcom_push(key="hdfs_save_path_raw_dict_voivodeships", value=fully_qualified_path)
    logger.info(f"Saved dictionary to hdfs: {fully_qualified_path}")


with DAG(
    dag_id="cepik_dictionary_voivodeships_7",
    default_args=default_dag_args,
    start_date=datetime(2025,8,30),
    schedule="@daily",
    catchup=False,
    params = {
        "bronze_hdfs_raw_voivodeships_data_path":"/cepik/bronze",
        "silver_hdfs_delta_voivodeships_path":"/cepik/silver/dict_dictionaries",
        "pg_schema_table_dict_voivodeships": "public.dict_voivodeships"
    }
) as dag:
    
    fetch_raw_voivodeship_data = PythonOperator(
        task_id="fetch_raw_voivodeship_data",
        python_callable=load_dictionary,
        op_kwargs = {
            "dict_name":Dictionary.WOJEWODZTWA
        }
    )

    upload_raw_voivodeship_dict = PythonOperator(
        task_id="upload_raw_voivodeship_data",
        python_callable=save_dictionary_to_hdfs
    )

    delete_local_raw_voivodeship_dict = PythonOperator(
        task_id="delete_local_raw_file",
        python_callable=delete_local_raw_file
    )

    transform_raw_voivodeship_dict = SparkSubmitOperator(
        task_id="transform_raw_voivodeship_dict",
        conn_id="spark-conn",
        application="/opt/airflow/libs/cepik/transformations/silver_save_dictionary_voivodeships_to_delta.py",
        packages="io.delta:delta-spark_2.13:4.0.0",
        env_vars=common_spark_env_vars,
        conf=common_spark_conf,
        application_args=[
            "--input",  
            "{{ ti.xcom_pull(task_ids='upload_raw_voivodeship_data', key='hdfs_save_path_raw_dict_voivodeships') }}",

            "--output", 
            "{{ var.value['hdfs-data-path'] }}{{ params.silver_hdfs_delta_voivodeships_path }}",
            "--mode",   "overwrite",
        ],
        verbose=True,
    )

    load_voivodeships_into_psql_db = SparkSubmitOperator(
        task_id="load_voivodeships_into_psql_db",
        conn_id="spark-conn",
        application="/opt/airflow/libs/cepik/transformations/gold_save_dictionary_voivodeshipts_to_psql.py",
        packages="io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.7.3",
        env_vars=common_spark_env_vars,
        conf=common_spark_conf,
        application_args=[
            "--source_url",
            "{{ var.value['hdfs-data-path'] }}{{ params.silver_hdfs_delta_voivodeships_path }}",

            "--sink_url",
            "jdbc:postgresql://{{ conn['conn-pg-cepik-db'].host }}:{{ conn['conn-pg-cepik-db'].port or 5432 }}/{{ conn['conn-pg-cepik-db'].schema }}?sslmode={{ conn['conn-pg-cepik-db'].extra_dejson.get('sslmode','disable') }}&rewriteBatchedInserts={{ conn['conn-pg-cepik-db'].extra_dejson.get('rewriteBatchedInserts','true') }}",

            "--psql_user",
            "{{ conn['conn-pg-cepik-db'].login }}",

            "--psql_password",
            "{{ conn['conn-pg-cepik-db'].password }}",

            "--psql_schema_table",
            "{{ params.pg_schema_table_dict_voivodeships }}",
        ],
        verbose=True,
    )

    fetch_raw_voivodeship_data >> upload_raw_voivodeship_dict >> delete_local_raw_voivodeship_dict >> transform_raw_voivodeship_dict >> load_voivodeships_into_psql_db