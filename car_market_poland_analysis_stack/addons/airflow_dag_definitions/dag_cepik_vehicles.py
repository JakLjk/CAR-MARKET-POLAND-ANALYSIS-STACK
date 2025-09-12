from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import os
import posixpath
import logging
from datetime import timedelta, datetime, date

from cepik.scraping.vehicles.get_raw_vehicles import iter_vehicles, write_ndjson


HDFS_URI = Variable.get("hdfs-data-path")
LOCAL_URI = Variable.get("local-data-path")
FETCH_POOL = Variable.get("task_pool", default_var="cepik_api_pool")

logger = logging.getLogger(__name__)

default_dag_args = {
    "owner":"pomeran",
    "retries":10,
    "retry_delay":timedelta(hours=1)
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

def _local_path_vehicles_ndjson(run_id:str, voivodeship_id:str) -> str:
    return os.path.join(LOCAL_URI, run_id, f"vehicles_{voivodeship_id}.ndjson")

def _hdfs_save_path(hdfs_base_path:str, run_id:str, filename:str):
    return posixpath.join(hdfs_base_path, run_id, filename)

with DAG(
    dag_id="cepik_vehicles_current_v01",
    default_args=default_dag_args,
    start_date=datetime(2025,9,1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    params = {
        "postgres_connection":"conn-pg-cepik-db",

        "query_sql_voivodeships":
        """
        SELECT klucz_slownika
        FROM public.dict_voivodeships;
        """,

        "call_upsert_vehicles_psql_function":
        """
        SELECT public.upsert_vehicles_from_stage() 
        AS affected
        """,

        "task_pool":"cepik_api_pool",

        "get_raw_data_offset_from_curr_day":-7,
        "get_raw_data_for_num_of_days":3,

        "bronze_hdfs_raw_vehicles_data_path":"/cepik/bronze",
        "psql_staging_vehicles_schema_table":"public.staging_vehicles"
    }
):

    @task
    def get_voivodeship_ids(sql:str, pg_conn_id:str):
        hook = PostgresHook.get_hook(pg_conn_id)
        rows = hook.get_records(sql=sql)
        return [str(r[0]) for r in rows]
    
    
    @task(pool=FETCH_POOL)
    def download_raw_vehicle_json(voivodeship_code:str):
        context = get_current_context()
        run_stamp = context["ds_nodash"]
        run_date = context["logical_date"].date()
        day_offset = int(context["params"].get("get_raw_data_offset_from_curr_day"))
        num_days = int(context["params"].get("get_raw_data_for_num_of_days"))

        date_to = run_date + timedelta(days=day_offset)
        date_from = date_to -timedelta(days=num_days)

        logger.info(f"Scraping vehicle information from {str(date_from)} to {str(date_to)}")
        vehicle_iterator = iter_vehicles(
            date_from=date_from,
            date_to=date_to,
            voivodeship_code=voivodeship_code
        )
        path = write_ndjson(
            path=_local_path_vehicles_ndjson(run_stamp, voivodeship_code),
            records=vehicle_iterator)
        return path
    
    @task
    def save_local_data_to_hdfs(
            local_path:str,
            bronze_base:str,
            remove_local_file_after_upload:bool=True
    ) -> str:
        context = get_current_context()
        run_stamp = context["ds_nodash"]
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file path not found: {local_path}")
        filename = os.path.basename(local_path)
        rel_hdfs_path = _hdfs_save_path(
            hdfs_base_path=bronze_base, 
            run_id=run_stamp, 
            filename=filename)
        hook = WebHDFSHook(webhdfs_conn_id="conn-webhdfs")
        client = hook.get_conn()
        client.upload(
            rel_hdfs_path,
            local_path,
            overwrite=True
        )
        if remove_local_file_after_upload:
            os.remove(local_path)

        full_hdfs_path = posixpath.join(HDFS_URI, rel_hdfs_path)
        return full_hdfs_path

        
    load_vehicles_into_psql_staging_table = SparkSubmitOperator(
        task_id = "load_vehicles_into_psql_staging_table",
        conn_id = "spark-conn",
        application = "/opt/airflow/libs/cepik/transformations/load_vehicles_to_psql_stage.py",
        packages="io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.7.3",
        env_vars=common_spark_env_vars,
        conf=common_spark_conf,
        application_args=[
            "--source_url",
            "{{var.value['hdfs-data-path']}}{{params.bronze_hdfs_raw_vehicles_data_path}}/{{ds_nodash}}/",

            "--sink_url",
            "jdbc:postgresql://{{ conn['conn-pg-cepik-db'].host }}:{{ conn['conn-pg-cepik-db'].port or 5432 }}/{{ conn['conn-pg-cepik-db'].schema }}?sslmode={{ conn['conn-pg-cepik-db'].extra_dejson.get('sslmode','disable') }}&rewriteBatchedInserts={{ conn['conn-pg-cepik-db'].extra_dejson.get('rewriteBatchedInserts','true') }}",

            "--psql_user",
            "{{ conn['conn-pg-cepik-db'].login }}",

            "--psql_password",
            "{{ conn['conn-pg-cepik-db'].password }}",

            "--psql_schema_table",
            "{{ params.psql_staging_vehicles_schema_table }}",
        ],
        verbose=False
    )

    @task
    def upsert_stg_vehicles_into_main_table(sql:str, pg_conn_id:str):
        hook = PostgresHook.get_hook(pg_conn_id)
        # explicit conenction
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                affected_rows = cur.fetchone()[0]
            conn.commit()
        finally:
            conn.close()
        return affected_rows
    
        
    voivodeship_ids = get_voivodeship_ids(
        sql="{{ params.query_sql_voivodeships }}",
        pg_conn_id="{{ params.postgres_connection}}"
        )
    

    local_paths = (
        download_raw_vehicle_json
        .expand(voivodeship_code=voivodeship_ids)
        )

    hdfs_paths = (
        save_local_data_to_hdfs
        .partial(
            bronze_base="{{params.bronze_hdfs_raw_vehicles_data_path}}"
            )
        .expand(local_path=local_paths)
    )

    upsert_stg_to_main = upsert_stg_vehicles_into_main_table(
        sql="{{params.call_upsert_vehicles_psql_function}}",
        pg_conn_id="{{ params.postgres_connection}}"
    )


    voivodeship_ids >> local_paths >> hdfs_paths >> load_vehicles_into_psql_staging_table >> upsert_stg_to_main


