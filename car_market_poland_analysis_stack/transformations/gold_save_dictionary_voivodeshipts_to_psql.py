from pyspark.sql import SparkSession, functions as F, types as T, DataFrame

import argparse


def build_spark() -> SparkSession:
    return (
        SparkSession
        .builder
        .appName("gold_dictionary_voivodeships")
        .getOrCreate()
    )

def read_delta_table(spark:SparkSession, delta_path:str) -> DataFrame:
    return (
        spark.read.format("delta").load(delta_path)
    )

def load_df_into_postgresql(
        df:DataFrame,
        jdbc_url:str,
        psql_user:str,
        psql_password:str,
        psql_schema_table:str,
        psql_driver:str="org.postgresql.Driver",
        write_mode:str="overwrite",
        write_truncate:str="true",
        write_batch_size:int=1000,):
    writer = (
        df
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("user", psql_user)
        .option("password", psql_password)
        .option("dbtable", psql_schema_table)
        .option("driver", psql_driver)
        .option("batchsize", str(write_batch_size))
    )
    if str(write_truncate).lower() == "true" and write_mode=="overwrite":
        writer.option("truncate", "true")
    writer.mode(write_mode).save()




def load_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source_url", required=True, help="Source uri for delta table with dictionary")
    p.add_argument("--sink_url", required=True, help="PSQL sink uri")
    p.add_argument("--psql_user", required=True, help="PSQL sink uri")
    p.add_argument("--psql_password", required=True, help="PSQL sink uri")
    p.add_argument("--psql_schema_table", required=True)
    p.add_argument("--psql_spark_driver", required=False, default="org.postgresql.Driver")
    p.add_argument("--psql_write_mode", required=False, default="overwrite")
    p.add_argument("--psql_write_batchsize", required=False, type=int, default=1000)
    p.add_argument("--psql_write_truncate", required=False, default="true")
    return p.parse_args()

def main():
    args=load_args()
    spark = build_spark()
    df = read_delta_table(spark, args.source_url)
    load_df_into_postgresql(
        df,
        jdbc_url=args.sink_url,
        psql_user=args.psql_user,
        psql_password=args.psql_password,
        psql_schema_table=args.psql_schema_table,
        psql_driver=args.psql_spark_driver,
        write_mode=args.psql_write_mode,
        write_truncate=args.psql_write_truncate,
        write_batch_size=args.psql_write_batchsize,

    )



if __name__ == "__main__":
    main()