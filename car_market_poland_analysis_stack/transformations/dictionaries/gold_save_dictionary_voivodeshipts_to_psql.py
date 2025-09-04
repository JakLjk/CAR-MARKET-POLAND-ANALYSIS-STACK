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
        SparkSession.read.format("delta").load(delta_path)
    )

def load_df_into_postgresql(
        df:DataFrame,
        psql_url:str,
        psql_schema_table:str,
        psql_user:str,
        psql_password:str,
        psql_driver:str="org.postgresql.Driver",
        write_mode:str="overwrite"):
    (
        df
        .write
        .format("jdbc")
        .option("url", psql_url)
        .option("dbtable", psql_schema_table)
        .option("user", psql_user)
        .option("password", psql_password)
        .option("driver", psql_driver)
        .mode(write_mode)
        .save()
    )


def load_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source_url", required=True, help="Source uri for delta table with dictionary")
    p.add_argument("--sink_url", required=True, help="PSQL sink uri")
    p.add_argument("--psql_user", required=True)
    p.add_argument("--psql_password", required=True)
    p.add_argument("--psql_schema_table", required=True)
    p.add_argument("--psql_spark_driver", required=False)
    p.add_argument("--psql_write_mode", required=False)
    return p.parse_args()

def main():
    args=load_args()
    spark = build_spark()
    df = read_delta_table(spark, args.source)
    load_df_into_postgresql(
        df,
        psql_url=args.source_url,
        psql_schema_table=args.psql_schema_table,
        psql_user=args.psql_user,
        psql_password=args.psql_password,
        psql_driver=args.psql_spark_driver,
        psql_write_mode=args.psql_write_mode

    )



if __name__ == "__main__":
    main()