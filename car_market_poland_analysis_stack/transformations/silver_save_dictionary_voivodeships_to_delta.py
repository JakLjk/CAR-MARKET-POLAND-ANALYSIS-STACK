from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
import argparse

from schemas.dict_voivodeships_schema import raw_schema

def build_spark() -> SparkSession:
    return (
        SparkSession
        .builder
        .appName("silver_dictionary_voivodeships")
        .getOrCreate()
    )
def read_json(spark:SparkSession, path:str) -> DataFrame:
   return spark.read.schema(raw_schema).option("multiline", True).json(path)

def flatten(df:DataFrame) -> DataFrame:
    # return df.select(
    #     F.col("data.id").alias("id_slownika"),
    #     F.col("data.links.self").alias("link_do_slownika"),
    #     F.explode("data.attributes.`dostepne-rekordy-slownika`").alias("rec")
    # ).select(
    #     "id_slownika",
    #     "link_do_slownika",
    #     F.col("rec.`klucz-slownika`").alias("klucz_slownika"),
    #     F.col("rec.`wartosc-slownika`").alias("wartosc_slownika"),
    #     F.col("rec.`liczba-wystapien`").alias("liczba_wystapien")
    # )

    exploded = (
       df
       .select(
            F.col("data.id").alias("id_slownika"),
            F.col("data.links.self").alias("link_do_slownika"),
            F.col("data.attributes.`dostepne-rekordy-slownika`").alias("items")
       )
       .withColumn("rec", F.explode_outer("items"))
    )
    flat = exploded.select(
       "id_slownika",
       "link_do_slownika",
        F.col("rec.`klucz-slownika`").alias("klucz_slownika"),
        F.col("rec.`wartosc-slownika`").alias("wartosc_slownika"),
        F.col("rec.`liczba-wystapien`").cast("bigint").alias("liczba_wystapien"),
    )
    return flat.dropDuplicates(["klucz_slownika"])



def save_to_delta(df:DataFrame, delta_path:str, mode:str="overwrite") -> None:
    (
       df
       .write
       .format("delta")
       .mode(mode)
       .save(delta_path)
    )

def parse_args():
   p = argparse.ArgumentParser()
   p.add_argument("--input", required=True, help="Input path from where spark job should load file")
   p.add_argument("--output", required=True, help="Output path to where delta table should be saved")
   p.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], 
                  help="Mode in which delta table should be saved")
   return p.parse_args()

def main():
    args=parse_args()
    spark = build_spark()
    raw = read_json(spark, args.input)
    flat = flatten(raw)
    save_to_delta(flat, args.output, args.mode)

if __name__ == "__main__":
 main()