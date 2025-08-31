from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
from delta.tables import DeltaTable
from typing import List

ATTRS: List[str] = [
    "marka",
    "kategoria-pojazdu",
    "typ",
    "model",
    "wariant",
    "wersja",
    "rodzaj-pojazdu",
    "podrodzaj-pojazdu",
    "przeznaczenie-pojazdu",
    "pochodzenie-pojazdu",
    "rodzaj-tabliczki-znamionowej",
    "sposob-produkcji",
    "rok-produkcji",
    "data-pierwszej-rejestracji-w-kraju",
    "data-ostatniej-rejestracji-w-kraju",
    "data-rejestracji-za-granica",
    "pojemnosc-skokowa-silnika",
    "moc-netto-silnika",
    "moc-netto-silnika-hybrydowego",
    "masa-wlasna",
    "masa-pojazdu-gotowego-do-jazdy",
    "dopuszczalna-masa-calkowita",
    "max-masa-calkowita",
    "dopuszczalna-ladownosc",
    "max-ladownosc",
    "dopuszczalna-masa-calkowita-zespolu-pojazdow",
    "liczba-osi",
    "dopuszczalny-nacisk-osi",
    "maksymalny-nacisk-osi",
    "max-masa-calkowita-przyczepy-z-hamulcem",
    "max-masa-calkowita-przyczepy-bez-hamulca",
    "liczba-miejsc-ogolem",
    "liczba-miejsc-siedzacych",
    "liczba-miejsc-stojacych",
    "rodzaj-paliwa",
    "rodzaj-pierwszego-paliwa-alternatywnego",
    "rodzaj-drugiego-paliwa-alternatywnego",
    "srednie-zuzycie-paliwa",
    "poziom-emisji-co2",
    "rodzaj-zawieszenia",
    "wyposazenie-i-rodzaj-urzadzenia-radarowego",
    "hak",
    "kierownica-po-prawej-stronie",
    "kierownica-po-prawej-stronie-pierwotnie",
    "katalizator-pochlaniacz",
    "nazwa-producenta",
    "kod-instytutu-transaportu-samochodowego",
    "rozstaw-kol-osi-kierowanej-pozostalych-osi",
    "max-rozstaw-kol",
    "avg-rozstaw-kol",
    "min-rozstaw-kol",
    "redukcja-emisji-spalin",
    "data-pierwszej-rejestracji",
    "rodzaj-kodowania-rodzaj-podrodzaj-przeznaczenie",
    "kod-rodzaj-podrodzaj-przeznaczenie",
    "data-wyrejestrowania-pojazdu",
    "przyczyna-wyrejestrowania-pojazdu",
    "data-wprowadzenia-danych",
    "rejestracja-wojewodztwo",
    "rejestracja-gmina",
    "rejestracja-powiat",
    "wlasciciel-wojewodztwo",
    "wlasciciel-powiat",
    "wlasciciel-gmina",
    "wlasciciel-wojewodztwo-kod",
    "wojewodztwo-kod",
    "poziom-emisji-co2-paliwo-alternatywne-1",
]

def build_spark() -> SparkSession:
    return (
       SparkSession.builder
       .appName("cepik_silver")
       .master("spark://192.168.0.12:7077")
       .getOrCreate()

    )

def hyphen_to_snake(name: str) -> str:
    return name.replace("-", "_")

def read_json(spark:SparkSession, path:str) -> DataFrame:
   return spark.read.json(path)

def flatten(df:DataFrame):
    base = df.select(
        F.col("id").cast("string").alias("id"),
        F.col("type").cast("string").alias("type"),
        F.col("links.self").cast("string").alias("link")
    )

    flat = base
    for a in ATTRS:
        flat = flat.withColumn(hyphen_to_snake(a), F.col(f"attributes.`{a}`"))
    return flat

def initial_write(delta_path: str, df: DataFrame) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(delta_path)
    )

def upsert(delta_path: str, df: DataFrame) -> None:
    spark = df.sparkSession
    if DeltaTable.isDeltaTable(spark, delta_path):
        tgt = DeltaTable.forPath(spark, delta_path)
        (
            tgt.alias("t")
            .merge(df.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        initial_write(delta_path, df)

def main():
    INPUT_PATH = "/home/thinkpad/shared_folder/PROJECTS_2/CAR-MARKET-POLAND-ANALYSIS-STACK/car_market_poland_analysis_stack/test.json"
    DELTA_PATH = "hdfs://192.168.0.12:8020/user/thinkpad/delta/cepik/silver"

    spark = build_spark()
    raw = read_json(spark, INPUT_PATH)
    flat = flatten(raw)
    upsert(DELTA_PATH, flat)
    spark.stop()

if __name__ == "__main__":
 main()