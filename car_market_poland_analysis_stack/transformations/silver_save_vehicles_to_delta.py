from pyspark.sql import SparkSession, functions as F, types as T, DataFrame
from delta.tables import DeltaTable
import argparse

from schemas.vehicles_schema import raw_schema

def build_spark() -> SparkSession:
    return (
        SparkSession
        .builder
        .appName("silver_cepik_vehicles")
        .getOrCreate()
    )

def read_json(spark:SparkSession, path:str):
    return spark.read.schema(raw_schema).json(path)

def flatten_json_df(df:DataFrame) -> DataFrame:
    return  df.select(
    F.col("id"),
    F.col("type"),
    F.col("links.self").alias("link_self"),

    F.col("attributes.marka").alias("marka"),
    F.col("attributes.`kategoria-pojazdu`").alias("kategoria_pojazdu"),
    F.col("attributes.typ").alias("typ_attr"),
    F.col("attributes.model").alias("model"),
    F.col("attributes.wariant").alias("wariant"),
    F.col("attributes.wersja").alias("wersja"),

    F.col("attributes.`rodzaj-pojazdu`").alias("rodzaj_pojazdu"),
    F.col("attributes.`podrodzaj-pojazdu`").alias("podrodzaj_pojazdu"),
    F.col("attributes.`przeznaczenie-pojazdu`").alias("przeznaczenie_pojazdu"),
    F.col("attributes.`pochodzenie-pojazdu`").alias("pochodzenie_pojazdu"),
    F.col("attributes.`rodzaj-tabliczki-znamionowej`").alias("rodzaj_tabliczki_znamionowej"),
    F.col("attributes.`sposob-produkcji`").alias("sposob_produkcji"),

    F.col("attributes.`rok-produkcji`").alias("rok_produkcji"),

    F.col("attributes.`data-pierwszej-rejestracji-w-kraju`").alias("data_pierwszej_rejestracji_w_kraju"),
    F.col("attributes.`data-ostatniej-rejestracji-w-kraju`").alias("data_ostatniej_rejestracji_w_kraju"),
    F.col("attributes.`data-rejestracji-za-granica`").alias("data_rejestracji_za_granica"),

    F.col("attributes.`pojemnosc-skokowa-silnika`").alias("pojemnosc_skokowa_silnika"),
    F.col("attributes.`stosunek-mocy-silnika-do-masy-wlasnej-motocykle`").alias("stosunek_mocy_do_masy_motocykle"),
    F.col("attributes.`moc-netto-silnika`").alias("moc_netto_silnika"),
    F.col("attributes.`moc-netto-silnika-hybrydowego`").alias("moc_netto_silnika_hybrydowego"),

    F.col("attributes.`masa-wlasna`").alias("masa_wlasna"),
    F.col("attributes.`masa-pojazdu-gotowego-do-jazdy`").alias("masa_gotowa_do_jazdy"),
    F.col("attributes.`dopuszczalna-masa-calkowita`").alias("dmc"),
    F.col("attributes.`max-masa-calkowita`").alias("max_dmc"),
    F.col("attributes.`dopuszczalna-ladownosc`").alias("dopuszczalna_ladownosc"),
    F.col("attributes.`max-ladownosc`").alias("max_ladownosc"),
    F.col("attributes.`dopuszczalna-masa-calkowita-zespolu-pojazdow`").alias("dmc_zespolu"),

    F.col("attributes.`liczba-osi`").alias("liczba_osi"),
    F.col("attributes.`dopuszczalny-nacisk-osi`").alias("dopuszczalny_nacisk_osi"),
    F.col("attributes.`maksymalny-nacisk-osi`").alias("maksymalny_nacisk_osi"),

    F.col("attributes.`max-masa-calkowita-przyczepy-z-hamulcem`").alias("max_masa_przyczepy_z_hamulcem"),
    F.col("attributes.`max-masa-calkowita-przyczepy-bez-hamulca`").alias("max_masa_przyczepy_bez_hamulca"),

    F.col("attributes.`liczba-miejsc-ogolem`").alias("liczba_miejsc_ogolem"),
    F.col("attributes.`liczba-miejsc-siedzacych`").alias("liczba_miejsc_siedzacych"),
    F.col("attributes.`liczba-miejsc-stojacych`").alias("liczba_miejsc_stojacych"),

    F.col("attributes.`rodzaj-paliwa`").alias("rodzaj_paliwa"),
    F.col("attributes.`rodzaj-pierwszego-paliwa-alternatywnego`").alias("paliwo_alt_1"),
    F.col("attributes.`rodzaj-drugiego-paliwa-alternatywnego`").alias("paliwo_alt_2"),

    F.col("attributes.`srednie-zuzycie-paliwa`").alias("srednie_zuzycie_paliwa"),
    F.col("attributes.`poziom-emisji-co2`").alias("poziom_emisji_co2"),

    F.col("attributes.`rodzaj-zawieszenia`").alias("rodzaj_zawieszenia"),
    F.col("attributes.`wyposazenie-i-rodzaj-urzadzenia-radarowego`").alias("urzadzenie_radarowe"),

    F.col("attributes.hak").alias("hak"),
    F.col("attributes.`kierownica-po-prawej-stronie`").alias("kierownica_po_prawej"),
    F.col("attributes.`kierownica-po-prawej-stronie-pierwotnie`").alias("kierownica_po_prawej_pierwotnie"),
    F.col("attributes.`katalizator-pochlaniacz`").alias("katalizator_pochlaniacz"),

    F.col("attributes.`nazwa-producenta`").alias("nazwa_producenta"),
    F.col("attributes.`kod-instytutu-transaportu-samochodowego`").alias("kod_its"),

    F.col("attributes.`rozstaw-kol-osi-kierowanej-pozostalych-osi`").alias("rozstaw_kol_osi_kierowanej_pozostalych"),
    F.col("attributes.`max-rozstaw-kol`").alias("max_rozstaw_kol"),
    F.col("attributes.`avg-rozstaw-kol`").alias("avg_rozstaw_kol"),
    F.col("attributes.`min-rozstaw-kol`").alias("min_rozstaw_kol"),

    F.col("attributes.`redukcja-emisji-spalin`").alias("redukcja_emisji_spalin"),

    F.col("attributes.`data-pierwszej-rejestracji`").alias("data_pierwszej_rejestracji"),

    F.col("attributes.`rodzaj-kodowania-rodzaj-podrodzaj-przeznaczenie`").alias("rodzaj_kodowania"),
    F.col("attributes.`kod-rodzaj-podrodzaj-przeznaczenie`").alias("kod_rodzaj_podrodzaj_przeznaczenie"),

    F.col("attributes.`data-wyrejestrowania-pojazdu`").alias("data_wyrejestrowania_pojazdu"),
    F.col("attributes.`przyczyna-wyrejestrowania-pojazdu`").alias("przyczyna_wyrejestrowania_pojazdu"),
    F.col("attributes.`data-wprowadzenia-danych`").alias("data_wprowadzenia_danych"),

    F.col("attributes.`rejestracja-wojewodztwo`").alias("rejestracja_wojewodztwo"),
    F.col("attributes.`rejestracja-gmina`").alias("rejestracja_gmina"),
    F.col("attributes.`rejestracja-powiat`").alias("rejestracja_powiat"),

    F.col("attributes.`wlasciciel-wojewodztwo`").alias("wlasciciel_wojewodztwo"),
    F.col("attributes.`wlasciciel-powiat`").alias("wlasciciel_powiat"),
    F.col("attributes.`wlasciciel-gmina`").alias("wlasciciel_gmina"),
    F.col("attributes.`wlasciciel-wojewodztwo-kod`").alias("wlasciciel_wojewodztwo_kod"),

    F.col("attributes.`wojewodztwo-kod`").alias("wojewodztwo_kod"),
    F.col("attributes.`poziom-emisji-co2-paliwo-alternatywne-1`").alias("poziom_emisji_co2_paliwo_alt_1"),
)

def delta_table_exists(spark:SparkSession, path:str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except:
        return False

def create_delta_table(df, delta_path):
    (
        df.limit(0)
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("wojewodztwo_kod")
        .save(delta_path)
    )

def upsert_json_to_delta(spark, df, delta_path):
    delta_tbl = DeltaTable.forPath(spark, delta_path)
    (
        delta_tbl.alias("t")
        .merge(df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()
    df_json = read_json(spark, args.source)
    flattened_df = flatten_json_df(df_json)
    if not delta_table_exists(spark, args.sink):
        create_delta_table(flattened_df, args.sink)
    upsert_json_to_delta(spark, flattened_df, args.sink)

if __name__ == "__main__":
    main()