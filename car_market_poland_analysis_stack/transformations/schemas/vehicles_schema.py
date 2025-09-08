from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, DateType
)

raw_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),

    StructField("attributes", StructType([
        StructField("marka", StringType(), True),
        StructField("kategoria-pojazdu", StringType(), True),
        StructField("typ", StringType(), True),
        StructField("model", StringType(), True),
        StructField("wariant", StringType(), True),
        StructField("wersja", StringType(), True),

        StructField("rodzaj-pojazdu", StringType(), True),
        StructField("podrodzaj-pojazdu", StringType(), True),
        StructField("przeznaczenie-pojazdu", StringType(), True),
        StructField("pochodzenie-pojazdu", StringType(), True),
        StructField("rodzaj-tabliczki-znamionowej", StringType(), True),
        StructField("sposob-produkcji", StringType(), True),

        StructField("rok-produkcji", StringType(), True),

        StructField("data-pierwszej-rejestracji-w-kraju", DateType(), True),
        StructField("data-ostatniej-rejestracji-w-kraju", DateType(), True),
        StructField("data-rejestracji-za-granica", DateType(), True),

        StructField("pojemnosc-skokowa-silnika", DoubleType(), True),
        StructField("stosunek-mocy-silnika-do-masy-wlasnej-motocykle", DoubleType(), True),
        StructField("moc-netto-silnika", DoubleType(), True),
        StructField("moc-netto-silnika-hybrydowego", DoubleType(), True),

        StructField("masa-wlasna", IntegerType(), True),
        StructField("masa-pojazdu-gotowego-do-jazdy", IntegerType(), True),
        StructField("dopuszczalna-masa-calkowita", IntegerType(), True),
        StructField("max-masa-calkowita", IntegerType(), True),
        StructField("dopuszczalna-ladownosc", IntegerType(), True),
        StructField("max-ladownosc", IntegerType(), True),
        StructField("dopuszczalna-masa-calkowita-zespolu-pojazdow", IntegerType(), True),

        StructField("liczba-osi", IntegerType(), True),
        StructField("dopuszczalny-nacisk-osi", DoubleType(), True),
        StructField("maksymalny-nacisk-osi", DoubleType(), True),

        StructField("max-masa-calkowita-przyczepy-z-hamulcem", IntegerType(), True),
        StructField("max-masa-calkowita-przyczepy-bez-hamulca", IntegerType(), True),

        StructField("liczba-miejsc-ogolem", IntegerType(), True),
        StructField("liczba-miejsc-siedzacych", IntegerType(), True),
        StructField("liczba-miejsc-stojacych", IntegerType(), True),

        StructField("rodzaj-paliwa", StringType(), True),
        StructField("rodzaj-pierwszego-paliwa-alternatywnego", StringType(), True),
        StructField("rodzaj-drugiego-paliwa-alternatywnego", StringType(), True),

        StructField("srednie-zuzycie-paliwa", DoubleType(), True),
        StructField("poziom-emisji-co2", DoubleType(), True),

        StructField("rodzaj-zawieszenia", StringType(), True),
        StructField("wyposazenie-i-rodzaj-urzadzenia-radarowego", StringType(), True),

        StructField("hak", BooleanType(), True),
        StructField("kierownica-po-prawej-stronie", BooleanType(), True),
        StructField("kierownica-po-prawej-stronie-pierwotnie", BooleanType(), True),
        StructField("katalizator-pochlaniacz", BooleanType(), True),

        StructField("nazwa-producenta", StringType(), True),
        StructField("kod-instytutu-transaportu-samochodowego", StringType(), True),

        StructField("rozstaw-kol-osi-kierowanej-pozostalych-osi", DoubleType(), True),
        StructField("max-rozstaw-kol", IntegerType(), True),
        StructField("avg-rozstaw-kol", IntegerType(), True),
        StructField("min-rozstaw-kol", IntegerType(), True),

        StructField("redukcja-emisji-spalin", StringType(), True),

        StructField("data-pierwszej-rejestracji", DateType(), True),

        StructField("rodzaj-kodowania-rodzaj-podrodzaj-przeznaczenie", StringType(), True),
        StructField("kod-rodzaj-podrodzaj-przeznaczenie", StringType(), True),

        StructField("data-wyrejestrowania-pojazdu", DateType(), True),
        StructField("przyczyna-wyrejestrowania-pojazdu", StringType(), True),
        StructField("data-wprowadzenia-danych", DateType(), True),

        StructField("rejestracja-wojewodztwo", StringType(), True),
        StructField("rejestracja-gmina", StringType(), True),
        StructField("rejestracja-powiat", StringType(), True),

        StructField("wlasciciel-wojewodztwo", StringType(), True),
        StructField("wlasciciel-powiat", StringType(), True),
        StructField("wlasciciel-gmina", StringType(), True),
        StructField("wlasciciel-wojewodztwo-kod", StringType(), True),

        StructField("wojewodztwo-kod", StringType(), True),
        StructField("poziom-emisji-co2-paliwo-alternatywne-1", DoubleType(), True),
    ]), True),

    StructField("links", StructType([
        StructField("self", StringType(), True),
    ]), True),
])