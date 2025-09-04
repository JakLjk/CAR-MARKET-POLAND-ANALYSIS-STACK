from pyspark.sql import types as T

raw_schema = T.StructType([
    T.StructField("data", T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("links", T.StructType([
            T.StructField("self", T.StringType())
        ])),
        T.StructField("attributes", T.StructType([
            T.StructField("dostepne-rekordy-slownika", T.ArrayType(
                T.StructType([
                    T.StructField("klucz-slownika", T.StringType()),
                    T.StructField("wartosc-slownika", T.StringType()),
                    T.StructField("liczba-wystapien", T.LongType()),
                ])
            ))
        ])),
    ]))
])