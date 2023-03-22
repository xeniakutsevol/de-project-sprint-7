import pyspark.sql.functions as F
from pyspark.sql.window import Window


def add_coords_timezone(events_df, geo_df):
    joined = events_df.select(
        "event.message_id",
        F.col("event.message_from").alias("user_id"),
        F.col("lat").alias("message_lat"),
        F.col("lon").alias("message_lon"),
        "date",
        F.to_timestamp("event.datetime").alias("datetime"),
    ).crossJoin(
        geo_df.select(
            "city",
            F.col("lat").alias("city_lat"),
            F.col("lon").alias("city_lon"),
            F.col("tz").alias("city_tz"),
        )
    )

    window = Window().partitionBy(["message_id"]).orderBy("distance")
    message_city = (
        joined.withColumn(
            "distance",
            F.acos(
                F.sin(F.col("message_lat")) * F.sin(F.col("city_lat"))
                + F.cos(F.col("message_lat"))
                * F.cos(F.col("city_lat"))
                * F.cos(F.col("message_lon") - F.col("city_lon"))
            )
            * F.lit(6371.0),
        )
        .withColumn("rn", F.row_number().over(window))
        .where("rn=1")
    )
    return message_city


def calculate_user_localtime(message_city_df):
    window = Window().partitionBy(["user_id"]).orderBy(F.desc("datetime"))
    local_time = (
        message_city_df.select("user_id", "datetime", "city_tz")
        .withColumn("rn", F.row_number().over(window))
        .where("rn=1")
        .withColumn(
            "local_time", F.from_utc_timestamp(F.col("datetime"), F.col("city_tz"))
        )
        .drop("datetime", "city_tz", "rn")
    )
    return local_time
