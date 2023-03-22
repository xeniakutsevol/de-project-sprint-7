import sys

import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.window import Window

from common_functions import add_coords_timezone, calculate_user_localtime


def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    geo_base_path = sys.argv[4]
    recs_dtmrt_base_path = sys.argv[5]

    conf = SparkConf().setAppName(f"RecommendationsDatamart-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    end_date = F.to_date(F.lit(date), "yyyy-MM-dd")

    events_recommendations_datamart = sql.read.parquet(events_base_path).filter(
        F.col("date").between(F.date_sub(end_date, days_count), end_date)
    )

    geo = (
        sql.read.option("delimiter", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(geo_base_path)
    )

    recs_dtmrt = calculate_recommendations_datamart(
        events_recommendations_datamart,
        add_coords_timezone(events_recommendations_datamart, geo),
        calculate_user_distance(
            add_coords_timezone(events_recommendations_datamart, geo)
        ),
    )
    recs_dtmrt.write.parquet(f"{recs_dtmrt_base_path}/date={date}/depth={days_count}")


def calculate_user_distance(message_city_df):
    window = Window().partitionBy(["user_id", "date"]).orderBy(F.desc("datetime"))
    user_date_lat_lon = (
        message_city_df.select(
            "user_id", "date", "datetime", "message_lat", "message_lon", "city"
        )
        .withColumn("rn", F.row_number().over(window))
        .where("rn=1")
        .select("user_id", "date", "message_lat", "message_lon", "city")
    )

    users_joined = user_date_lat_lon.join(
        user_date_lat_lon.select(
            F.col("user_id").alias("user_right"),
            "date",
            F.col("message_lat").alias("lat_right"),
            F.col("message_lon").alias("lon_right"),
            F.col("city").alias("city_right"),
        ),
        "date",
    )

    users_joined_distance = users_joined.withColumn(
        "distance",
        F.acos(
            F.sin(F.col("message_lat")) * F.sin(F.col("lat_right"))
            + F.cos(F.col("message_lat"))
            * F.cos(F.col("lat_right"))
            * F.cos(F.col("message_lon") - F.col("lon_right"))
        )
        * F.lit(6371.0),
    ).where("distance<=1 and user_id<user_right")

    return users_joined_distance


def calculate_recommendations_datamart(events_df, message_city_df, users_distance_df):
    subs = (
        events_df.where("event.subscription_channel is not null")
        .select(F.col("event.user").alias("user_id"), "event.subscription_channel")
        .distinct()
    )
    subs_pairs = subs.join(
        subs.select(F.col("user_id").alias("user_right"), "subscription_channel"),
        "subscription_channel",
    )

    messages = (
        events_df.where(
            "event.message_from is not null and event.message_to is not null"
        )
        .select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_to").alias("user_right"),
        )
        .distinct()
    )
    messages_pairs = messages.union(
        messages.select(
            F.col("user_id").alias("user_right"), F.col("user_right").alias("user_id")
        )
    )

    local_time = calculate_user_localtime(message_city_df)

    recommendations_datamart = (
        users_distance_df.join(subs_pairs, on=["user_id", "user_right"], how="leftsemi")
        .join(messages_pairs, on=["user_id", "user_right"], how="leftanti")
        .join(local_time, on="user_id", how="left")
        .select(
            F.col("user_id").alias("user_left"),
            "user_right",
            F.col("date").alias("processed_dttm"),
            F.col("city").alias("zone_id"),
        )
    )

    return recommendations_datamart


if __name__ == "__main__":
    main()
