import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime


def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    geo_base_path = sys.argv[4]
    users_dtmrt_base_path = sys.argv[5]
    zones_dtmrt_base_path = sys.argv[6]
    recs_dtmrt_base_path = sys.argv[7]

    conf = SparkConf().setAppName(f"UsersDatamart-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    events_users_datamart = (
        sql.read.option("basePath", events_base_path)
        .parquet(*input_event_paths(events_base_path, date, days_count))
        .where("event.message_from is not null and event_type = 'message'")
    )

    events_zones_datamart = (
        sql.read.option("basePath", events_base_path)
        .parquet(*input_event_paths(events_base_path, date, days_count))
        .sample(0.001, 123)
    )

    # В исходный датасет добавлена колонка с timezone.
    geo = (
        sql.read.option("delimiter", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(geo_base_path)
    )

    users_dtmrt = calculate_users_datamart(
        add_coords_timezone(events_users_datamart, geo)
    )
    users_dtmrt.write.parquet(f"{users_dtmrt_base_path}/date={date}/depth={days_count}")

    zones_dtmrt = calculate_zones_datamart(
        add_coords_timezone(events_zones_datamart, geo)
    )
    zones_dtmrt.write.parquet(f"{zones_dtmrt_base_path}/date={date}/depth={days_count}")

    recs_dtmrt = calculate_recommendations_datamart(
        add_coords_timezone(events_users_datamart, geo),
        calculate_user_distance(add_coords_timezone(events_users_datamart, geo)),
    )
    recs_dtmrt.write.parquet(f"{recs_dtmrt_base_path}/date={date}/depth={days_count}")


def input_event_paths(base_path, date, depth):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"{base_path}/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
        for x in range(int(depth))
    ]


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
                F.sin(F.toRadians(F.col("message_lat")))
                * F.sin(F.toRadians(F.col("city_lat")))
                + F.cos(F.toRadians(F.col("message_lat")))
                * F.cos(F.toRadians(F.col("city_lat")))
                * F.cos(
                    F.toRadians(F.col("message_lon")) - F.toRadians(F.col("city_lon"))
                )
            )
            * F.lit(6371.0),
        )
        .withColumn("rn", F.row_number().over(window))
        .where("rn=1")
    )
    return message_city


def calculate_user_localtime(message_city_df):
    local_time = (
        message_city_df.select("user_id", "datetime", "city_tz")
        .withColumn(
            "local_time", F.from_utc_timestamp(F.col("datetime"), F.col("city_tz"))
        )
        .groupBy("user_id")
        .agg(F.max("local_time").alias("local_time"))
    )
    return local_time


def calculate_users_datamart(message_city_df):
    w_act_city = Window().partitionBy(["user_id"]).orderBy(F.desc("date"))
    act_city = (
        message_city_df.withColumn("rn_act_city", F.row_number().over(w_act_city))
        .where("rn_act_city=1")
        .select("user_id", F.col("city").alias("act_city"))
        .distinct()
    )

    residence_days = 27
    w_home_city_cnt = Window().partitionBy(["user_id", "city"])
    w_home_city_rn = Window().partitionBy(["user_id"]).orderBy(F.desc("dates_count"))
    home_city = (
        message_city_df.withColumn(
            "dates_count", F.approx_count_distinct("date").over(w_home_city_cnt)
        )
        .withColumn("rn_home_city", F.row_number().over(w_home_city_rn))
        .where(f"rn_home_city=1 and dates_count>={residence_days}")
        .select("user_id", F.col("city").alias("home_city"))
        .distinct()
    )

    travel_cities = (
        message_city_df.select("user_id", "city", "date")
        .orderBy("user_id", "date")
        .groupby("user_id")
        .agg(F.collect_set("city").alias("travel_array"))
        .withColumn("travel_count", F.size(F.col("travel_array")))
    )

    local_time = calculate_user_localtime(message_city_df)

    users_datamart = (
        act_city.join(home_city, "user_id")
        .join(travel_cities, "user_id")
        .join(local_time, "user_id")
    )

    return users_datamart


def calculate_zones_datamart(message_city_df):
    month_w = Window().partitionBy(["month", "city"])
    zones_datamart = (
        message_city_df.select("message_id", "user_id", "date", "event_type", "city")
        .withColumn("month", F.trunc(F.col("date"), "month"))
        .withColumn("week", F.trunc(F.col("date"), "week"))
        .withColumn(
            "month_message",
            F.sum(F.when(message_city_df.event_type == "message", 1).otherwise(0)).over(
                month_w
            ),
        )
        .withColumn(
            "month_reaction",
            F.sum(
                F.when(message_city_df.event_type == "reaction", 1).otherwise(0)
            ).over(month_w),
        )
        .withColumn(
            "month_subscription",
            F.sum(
                F.when(message_city_df.event_type == "subscription", 1).otherwise(0)
            ).over(month_w),
        )
        .withColumn("month_user", F.approx_count_distinct("user_id").over(month_w))
        .withColumn(
            "message", F.when(message_city_df.event_type == "message", 1).otherwise(0)
        )
        .withColumn(
            "reaction", F.when(message_city_df.event_type == "reaction", 1).otherwise(0)
        )
        .withColumn(
            "subscription",
            F.when(message_city_df.event_type == "subscription", 1).otherwise(0),
        )
        .groupBy(
            "month",
            "week",
            F.col("city").alias("zone_id"),
            "month_message",
            "month_reaction",
            "month_subscription",
            "month_user",
        )
        .agg(
            F.sum("message").alias("week_message"),
            F.sum("reaction").alias("week_reaction"),
            F.countDistinct("user_id").alias("week_user"),
        )
    )

    return zones_datamart


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
            F.sin(F.toRadians(F.col("message_lat")))
            * F.sin(F.toRadians(F.col("lat_right")))
            + F.cos(F.toRadians(F.col("message_lat")))
            * F.cos(F.toRadians(F.col("lat_right")))
            * F.cos(F.toRadians(F.col("message_lon")) - F.toRadians(F.col("lon_right")))
        )
        * F.lit(6371.0),
    ).where("distance<=1 and user_id<user_right")

    return users_joined_distance


def calculate_recommendations_datamart(message_city_df, users_distance_df):
    local_time = calculate_user_localtime(message_city_df)

    recommendations_datamart = users_distance_df.select(
        F.col("user_id").alias("user_left"),
        "user_right",
        F.col("date").alias("processed_dttm"),
        F.col("city").alias("zone_id"),
    ).join(local_time, F.col("user_id") == F.col("user_left"))

    return recommendations_datamart


if __name__ == "__main__":
    main()
