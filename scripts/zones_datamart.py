import sys

import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from common_functions import add_coords_timezone


def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    geo_base_path = sys.argv[4]
    zones_dtmrt_base_path = sys.argv[5]

    conf = SparkConf().setAppName(f"ZonesDatamart-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    end_date = F.to_date(F.lit(date), "yyyy-MM-dd")

    events_zones_datamart = (
        sql.read.parquet(events_base_path)
        .filter(F.col("date").between(F.date_sub(end_date, days_count), end_date))
    )

    geo = (
        sql.read.option("delimiter", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(geo_base_path)
    )

    zones_dtmrt = calculate_zones_datamart(
        add_coords_timezone(events_zones_datamart, geo)
    )
    zones_dtmrt.write.parquet(f"{zones_dtmrt_base_path}/date={date}/depth={days_count}")


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
        .withColumn("month_user", F.size(F.collect_set("user_id").over(month_w)))
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


if __name__ == "__main__":
    main()
