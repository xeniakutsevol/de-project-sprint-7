import datetime
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
    users_dtmrt_base_path = sys.argv[5]

    conf = SparkConf().setAppName(f"UsersDatamart-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    end_date = F.to_date(F.lit(date), "yyyy-MM-dd")

    events_users_datamart = (
        sql.read.parquet(events_base_path)
        .filter(F.col("date").between(F.date_sub(end_date, days_count), end_date))
        .where("event.message_from is not null and event_type = 'message'")
    )

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


def calculate_users_datamart(message_city_df):
    w_act_city = Window().partitionBy(["user_id"]).orderBy(F.desc("date"))
    act_city = (
        message_city_df.withColumn("rn_act_city", F.row_number().over(w_act_city))
        .where("rn_act_city=1")
        .select("user_id", F.col("city").alias("act_city"))
    )

    w_visits = Window().partitionBy(["user_id"]).orderBy("date")
    visits = (
        message_city_df.select("user_id", "city", "date")
        .withColumn("lag_city", F.lag("city", 1).over(w_visits))
        .where("lag_city!=city or lag_city is null")
        .withColumn("lead_date", F.lead("date", 1).over(w_visits))
        .withColumn(
            "lead_date_filled",
            F.when(F.col("lead_date").isNull(), datetime.date.today()).otherwise(
                F.col("lead_date")
            ),
        )
        .withColumn(
            "days_in_city", F.datediff(F.col("lead_date_filled"), F.col("date"))
        )
    )

    residence_days = 27
    w_home_city = (
        Window()
        .partitionBy(["user_id"])
        .orderBy(F.desc("days_in_city"), F.desc("date"))
    )
    home_city = (
        visits.where(f"days_in_city>={residence_days}")
        .withColumn("rn_home_city", F.row_number().over(w_home_city))
        .where(f"rn_home_city=1")
        .select("user_id", F.col("city").alias("home_city"))
    )

    travel_cities = (
        visits.groupby("user_id")
        .agg(F.collect_list("city").alias("travel_array"))
        .withColumn("travel_count", F.size(F.col("travel_array")))
    )

    local_time = calculate_user_localtime(message_city_df)

    users_datamart = (
        act_city.join(home_city, on="user_id", how="left")
        .join(travel_cities, on="user_id", how="left")
        .join(local_time, on="user_id", how="left")
    )

    return users_datamart


if __name__ == "__main__":
    main()
