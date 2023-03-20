import os


from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"


default_args = {"start_date": datetime(2023, 3, 20), "owner": "airflow"}

dag = DAG("datamarts_dag", schedule_interval=None, default_args=default_args)


t1 = SparkSubmitOperator(
    task_id="calculate_datamarts",
    dag=dag,
    application="/lessons/datamarts_dag.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-06-15",
        "30",
        "/user/xeniakutse/data/events"
        "/user/xeniakutse/data/geo_tz.csv"
        "/user/xeniakutse/data/analytics/users_datamart"
        "/user/xeniakutse/data/analytics/zones_datamart"
        "/user/xeniakutse/data/analytics/recommendations_datamart",
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    executor_cores=4,
    executor_memory="4g",
)


t1
