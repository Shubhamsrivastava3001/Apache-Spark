import configparser

from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIG"):
        spark_conf.set(key,val)
    return spark_conf

def load_attach_data(spark, data_file):
    read1 = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)
    return read1

def country_check(spark, country_df):
    filter_df = country_df \
        .where("Age < 40") \
        .select("AGE", "Gender", "Country", "State") \
        .groupBy("Country").count()
    return filter_df

def partition_check(partition_df):
    filter_df = partition_df \
        .where("Age < 40") \
        .select("AGE", "Gender", "Country", "State") \
        .groupBy("Country").count()
    return filter_df