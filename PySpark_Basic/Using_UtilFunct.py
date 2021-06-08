import sys

from pyspark.sql import  *
from pyspark.sql import DataFrame
from pyspark import SparkConf

from lib.logger import Log4J
from lib.Utils import get_spark_app_config
from lib.Utils import load_attach_data
from lib.Utils import country_check

if __name__ == "__main__":

    conf = get_spark_app_config() #can be used to avoid hard coding
    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("info")
    logger = Log4J(spark)

    if len(sys.argv) !=2:
        logger.error("File: Attachment File is missing")
        sys.exit(-1)

    logger.info("Starting Program")

    read_df = load_attach_data(spark, sys.argv[1])

    fil_df = country_check(spark, read_df)

    logger.info(fil_df.collect())

    logger.info("Ending Program")

   # spark.stop()