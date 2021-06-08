import sys

from pyspark.sql import  *
from pyspark import SparkConf

from lib.logger import Log4J
from lib.Utils import get_spark_app_config
from lib.Utils import load_attach_data

if __name__ == "__main__":

    conf = get_spark_app_config() #can be used to avoid hard coding
    # conf = SparkConf()
    # conf.set("spark.app.name", "Spark_Hello")
    # conf.set("spark.master", "local[3]")
    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()
        # .appName("Spark_Hello") \
        # .master("local[3]") \
    sc = spark.sparkContext
    sc.setLogLevel("info")
    logger = Log4J(spark)

    if len(sys.argv) !=2:
        logger.error("File: Attachment File is missing")
        sys.exit(-1)

    #conf_out = spark.sparkContext.getConf()
   # logger.info(conf_out.toDebugString())
    logger.info("Starting Program")
    # read1 = spark.read.csv(sys.argv[1]) #directly ask to read from Argument

    #Below Code can be used as hard coded
    # read1 = spark.read \
    #     .option("header", "true") \
    #     .option("inferSchema", "true") \
    #     .csv(sys.argv[1])
    read_df = load_attach_data(spark, sys.argv[1])
    read_df.show()



    logger.info("Ending Program")


   # spark.stop()

