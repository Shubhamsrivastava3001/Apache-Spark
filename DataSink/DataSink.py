from pyspark.sql import *
from lib.logger import *
import avro.schema


if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .master("local[3]")\
            .appName("DataSink")\
            .getOrCreate()

    logger = Log4J(spark)

    flightTimeParquet = spark.read\
                        .format("parquet")\
                        .load("dataSource/flight-time.parquet")

    flightTimeParquet.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()

    flightTimeParquet.write\
        .format("json")\
        .mode("overwrite")\
        .option("path", "jsonDataSink/json/")\
        .partitionBy("OP_CARRIER", "ORIGIN")\
        .option("maxRecordPerFile", 10000)\
        .save()


    logger.info("Num Partion" + str(flightTimeParquet.rdd.getNumPartitions()))