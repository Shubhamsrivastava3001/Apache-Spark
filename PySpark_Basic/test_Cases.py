import sys
from unittest import TestCase
from pyspark.sql import SparkSession
from lib.Utils import load_attach_data, country_check


class test_Case_Util(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

    def data_file_test(self):
        sample_df = load_attach_data(self.spark, sys.argv[1])
        count_df = sample_df.count()
        self.assertEqual(count_df, 9, "Record count for 9")

    def country_count(self):
        sample_df = load_attach_data(self.spark, sys.argv[1])
        count_df = country_check(self.spark, sample_df).collect()
        count_dict = dict()
        for row in count_df:
            count_dict[row["Country"]] = row["count"]
        self.assertEqual(count_dict["United States"], 4, "Count of US is 4")
        self.assertEqual(count_dict["Canada"], 2, "Count of Canada is 2")
        self.assertEqual(count_dict["United Kingdom"], 1, "Count of Uk is 1")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
