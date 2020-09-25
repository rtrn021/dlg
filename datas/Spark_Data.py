from pyspark.sql.types import *
from pyspark.sql import SparkSession

class Spark_Data:

    df = SparkSession.builder.getOrCreate().createDataFrame([],StructType([]))
    list_data = []

    @classmethod
    def get_df(cls):
        return cls.df

    @classmethod
    def get_list_data(cls):
        return cls.list_data

    @classmethod
    def set_df(cls,df):
        cls.df = df

    @classmethod
    def set_list_data(cls,list_data):
        cls.list_data = list_data

    @classmethod
    def extend_list_data(cls,data):
        cls.list_data.extend(data)