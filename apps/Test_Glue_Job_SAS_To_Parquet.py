
"""
A simple program to read file from S3 and transform it and reload to S3 again.

AWS Glue ETL pipeline

Source: S3 Buckets
Source Format: SAS7BDAT

ETL Process: Glue Job Convert SAS to Parquet

Target: S3 Buckets
Target Format: Parquet


"""
import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
import boto3


def create_glue_context():
    """ Creates Glue Context / Spark session """
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    return spark, job

def list_folders(s3_client, bucket_name: str, prefix: str) -> list:
    """
    Returns a list of folders in the specified S3 bucket
    :param s3_client: boto3 client
    :param bucket_name: S3 bucket from which the directories are required
    :param prefix: Prefix of the input S3 bucket
    :return: List of folders in the input location
    """

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    for content in response.get('CommonPrefixes', []):
        yield content.get('Prefix'.split('/')[0])

def read_sas_table(spark, filepath):
    """
    Reads a SAS table using spark and converts it to Spark Datafrane"""

    df = spark.read.format("com.github.saurfang.sas.spark")\
                    .load(filepath)
    return df


def add_audit_cols(df, changedt):
    """ Adds audit columns to the dataframe
    """
    df = df.withColumn("operation", f.lit("I")) \
           .withColumn("processeddate", f.current_timestamp()) \
           .withColumn("changedate", f.lit(changedt)) \
           .withColumn('changedate_year', f.year('changedate').cast("String")) \
           .withColumn('changedate_month', f.month('changedate').cast("String")) \
           .withColumn('changedate_day', f.dayofmonth('changedate').cast("String"))
    return df

def write_to_parquet(df, write_mode, partition_cols, target_path):
    df.write.mode(write_mode).format("parquet").partitionBy(partition_cols).save(target_path)

def main():
    spark, glue_job = create_glue_context()
    src_bucket = "source-bucket"
    src_basePath = "test-sas/data"
    trgt_bucket = "target-bucket"
    trgt_basePath = "test-output/data"
    partition_cols=['processeddate']
    source_sas_tables_names=['table1','table2','table3']
    s3_client = boto3.client('s3')
    current_ts = datetime.now()
    s3folders = list_folders(s3_client, src_bucket, src_basePath)
    for table in source_sas_tables_names:
            if table in s3folders:
               df = read_sas_table(spark, 's3://'+src_bucket+'/'+table+'/')
               df = add_audit_cols(df, current_ts)
               records_read=df.count()
               if records_read > 0:
                    write_to_parquet(df, "overwrite", partition_cols, 's3://'+trgt_bucket+'/'+trgt_basePath+'/'+table.lower())


    glue_job.commit()


if __name__ == '__main__':
    main()

