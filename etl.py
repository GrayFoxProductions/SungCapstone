import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.functions import col, hour, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import monotonically_increasing_id
import tools
import etl_functions

# set up AWS environment
config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


# Edited spark session by adding in repositories and repos line (1st line after builder) Leesung
def create_spark_session():

    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data, file_name, temperature_file, mapping_file):

    # get the file path to the immigration data
    immigration_file = input_data + file_name

    # read immigration data file
    immigration_df = spark.read.load(immigration_file)

    # clean immigration spark dataframe
    immigration_df = tools.clean_spark_immigration_data(immigration_df)

    # visa_type dimension table create
    visatype_df = etl_functions.create_visa_type_dimension_table(immigration_df, output_data)

    # time dimension table create
    time_df = etl_functions.create_immigration_time_dimension_table(immigration_df, output_data)

    # get global temperatures data (Function defined below)
    temp_df = process_global_land_temperatures(spark, input_data, temperature_file)

    # country dimension table create using immigration + temp data
    dim_df = etl_functions.create_country_dimension_table(spark, immigration_df, temp_df, output_data, mapping_file)

    # create immigration fact table
    fact_df = etl_functions.create_immigration_fact_table(spark, immigration_df, output_data)


def process_demographics_data(spark, input_data, output_data, file_name):

    # load demographics data
    file = input_data + file_name
    demographics_df = spark.read.csv(file, inferSchema=True, header=True, sep=';')

    # clean demographics data
    new_demographics_df = tools.clean_spark_demographics_data(demographics_df)

    # create demographic dimension table
    df = etl_functions.create_demographics_dimension_table(new_demographics_df, output_data)


def process_global_land_temperatures(spark, input_data, file_name):

    # load data
    file = input_data + file_name
    temperature_df = spark.read.csv(file, header=True, inferSchema=True)

    # clean the temperature data
    new_temperature_df = tools.clean_spark_temperature_data(temperature_df)

    return new_temperature_df


def main():

    spark = create_spark_session()
    input_data = "s3://sungs3bucket/"
    output_data = "s3://sungs3bucket/"

    immigration_file_name = 'i94_apr16_sub.sas7bdat'
    temperature_file_name = 'GlobalLandTemperaturesByCity.csv'
    demographics_file_name = 'us-cities-demographics.csv'

    # Loads the i94res.csv(country codes) to the country mapping data
    mapping_file = input_data + "i94res.csv"
    mapping_file = spark.read.csv(mapping_file, header=True, inferSchema=True)

    process_immigration_data(spark, input_data, output_data, immigration_file_name, temperature_file_name, mapping_file)

    process_demographics_data(spark, input_data, output_data, demographics_file_name)


if __name__ == "__main__":
    main()
    

# List used in 3rd data quality test from etl_functions
tables_to_check = { 'immigration_fact_df' : ['record_id'], "visatype_df":['visa_type_key'], 'time_df':['arrdate'],\
                    'demographics_df': ['City','state_code'],\
                    'country_df':['country_code']}     
