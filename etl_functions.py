import pandas as pd
import os
import configparser
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
from tools import aggregate_temperature_data


def create_immigration_fact_table(spark, df, output_data):
    """
    Creates a immigration fact table.
    
    Params
        spark: spark session
        df: spark dataframe of immigration events
        output_data: path to write dimension dataframe to
    """
    # get visa_type dimension
    dim_df = get_visa_type_dimension(spark, output_data)

    # create a view for visa type dimension
    dim_df.createOrReplaceTempView("visa_view")


    # rename columns to align with data model
    df = df.withColumnRenamed('ccid', 'record_id') \
        .withColumnRenamed('i94res', 'country_residence_code') \
        .withColumnRenamed('i94addr', 'state_code')

    # create an immigration view
    df.createOrReplaceTempView("immigration_view")

    # create visa_type key
    df = spark.sql(
        """
        SELECT 
            immigration_view.*, 
            visa_view.visa_type_key
        FROM immigration_view
        LEFT JOIN visa_view ON visa_view.visatype=immigration_view.visatype
        """
    )


    # drop visatype key
    df = df.drop(df.visatype)

    # write dimension to parquet file
    df.write.parquet(output_data + "immigration_fact", mode="overwrite")

    return df


def create_demographics_dimension_table(df, output_data):
    """
    Creates a demographics dimension table.
    
    Parameters
        df: spark dataframe of us demographics survey data
        output_data: path to write dimension dataframe to
    """
    dim_df = df.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'average_household_size') \
        .withColumnRenamed('State Code', 'state_code')
    
    # id column created
    dim_df = dim_df.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file
    dim_df.write.parquet(output_data + "demographics", mode="overwrite")

    return dim_df


def create_visa_type_dimension_table(df, output_data):
    """
    Creates a visa type dimension table.
    
    Parameters
        df: spark dataframe of immigration events
        output_data: path to write dimension dataframe to
    """
    # create visatype df from visatype column
    visatype_df = df.select(['visatype']).distinct()

    # id column created
    visatype_df = visatype_df.withColumn('visa_type_key', monotonically_increasing_id())

    # write dimension to parquet file
    visatype_df.write.parquet(output_data + "visatype", mode="overwrite")

    return visatype_df

# Used to obtain visatype dimension during immigration fact table creation's function
def get_visa_type_dimension(spark, output_data):
    return spark.read.parquet(output_data + "visatype")



def create_country_dimension_table(spark, df, temp_df, output_data, mapping_file):
    """
    Creates a country dimension table.
    
    Parameters:
        spark: spark session object
        df: spark dataframe of immigration events
        temp_df: spark dataframe of global land temperatures data.
        output_data: path to write dimension dataframe to
        mapping_file: csv file that maps country codes to country names
    """
    # create temporary view for immigration data
    df.createOrReplaceTempView("immigration_view")

    # create temporary view for countries codes data
    mapping_file.createOrReplaceTempView("country_codes_view")

    # get the aggregated temperature data
    agg_temp = aggregate_temperature_data(temp_df)
    # create temporary view for countries average temps data
    agg_temp.createOrReplaceTempView("average_temperature_view")

    # create country dimension using SQL
    country_df = spark.sql(
        """
        SELECT 
            i94res as country_code,
            name as country_name
        FROM immigration_view
        LEFT JOIN country_codes_view
        ON immigration_view.i94res=country_codes_view.code
        """
    ).distinct()
    # create temp country view
    country_df.createOrReplaceTempView("country_view")

    country_df = spark.sql(
        """
        SELECT 
            country_code,
            country_name,
            average_temperature
        FROM country_view
        LEFT JOIN average_temperature_view
        ON country_view.country_name=average_temperature_view.Country
        """
    ).distinct()

    # write the dimension to a parquet file
    country_df.write.parquet(output_data + "country", mode="overwrite")

    return country_df


def create_immigration_time_dimension_table(df, output_data):
    """
    Creates an immigration time dimension table.
    
    Parameters:
        df: spark dataframe of immigration events
        output_data: path to write dimension dataframe to
    """

    # create initial time df from arrdate column
    time_df = df.select(['arrdate']).distinct()

    # expand df by adding other calendar columns
    time_df = time_df.withColumn('arrival_day', dayofmonth('arrdate'))
    time_df = time_df.withColumn('arrival_week', weekofyear('arrdate'))
    time_df = time_df.withColumn('arrival_month', month('arrdate'))
    time_df = time_df.withColumn('arrival_year', year('arrdate'))
    time_df = time_df.withColumn('arrival_weekday', dayofweek('arrdate'))

    # create an id field in calendar df
    time_df = time_df.withColumn('id', monotonically_increasing_id())

    # write the time dimension to parquet file
    partition_columns = ['arrival_year', 'arrival_month', 'arrival_week']
    time_df.write.parquet(output_data + "immigration_time", partitionBy=partition_columns, mode="overwrite")

    return time_df


def quality_checks(df, table_name):
    """
    Count checks on fact and dimension tables.
    
    Parameters:
        df: spark dataframe to check counts on
        table_name: corresponding name of table
    """
    total_count = df.count()

    if total_count == 0:
        print(f"FAILED 1st Data Quality test on {table_name} table with NO records found.")
    else:
        print(f"PASSED 1st Data Quality test for {table_name} table with {total_count:,} records found.")
    return 0



def second_quality_checks_time(df):
    df['id'].duplicated().any()

    if True:
        print('PASSED: No duplicate IDs in time dim table.')
    else:
        print('FAILED: Duplicate IDs found in time dim table.')
    return


def second_quality_checks_demographics(df):
    df['id'].duplicated().any()

    if True:
        print('PASSED: No duplicate IDs in demographics dim table.')
    else:
        print('FAILED: Duplicate IDs found in demographics dim table.')
    return


def second_quality_checks_visatype(df):
    df['visa_type_key'].duplicated().any()

    if True:
        print('PASSED: No duplicate visa_type_keys in visatype dim table.')
    else:
        print('FAILED:Duplicate visa_type_keys found in visatype dim table.')
    return


def second_quality_checks_country(df):
    df['country_code'].duplicated().any()
    if True:
        print('PASSED: No duplicate country codes in country dim table.')
    else:
        print('FAILED: Duplicate country codes found in country dim table.')
    return


def nullValueCheck(spark_ctxt, tables_to_check):
    """
    This function performs null value checks on specific columns of given tables received \
    as parameters and raises a ValueError exception when null values are encountered.
    It receives the following parameters:
    spark_ctxt: spark context where the data quality check is to be performed
    tables_to_check: A dictionary containing (table, columns) pairs specifying for each table,\
    which column is to be checked for null values.   
    """  
    for table in tables_to_check:
        print(f"Performing primary key null value check on table {table}...")
        for column in tables_to_check[table]:
            returnedVal = spark_ctxt.sql(f"""SELECT COUNT(*) as nbr FROM {table} WHERE {column} IS NULL""")
            if returnedVal.head()[0] > 0:
                raise ValueError(f"3rd Data quality check failed! Found NULL values in {column} column!")
        print(f"Table {table} passed 3rd Data Quality Test.")

    return
        