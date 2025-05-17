import pandas as pd
import seaborn as sns
import os
import configparser
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
import requests
requests.packages.urllib3.disable_warnings()

    

def clean_spark_immigration_data(df):
    """Clean immigration dataframe
    param
        df: spark dataframe with immigration data
    """
    total_records = df.count()
    
    print(f'Total records in Immigration source data: {total_records:,}')
    
    # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them
    drop_columns = ['occup', 'entdepu','insnum']
    df = df.drop(*drop_columns)
    
    # Drop duplicates using primary key as identifier
    df= df.dropDuplicates(['cicid'])
    
    new_total_records = df.count()
    
    print(f'Total records in immigration source data after cleaning: {new_total_records:,}')
    
    # DOUBLE to INTEGER datatype conversion
    toInt = udf(lambda x: int(x) if x!=None else x, IntegerType())
    df = df.select( [ toInt(colname).alias(colname) if coltype == 'double' else colname for colname, coltype in df.dtypes])

    # arrdate & depdate conversion to datetime    
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    df = df.withColumn("arrdate", get_datetime(df.arrdate))
    df = df.withColumn("depdate", get_datetime(df.depdate))
    
    return df



def clean_spark_temperature_data(df):
    """Clean global temperatures dataset
    
    param 
        df: dataframe representing global temperatures
    """
    total_records = df.count()
    
    print(f'Total records in Temperature source data: {total_records:,}')
    
    # drop rows with missing average temperature
    df = df.dropna(subset=['AverageTemperature'])
    
    total_recs_after_dropping_nullav = df.count()
    print('Rows of Null Average Temp values dropped: {:,}'.format(total_records-total_recs_after_dropping_nullav))
    
    # Drop duplicate rows
    df = df.drop_duplicates(subset=['dt', 'City', 'Country'])
    
    
    new_total_records = df.count()
    
    print('Rows of duplicates dropped: {:,}'.format(total_recs_after_dropping_nullav-df.count()))
    
    print(f'Total records in temperature source data after cleaning: {new_total_records:,}')
    
    # Convert dt column datatype: timestamp to string 
    df = df.withColumn("dt",col("dt").cast(StringType()))
    
    
    return df



def aggregate_temperature_data(df):
    """Aggregate clean temperature data at country level
    
    param 
        df: spark dataframe of clean global temperaturs data
    """
    new_df = df.select(['Country', 'AverageTemperature']).groupby('Country').avg()
    
    new_df = new_df.withColumnRenamed('avg(AverageTemperature)', 'average_temperature')
    
    return new_df




def clean_spark_demographics_data(df):
    """Clean the US demographics dataset
    
    param 
        df: spark dataframe of US demographics dataset
    """
    
    total_records= df.count()
    print(f'Total records in Demographics source data: {total_records:,}')
    
    
    # drop rows with missing values
    subset_cols = [
        'Male Population',
        'Female Population',
        'Number of Veterans',
        'Foreign-born',
        'Average Household Size'
    ]
    new_df = df.dropna(subset=subset_cols)
    
    rows_dropped = df.count()-new_df.count()
    print("Rows dropped with Null values: {}".format(rows_dropped))
    
    # drop duplicate columns
    new_df2 = new_df.dropDuplicates(subset=['City', 'State', 'State Code', 'Race'])
    
    rows_dropped_with_duplicates = new_df.count()-new_df2.count()
    print(f'Rows dropped with duplicates: {rows_dropped_with_duplicates}')
    
    print(f'Total records in Demographics source data after cleaning: {new_df2.count():,}')
    
    
    return new_df2



def create_time_dim_table(df):
    return df.count()

def print_formatted_float(number):
    print('{:,}'.format(number))