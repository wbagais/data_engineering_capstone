from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
import pyspark.sql.functions as F
import datetime
import os
import configparser
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import isnan, when, count
from pyspark.sql.functions import to_date


config = configparser.ConfigParser()
config.read('helpers/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create spark session
    ----------
    return: spark session
    """
    spark = SparkSession.builder.config("spark.jars.packages",
                                        "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")\
    .enableHiveSupport().getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data):
    """
    read immigration data and create immigration, and date tables.
    Then partition and parquet the data into S3 bucket
    ----------
    ARGS
    spark: session 
           created in create_spark_session function
    input_data: path
           The path for immigration data.
    output_data: path
            The path for the parquet files.
    """

    # get filepath to song data file
    #immigration_data = input_data + "i94_apr16_sub.sas7bdat"
    immigration_data = "../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat"
    # read thr data file
    immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data)
    
    # select columns
    columns = ['cicid','i94yr','i94mon', 'i94cit', 'i94res', 'arrdate', 'i94addr', 
               'depdate', 'dtadfile', 'biryear', 'gender',
               'airline', 'fltno', 'visatype']

    immigration_df = immigration_df[columns]

    # drop rows where all elements are missing
    immigration_df = immigration_df.dropna(how='all')

    #create function for. convert columns
    spark.udf.register("date", lambda x: str(pd.to_timedelta(x , unit='D') + pd.Timestamp('1960-1-1')))
    
    #create a view
    immigration_df.createOrReplaceTempView("immigration_df")
    
    #final table
    immigration = spark.sql('''
                  SELECT DISTINCT 
                  int(cicid),
                  i94yr,
                  i94mon, 
                  int(i94cit), 
                  int(i94res), 
                  date(arrdate) AS arrdate,
                  i94addr,
                  depdate,
                  dtadfile, 
                  biryear, 
                  gender,
                  airline, 
                  fltno, 
                  visatype 
                  FROM immigration_df
                  ORDER BY i94yr, i94mon
          '''
          )
    
    #write and partition the data
    immigration_output = output_data + 'immigration/'
    immigration.write.partitionBy("i94yr", "i94mon").parquet(immigration_output, mode = "overwrite")
    print("successfully write immigration table")
    
    #create the date table
    date = spark.sql('''
                  SELECT DISTINCT 
                  date,
                  dayofmonth(date) AS day,
                  weekofyear(date) AS week,
                  month(date) AS month,
                  year(date) AS year,
                  dayofweek(date) AS weekday
                  FROM (SELECT date(arrdate) AS date FROM immigration_df)
          '''
          )
    
    
    #write the data
    date_output = output_data + 'date/'
    #date.coalesce(1).write.format('com.databricks.spark.csv').save(date_output,header = 'true', mode = "overwrite")
    date.write.parquet(date_output, mode = "overwrite")
    print("successfully write date table")



def process_temperature_data(spark, input_data, output_data):
    """
    read log and song data.
    create songs and artists tables.
    Then partition and parquet the data into S3 bucket
    ----------
    ARGS
    spark: session 
           created in create_spark_session function
    input_data: path
           The path for udacity-dend s3 bucket which contains the song data.
    output_data: path
            The path for the parquet files.
    """
    
    #read temperature file
    temperature_file = input_data + "GlobalLandTemperaturesByCity.csv"
    temperature_df = spark.read.format("csv").options(header='true').load(temperature_file)

    temperature_df = temperature_df.withColumn('dt',to_timestamp(temperature_df.dt))
    
    temperature_df.createOrReplaceTempView("temperature_df")
    
    temperature = spark.sql("""
            SELECT DISTINCT 
            Country,
            month(dt) AS Month,
            AVG(AverageTemperature) as AverageTemperature, 
            FIRST(Latitude) AS Latitude, 
            fIRST(Longitude) AS Longitude
            FROM temperature_df
            GROUP BY Month, Country
            ORDER BY Country, Month
        """)
    
    output_file = output_data + 'temperature/'
    #temperature.coalesce(1).write.format('com.databricks.spark.csv').save(output_file,header = 'true', mode = "overwrite")
    temperature.write.parquet(output_file, mode = "overwrite")
    print("successfully write temperature table")

def process_country_data(spark, input_data, output_data):
    """
    read log and song data.
    create songs and artists tables.
    Then partition and parquet the data into S3 bucket
    ----------
    ARGS
    spark: session 
           created in create_spark_session function
    input_data: path
           The path for udacity-dend s3 bucket which contains the song data.
    output_data: path
            The path for the parquet files.
    """
    #read country file
    country_file = input_data + "countries.csv"
    country = spark.read.format("csv").options(header='true').load(country_file)
    
    output_file = output_data + 'country/'
    #country.coalesce(1).write.format('com.databricks.spark.csv').save(output_file,header = 'true', mode = "overwrite")
    country.write.parquet(output_file, mode = "overwrite")
    print("successfully write country table")
    
    
def process_state_data(spark, input_data, output_data):
    """
    read log and song data.
    create songs and artists tables.
    Then partition and parquet the data into S3 bucket
    ----------
    ARGS
    spark: session 
           created in create_spark_session function
    input_data: path
           The path for udacity-dend s3 bucket which contains the song data.
    output_data: path
            The path for the parquet files.
    """
    
    #read country file
    
    state_file = input_data + 'states.csv'
    state = spark.read.format("csv").options(header='true').load(state_file)
    
    output_file = output_data + 'state/'
    #state.coalesce(1).write.format('com.databricks.spark.csv').save(output_file,header = 'true', mode = "overwrite")
    state.write.parquet(output_file, mode = "overwrite")
    print("successfully write state table")
    


def main():
    """
    Call create_spark_session, process_immigration_data, and process_temperature_data functions
    """
    spark = create_spark_session()
    
    #output_data = "data/output/"
    output_data="s3a://wejdan-dend/output/"
    input_data = "s3a://wejdan-dend/input/"  
    process_immigration_data(spark, input_data, output_data)
    process_temperature_data(spark, input_data, output_data) 
    process_country_data(spark, input_data, output_data)
    process_state_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
