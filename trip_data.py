from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd

from pyspark.sql.types import \
    StringType, IntegerType, DoubleType, \
    TimestampType, StructType, StructField


schema = StructType([
    StructField('vendor_id', IntegerType(), True),
    StructField('lpep_pickup_datetime', TimestampType(), True),
    StructField('lpep_dropoff_datetime', TimestampType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('rate_code_id', IntegerType(), True),
    StructField('pickup_longitude', DoubleType(), True),
    StructField('pickup_latitude', DoubleType(), True),
    StructField('dropoff_longitude', DoubleType(), True),
    StructField('dropoff_latitude', DoubleType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", StringType(), True)
])


def get_spark():
    return SparkSession \
        .builder \
        .appName("Trip Data Transformer") \
        .getOrCreate()


def convert_to_parquet(spark=get_spark()):
    #input file has inconsistent newline character leading to header has 20 columns but read as 21 columns
    #Skip the header and name column from 0 to 20, then drop the last column.
    input_df =pd.read_csv(
        "https://nyc-tlc.s3.us-east-1.amazonaws.com/trip%20data/green_tripdata_2013-09.csv",
        header=0, index_col=False, names=range(21), parse_dates=[1,2]
    )

    #Drop last column and put the correct label for columns
    fixed_df = input_df[input_df.columns[:20]]
    fixed_df.columns = schema.names

    #apply schema & write the data out
    spark.createDataFrame(fixed_df, schema=schema)\
        .write.mode("overwrite")\
        .parquet(
            "./output/trip-data-parquet"
        )


def transform_df(spark=get_spark()):

    df = spark.read.parquet("./output/trip-data-parquet")

    for hour in range(24):
        df = df\
            .withColumn(
                f"pickup_hour_{hour}",
                f.when(f.hour("lpep_pickup_datetime") == hour, 1).otherwise(0)
            )

    for dayofweek in range(7):
        df = df\
            .withColumn(
                f"pickup_dow_{dayofweek}",
                f.when(f.dayofweek('lpep_pickup_datetime') == dayofweek, 1).otherwise(0)
            )

    df = df\
        .withColumn(
            "duration_in_second",
            (f.col("lpep_dropoff_datetime").cast("long") - f.col("lpep_pickup_datetime").cast("long"))
        )

    #Coordinate of boundingbox for JFK Airport = (-73.821618,40.622204),(-73.74587,40.666826)
    #Taken from https://boundingbox.klokantech.com/

    df = df.withColumn(
        "pickup_from_jdk_airport",
        f.when(
            f.col("pickup_longitude").between(-73.821618, -73.74587) &
            f.col("pickup_latitude").between(40.622204, 40.666826),
            1
        ).otherwise(0)
    )
    df.write.mode("overwrite").parquet("./output/trip-data-enriched-parquet")


if __name__ == '__main__':
    convert_to_parquet()
    transform_df()
