from distutils.command.install import value

from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from jobs.config import configuration
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType


#1 usage
def main():
    spark = SparkSession.builder .appName("SmartCityStreaming")\
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFilesystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .getOrCreate()

    #Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    #vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])

    #gpsSchema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])

    #trafficSchema
    trafficeSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])

    #weatherSchema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    #emergencySchema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("Description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option(key= 'kafka.bootstrap.servers', value= 'broker:29092')
                .option(key= 'subscribe', topic)
                .option(key='strtingOffsets', value='earliest')
                .load()
                .selectExpr('CAST(values AS STRING)')
                .select(from_json(col('values'), schema).alias('data'))
                .select('data.*')
                .withWatermark(eventTime='timestamp', delayThreshold='2 minutes')
                )

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('trafic_data', trafficeSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    #join all the dfs with id and timestamp
    query1 = streamWriter(vehicleDF, checkpointFolder= 's3a://spark-streaming-data/checkpoints/vehicle_data',
                 output= 's3a://spark-streaming-data/data/vehicle_data')
    query2 = streamWriter(gpsDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/pgs_data',
                 output='s3a://spark-streaming-data/data/pgs_data')
    query3 = streamWriter(trafficDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/traffic_data',
                 output='s3a://spark-streaming-data/data/traffic_data')
    query4 = streamWriter(weatherDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/weather_data',
                 output='s3a://spark-streaming-data/data/weather_data')
    query5 = streamWriter(emergencyDF, checkpointFolder='s3a://spark-streaming-data/checkpoints/emergency_data',
                 output='s3a://spark-streaming-data/data/emergency_data')

    query5.awaitTermination()

if __name__ == "__main__":
    main()

