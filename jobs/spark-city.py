from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DoubleType, IntegerType
from config import configuration
from pyspark.sql.functions import col, from_json

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.828,")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", 'AWS_ACCESS_KEY')\
        .config("spark.hadoop.fs.s3a.secret.key", 'AWS_SECRET_KEY')\
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .getOrCreate()

    # Adjust the log level to minimize the console output
    spark.sparkContext.setLogLevel('WARN')

    # Define schemas
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fueltype", StringType(), True),
    ])

    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    traffic_camera_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)  # Assuming snapshot is a Base64 encoded string
    ])

    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])

    emergency_incident_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("incidentid", StringType(), True),
        StructField("type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic: str, schema: StructType) -> DataFrame:
        return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes'))

    def stream_writer(input: DataFrame, checkpoint_folder: str, output: str):
        return (input.writeStream
            .format('parquet')
            .option('checkpointLocation', checkpoint_folder)
            .option('path', output)
            .outputMode('append')
            .start())

    vehicleDF = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gps_schema).alias('gps')
    trafficDF = read_kafka_topic('traffic_camera_data', traffic_camera_schema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weather_schema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_incident_data', emergency_incident_schema).alias('emergency')

    # Start streaming queries
    query1 = stream_writer(vehicleDF, 's3a://spark-streaming-data-realtime2/checkpoints/vehicle_data',
                 's3a://spark-streaming-data-realtime2/data/vehicle_data')
    query2 = stream_writer(gpsDF, 's3a://spark-streaming-data-realtime2/checkpoints/gps_data',
                 's3a://spark-streaming-data-realtime2/data/gps_data')
    query3 = stream_writer(trafficDF, 's3a://spark-streaming-data-realtime2/checkpoints/traffic_data',
                 's3a://spark-streaming-data-realtime2/data/traffic_data')
    query4 = stream_writer(weatherDF, 's3a://spark-streaming-data-realtime2/checkpoints/weather_data',
                 's3a://spark-streaming-data-realtime2/data/weather_data')
    query5 = stream_writer(emergencyDF, 's3a://spark-streaming-data-realtime2/checkpoints/emergency_data',
                 's3a://spark-streaming-data-realtime2/data/emergency_data')

    # Await termination of the last query
    query5.awaitTermination()

if __name__ == "__main__":
    main()
