from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark.sql import functions as f

spark = SparkSession.builder.appName('myapp').getOrCreate()

sdf = spark.readStream.format('kafka').option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","f1telemetry").option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()

dataSchema = StructType([
    StructField("RPM", IntegerType()),
    StructField("Speed", FloatType()),
    StructField("nGear", ShortType()),
    StructField("Throttle", FloatType()),
    StructField("Brake",BooleanType()),
    StructField("DRS",ShortType()),
    StructField("Source",StringType()),
    StructField("Time", DoubleType()),
    StructField("X", FloatType()),
    StructField("Y", FloatType()),
    StructField("Lap", ShortType()),
    StructField("Driver", StringType()),
    StructField("Team", StringType())])

df = sdf.selectExpr('CAST(value AS STRING)').select(f.from_json(f.col('value'), dataSchema).alias('value')).select(f.col('value.*'))  

df = df.toDF('rpm', 'speed', 'ngear', 'throttle','brake','drs','source','time','x','y','lap','driver','team')
#df.writeStream.format("console").start()



df.writeStream.trigger(processingTime="0 seconds").format("org.apache.spark.sql.cassandra").option("checkpointLocation", 'chkpnt').options(table="f1telemetry", keyspace="formula1").outputMode("append").start().awaitTermination()