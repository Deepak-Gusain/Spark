#https://medium.com/@nitingupta.bciit/apache-spark-streaming-from-text-file-fa40155b7a21
#https://hackersandslackers.com/structured-streaming-in-pyspark/

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
spark = SparkSession.builder.appName("Streaming").config('').getOrCreate()


schema_1 = StructType([StructField('Athlete', StringType(), True),
                       StructField('Age', IntegerType(), True),
                       StructField('Country', StringType(), True),
                       StructField('Year', IntegerType(), True),
                       StructField('Closing Ceremony Date', TimestampType() , True),
                       StructField('Sport', StringType(), True),
                       StructField('Medal and Number', StringType(), True),
                       ])


activityDataSample = 'folder_path'

#dont use max file = 1 in production
streamingDF = (
  spark
    .readStream
    .schema(schema_1)
    .option("maxFilesPerTrigger", 1)
    .csv(activityDataSample)
)


streamingDF = streamingDF.filter((streamingDF.Country != 'null') | (streamingDF.Country != 'Null') \
                                 | (streamingDF.Country != ''))
streamingActionCountsDF = streamingDF.groupBy('Country').count()

print(streamingActionCountsDF.isStreaming)

#As we are testing it in standalone 
spark.conf.set("spark.sql.shuffle.partitions", "5")

# View stream in real-time
query = (
  streamingActionCountsDF
    .writeStream
    .format("memory")
    .queryName("counts")
    .outputMode("complete")
    .start()
)

#below command will wait till query status is active
#query.awaitTermination()

print(spark.streams.active)

#just to verify our output we are print data 
from time import sleep
for x in range(3):
    spark.sql("select * from counts").show()
    sleep(1)



