from pyspark import SparkContext
from pyspark.sql import functions
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import collect_list, lit, udf, when, rand, struct, col
from pyspark.sql.types import StringType, ArrayType, DoubleType, StructType, StructField, TimestampType

from datetime import datetime, timedelta
import json

sc = SparkContext()
sqlContext = SQLContext(sc)

output_path = "/tmp/result.avro"

# 讀取HDFS上的avro檔，也可以讀取本地的文件(--files指令)
df = sqlContext.read.format("com.databricks.spark.avro").load("/tmp/hotelList.avro")

df = df.filter(df["price"]<3000)

if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(output_path)):
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(output_path), True)

df.repartition(1).write.format('com.databricks.spark.avro').save(output_path)

