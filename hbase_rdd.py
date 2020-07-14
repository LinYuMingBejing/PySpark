from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType,StructField, ArrayType
from pyspark.sql import SQLContext

"""該方法效率低落，不推薦使用該模式"""

sc = SparkContext()
conf = {"hbase.zookeeper.property.clientPort":"2181",
        "zookeeper.znode.parent":"/hbase-unsecure",
        "hbase.zookeeper.quorum": "localhost",
        "hbase.mapreduce.scan.columns": "booking:url(decode) booking:session_id", 
        "hbase.mapreduce.scan.row.start":"e_2020-04-19",
        "hbase.mapreduce.scan.row.stop": "e_2020-04-20",
        "hbase.mapreduce.inputtable": "booking",
    }


keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat","org.apache.hadoop.hbase.io.ImmutableBytesWritable","org.apache.hadoop.hbase.client.Result",keyConverter=keyConv,valueConverter=valueConv,conf=conf)

count = hbase_rdd.count()
hbase_rdd.cache()
output = hbase_rdd.collect()
for (k, v) in output:
    print(k, v)
    break