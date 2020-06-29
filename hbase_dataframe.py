from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import lit, udf, when, col, concat_ws
from pyspark.sql.types import StringType, Row, ArrayType, DoubleType, StructType, StructField, TimestampType


sc = SparkContext()
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")

def load_hotel_data():
    footprint_report_catalog = ''.join("""{
                    "table":{"namespace":"default", "name":"booking"},
                    "rowkey":"key1",
                    "columns":{
                        "key":{"cf":"rowkey", "col":"key1", "type":"string", "length":"12"},
                        "name":{"cf":"booking","col":"name","type":"string"},
                        "income":{"cf":"booking","col":"income","type":"string"},
                        "creation_time":{"cf":"booking","col":"creation_time","type":"string"}
                    }
                }""".split())

    tmp_df = sqlContext.read.format("org.apache.spark.sql.execution.datasources.hbase") \
            .options(catalog=footprint_report_catalog) \
            .load()

    tmp_df.createOrReplaceTempView("hotel")

    # 查詢2020/05/20訂單記錄
    hotel_df = sqlContext.sql("select * from hotel where key > 'a_2020-05-20' and key < 'a_2020-05-21' ")

    hotel_df = hotel_df.withColumn("income",col("income").case("long"))

    hotel_df = hotel_df.groupBy('name').agg({'income': 'sum'})

    return hotel_df


if __name__=="__main__":
    hotel_df = load_hotel_data()
    hotel_df.repartition(1).write.format('com.databricks.spark.avro').save("/tmp/yuming/202004.avro")
