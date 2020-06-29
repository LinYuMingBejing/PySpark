from pyspark import SparkContext
from pyspark.sql import functions
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import collect_list, lit, udf, when, rand, struct, col
from pyspark.sql.types import StringType, ArrayType, DoubleType, StructType, StructField, TimestampType

sc = SparkContext()
sqlContext = SQLContext(sc)

options = {
    'url': 'jdbc:mysql://localhost/order?serverTimezone=Asia/Taipei&useServerPrepStmts=false&rewriteBatchedStatements=true',
    'driver': 'com.mysql.cj.jdbc.Driver',
    'user': 'root',
    'password': 'root'
}

# 讀取
tmp_df = sqlContext.read.format('jdbc').options(dbtable='hotel', **options).load()
tmp_df.printSchema()

tmp_df = tmp_df.filter(tmp_df["city"]=="台北市")

# 寫入
tmp_df.write.format('jdbc') \
        .options(dbtable=table, truncate='true', **options) \
        .mode('overwrite') \
        .save()
