""" 基本PySpark語法 : 透過PySpark Shell"""
from pyspark.sql.types improt *
sc = SparkContext() # PySpark入口
sqlContext = SQLContext(sc)
df = sqlContext.read.format("json").load("./spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/employees.json")

"""查看Schema"""
df.printSchema() 

"""建構欄位"""
from pyspark.sql.functions import col, column, expr
col("col1")
column("col2")

"""查看欄位"""
df.columns

"""獲取第一條row"""
df.first()

"""查看數據類型"""
df.select("salary").dtypes


"""
    建立DataFrame
        方法: createOrReplaceTempView
        說明：存在就替換，不存在就創建
"""

df.createOrReplaceTempView("dfTable")

df.select("name").show(2)
df.select("name","salary").show(2)

df.select(expr("name as employee")).show(2)
df.select(expr("name").alias("Employee")).show(2)

df.selectExpr("sum(salary)").show(2)

from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("count")).show(2)

"""新增欄位：withColumn"""
df.withColumn("number1",lit(1)).show(2)

"""重新命名欄位：withColumnRenamed"""
df.withColumnRenamed("name","Employee")

# 區分大小寫 set sparl.sql.caseSensitive true

"""刪除欄位：drop"""
df.drop("name")

"""更改欄位type：case"""
df.withColumn("salary",col("debt").case("long"))

"""過濾Row: filter 和 where"""
df.filter(col("salary")>2000).show()
df.where("salary < 2000").show()
df.where("salary >= 4000").where(col("name") != "Andy").show()

"""獲取唯一的Row"""
df.select("name").distinct().count()

"""隨機抽樣"""
seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

"""排序Row：orderBy sort"""
from pyspark.sql.functions import desc
df.orderBy(desc("salary")).show(5)
df.sort(desc("salary")).show(5)

"""操作Number"""
# 1. 取次方 透過pwd
from pyspark.sql.functions import expr, pow
expected_salary = df.select(expr("name"), pow(col("salary"),2)-10000)
# 2. 四捨五入 round()
df.describe().show()

"""操作String"""
# 1. 將首字母轉為大寫
from pyspark.sql.functions import initcap, lower,upper

df.select(initcap(col("name"))).show()
df.select(lower(col("name"))).show()
df.select(upper(col("name"))).show()

# 2. 刪除空格 : 
from pyspark.sql.functions import lpad, ltrim, rtrim, rpad, trim

# 3. 模糊查詢：
df.filter(col('name').like('%nd%')).show()

# 4. 去重：
df.select('name').dropDuplicates().show()

# 5. 分割字符串 :
df.withColumn("splited_name",split(df_webpages["name"],",")[1]).show()


"""操作Date"""
from pyspark.sql.functions import current_date, current_timestamp

dateDF = spark.range(10).withColumn("today",current_date())\
                        .withColumn("now",current_timestamp())

from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"),5), date_add(col("today"),5))

# 時間差
from pyspark.sql.functions import to_date, datediff, months_between
dateDF.select(to_date(lit("2020-06-01")).alias("start"),\
              to_date(lit("2022-06-01")).alias("end"))\
              .select(months_between(col("start"),col("end"))).show(1)

"""處理遺漏值"""
df.na.drop()
df.na.fill(0)

"""處理多型態"""
# array是否包含某個值 array_contains()

# first() last()
from pyspark.sql.functions import first, last
df.select(first("name"),last("salary"))

# min(), max()
from pyspark.sql.functions import min, max
df.select(min("salary"),max("salary")).show()

from pyspark.sql.functions import sum
df.select(sum("salary")).show()

#sumDistinct()
from pyspark.sql.functions import avg
df.select(avg("salary")).show()


"""自定義函數"""
def toFormat(s):
    return str(s).split(",")[0].replace("[","").replace("'","")

toFormat=udf(toFormat, StringType())
df.withColumn('words',toFormat('keywords')).select("words").show()


"""cache"""
DF1.cache()
DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect()
DF3 = DF1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
DF4 = DF1.groupBy("count").count().collect()