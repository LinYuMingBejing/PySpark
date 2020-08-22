from pyspark.sql import SparkSession


spark = SparkSession.builder.master('local')\
                .config("spark.mongodb.input.partitioner","MongoShardedPartitioner")\
                .config('spark.mongodb.input.uri', 'mongodb://localhost:27017')\
                .config('spark.mongodb.output.uri', 'mongodb://localhost:27017')\
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')\
                .getOrCreate()

### load data
df = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
            .option("database","restaurant")\
            .option("collection", "price")\
            .load()


### write data
people = spark.createDataFrame([("Bilbo Baggins",  50), 
                                ("Gandalf", 1000), 
                                ("Thorin", 195), 
                                ("Balin", 178), 
                                ("Kili", 77),
                                ("Dwalin", 169), 
                                ("Oin", 167), 
                                ("Gloin", 158), 
                                ("Fili", 82), 
                                ("Bombur", None)],
                            ["name", "age"])

people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()