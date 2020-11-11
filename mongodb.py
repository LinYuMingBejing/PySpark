from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf
from pyspark.sql.types import StringType, StructType, DoubleType, StructField


sc = SparkContext()
sqlContext = SQLContext(sc)


schema = StructType([
    StructField('hotel', StringType()),
    StructField('avg_ratings', DoubleType())
])


spark = SparkSession.builder.master('local')\
            .config('spark.mongodb.input.partitioner', 'MongoShardedPartitioner')\
            .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/booking.hotel')\
            .config('spark.mongodb.output.uri', 'mongodb://localhost:27017/booking.result')
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')\
            .getOrCreate()


def get_hotel_address_udf(hotel):
    ### target1: load data from mongodb
    ### target2: extract element from list
    ### target3: use udf
    spark = SparkSession.builder.master('local')\
                .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/booking.hotel')\
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')\
                .getOrCreate()
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                .option('pipeline', {'$match':{'hotel': '{}'.format(hotel)}})\
                .load()
    return df.select('address').first()[0][0]


@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def group_process(data):
    ### target1: use groupby and apply 
    return_dict = {}
    return_dict['avg_ratings'] = data['ratings'].sum() // data.count()
    return_dict = pd.DataFrame([return_dict])
    return_dict = pd.concat([data[['hotel']], return_dict], axis=1, ignore_index = True)
    return return_dict


def write_data():
    ### target1: save data to mongodb
    ### you can use 'append' or 'overwrite' to save data
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
                                
    people.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').save()


def __name__ == '__main__':
    # load data via different way
    df = content = spark.read.format('com.mongodb.spark.sql.DefaultSource')\
            .option('database', 'booking')\
            .option('collection', 'hotel')\
            .load()

    invert_address_udf = udf(get_hotel_address_udf, StringType())
    df = df.withColumn('address', invert_address_udf('hotel'))
    df.show()

    df = df.drop('key').groupby(['hotel']).apply(group_process)
    df.show()
