from pyspark import SparkContext
from pyspark.sql import functions
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import collect_list, lit, udf, when, rand, struct, col
from pyspark.sql.types import StringType, ArrayType, DoubleType, StructType, StructField, TimestampType

from datetime import datetime, timedelta
import json

sc = SparkContext()
sqlContext = SQLContext(sc)


def query_hotel_info(city, start_date, end_date):
    es_read_conf = {
            "es.nodes": "localhost",
            "es.port": 9200,
            "es.resource": "booking/_doc",
            "es.mapping.date.rich": False,
            "es.read.field.as.array.include": "tourists, facilities",
            "es.read.field.include": "pageUrl, hotel, address, city, ratings, description, tourists, facilities, created_time"
        }

    query = {
            "_source": ["pageUrl, hotel, address, city, ratings, description, tourists, facilities"],
            "query": {
                "bool": {
                        "must": [
                            {"exists": {"field": "ratings"}},
                            {"match_phrase":{"city":city}},
                            {"range": { 
                                "created_time": { 
                                    "gte": start_date, "lte":end_date,"time_zone": "+08:00", "format": "yyyy-MM-dd HH:mm:ss"
                                    } 
                                } 
                            }
                        ]
                    }
            }
        }
    
    schema = StructType([
        StructField("pageUrl", StringType(), True),
        StructField("hotel", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("ratings", StringType(), True),
        StructField("description", StringType(), True),
        StructField('tourists', ArrayType(StringType())),
        StructField('facilities', ArrayType(StringType()))
    ])

    q = json.dumps(query)

    df_hotel = sqlContext.read.format("es") \
        .options(**es_read_conf) \
        .option('es.query', q) \
        .schema(schema) \
        .load()
    
    return df_hotel

if __name__=="__main__":
    city = "台北市"
    start_date = datetime.now().strftime("%Y-%m-%d")
    end_date = (start_date + timedelta(days=10)).strftime("%Y-%m-%d")
    query_hotel_info(city, start_date, end_date)


# spark-submit --name=elk_caculate --master yarn --deploy-mode cluster --packages org.apache.spark:spark-avro_2.11:2.4.3,org.elasticsearch:elasticsearch-hadoop:7.4.2 --conf=spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/pyspark/env/bin/python --driver-memory=10g --executor-memory=5g ./elasticsearch.py