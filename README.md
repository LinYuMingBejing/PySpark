># Hadoop + PySpark Introduction  
### Enviroment:
* Ubuntu: 16.04 
* Python: 3.6.2
* PySpark: 2.4.4
* Database: MySQL, MongoDB, Elasticsearch, Hbase, HDFS
* Deploy Mode: Cluster (YARN)

![YARN1](https://img.onl/1rDJdy)
![YARN2](https://img.onl/OVs21C)

### Hadoop Configuration
* You can modify yarn-site.xml depending on your Hadoop uri.

### How to install Pyspark?
* Install Java
```
sudo apt-get update
sudo apt-get install openjdk-8-jdk
java -version
```

* Install Scala
```
wget https://downloads.lightbend.com/scala/2.12.8/scala-2.12.8.deb
sudo dpkg -i scala-2.12.8.deb
scala -version
```

* Install Spark
```
wget http://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz 
 tar xvf  spark-2.4.4-bin-hadoop2.7.tgz
```

* Configure environment variables
```
vim ~/.bashrc
```
```
export SPARK_HOME=/home/ubuntu/spark-2.4.4-bin-hadoop2.7
export PATH=${SPARK_HOME}/bin:$PATH
```
```
source ~/.bashrc
```

### How to deploy to YARN?
```
export HADOOP_USER_NAME=USERNAME
export HADOOP_CONF_DIR=/yarn/
```

### How to test PySpark in shell?
```
pyspark
```
![pyspark shell](https://img.onl/HyByD5)

### How to run avro.py?
```
spark-submit --name=avro_test --master yarn --deploy-mode cluster --packages org.apache.spark:spark-avro_2.11:2.4.3 --conf=spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/pyspark/env/bin/python --driver-memory=5g --executor-memory=10g ./avro.py 
```


### How to run elasticsearch.py?
```
spark-submit --name=elasticsearch_test --master yarn --deploy-mode cluster --packages org.elasticsearch:elasticsearch-hadoop:7.4.2 --conf=spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/pyspark/env/bin/python  --driver-memory=5g --executor-memory=10g ./elasticsearch.py 
```

### How to run hbase_rdd.py?
```
spark-submit --name=hbaseRdd_test --master yarn --deploy-mode cluster --conf "spark.driver.extraClassPath=/etc/hbase/conf/",spark.driver.maxResultSize=4g --driver-memory=10g --executor-memory=20g ./hbase_rdd.py 
```

### How to run mongodb.py?
```
spark-submit --name=mongoDB_test --master yarn --deploy-mode cluster --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 --driver-memory=5g --executor-memory=10g ./mongodb.py 
```

### How to run jdbc.py?
```
spark-submit --name=jdbc_test --master yarn --deploy-mode cluster --packages mysql:mysql-connector-java:8.0.18 --driver-memory=5g --executor-memory=10g ./jdbc.py 
```

### How to run hbase_dataframe.py?
```
spark-submit --name=hbase_test --master yarn --deploy-mode cluster --conf=spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/pyspark/env/bin/python --jars shc-core-spark-2.3.0-hbase-2.1.0.jar,hbase-spark-1.2.0-cdh5.7.1.jar,shc-core-1.1.1-2.1-s_2.11.jar,hbase-client-2.2.4.jar,hbase-server-2.2.4.jar --driver-memory=5g --executor-memory=10g ./hbase_dataframe.py
```