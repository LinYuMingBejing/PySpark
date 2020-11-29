># Hadoop + PySpark Introduction  
### Enviroment:
* Ubuntu: 16.04 
* Python: 3.6.2
* PySpark: 2.4.4
* Database: MySQL, MongoDB, Elasticsearch, Hbase, HDFS
* Deploy Mode: Cluster (YARN)

![YARN1](https://img.onl/OVs21C)


### How to install Pyspark?

* Install Java
```
$ sudo apt-get update
$ sudo apt-get install openjdk-8-jdk
$ java -version
```

* Install Scala
```
$ wget https://downloads.lightbend.com/scala/2.12.8/scala-2.12.8.deb
$ sudo dpkg -i scala-2.12.8.deb
$ scala -version
```

* Install Spark
```
$ wget http://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz 
$ tar xvf  spark-2.4.4-bin-hadoop2.7.tgz
```

* Configure Environment Variables
```
$ vim ~/.bashrc
```
```
export SPARK_HOME=/home/ubuntu/spark-2.4.4-bin-hadoop2.7
export PATH=${SPARK_HOME}/bin:$PATH
```
```
$ source ~/.bashrc
```

### Introducing conf Directory.
  +-- conf
  |   +-- yarn-site.xml
  |   +-- core-site.xml

* The core-site. xml file informs Hadoop daemon where NameNode runs in the cluster.

* The yarn-site.xml file contains the configuration settings for HDFS daemons; the NameNode, the Secondary NameNode, and the DataNodes.

* You can modify yarn-site.xml depending on your Hadoop IP.

### Running Spark on YARN.
* Ensure that HADOOP_CONF_DIR or YARN_CONF_DIR points to the directory which contains the configuration files for the Hadoop cluster.
* These configs are used to write to HDFS and connect to the YARN ResourceManager. 
```
$  export HADOOP_USER_NAME=admin
$  export HADOOP_CONF_DIR=./conf/
```

### How to test PySpark in shell?
```
$  pyspark
```
![pyspark shell](https://img.onl/HyByD5)


### How to run avro.py?
```
$ spark-submit \
    --name=avro_test \
    --master yarn \
    --deploy-mode cluster \
    --packages org.apache.spark:spark-avro_2.11:2.4.3 \
    --conf=spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/pyspark/env/bin/python \
    --driver-memory=5g \
    --executor-memory=10g \
    ./avro.py 
```


### How to run elasticsearch.py?
```
$ spark-submit \
    --name=elasticsearch_test \
    --master yarn \
    --deploy-mode cluster \
    --packages org.elasticsearch:elasticsearch-hadoop:7.4.2 \
    --conf=spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/pyspark/env/bin/python \
    --driver-memory=5g \
    --executor-memory=10g \
    ./elasticsearch.py 
```

### How to run hbase_rdd.py?
```
$ spark-submit \
    --name=hbaseRdd_test \
    --master yarn \
    --deploy-mode cluster \
    --conf "spark.driver.extraClassPath=/etc/hbase/conf/",spark.driver.maxResultSize=4g \
    --driver-memory=10g \
    --executor-memory=20g \
    ./hbase_rdd.py 
```

### How to run mongodb.py?
```
$ spark-submit \
    --name=mongoDB_test \
    --master yarn \
    --deploy-mode cluster \
    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 \
    --driver-memory=5g \
    --executor-memory=10g \
    ./mongodb.py 
```

### How to run jdbc.py?
```
$ spark-submit \
    --name jdbc_test \
    --master yarn \
    --deploy-mode cluster \
    --packages mysql:mysql-connector-java:8.0.18 \
    --driver-memory=5g \
    --executor-memory=10g \
    ./jdbc.py 
```

### How to run hbase_dataframe.py?
```
$ spark-submit \
    --name=hbase_test \
    --master yarn \
    --deploy-mode cluster \
    --conf=spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/pyspark/env/bin/python \
    --jars shc-core-spark-2.3.0-hbase-2.1.0.jar,    hbase-spark-1.2.0-cdh5.7.1.jar,shc-core-1.1.1-2.1-s_2.11.jar,hbase-client-2.2.4.jar,hbase-server-2.2.4.jar \
    --driver-memory=5g \
    --executor-memory=10g \
    ./hbase_dataframe.py
```


#### Note1 : Driver and Executor

![Spark Architecture](https://blog.knoldus.com/wp-content/uploads/2019/12/Image-1-1.png)

+ Spark Architecture
  * The central coordinator is called Spark Driver and it communicates with all the Workers.
  * Each Worker node consists of one or more Executor(s) who are responsible for running the Task. Executors register themselves with Driver. The Driver has all the information about the Executors at all the time.

+ Driver
  * Driver is a Java process. This is the process where the main() method of our Scala, Java, Python program runs.


+ Executor

  * To run an individual Task and return the result to the Driver.
  * It can cache (persist) the data in the Worker node.


#### Note: How to allow multiple ips connect mongodb?

* vim /etc/mongod.conf
```
    # bindIp: 127.0.0.1
    bindIp: 0.0.0.0  
```

* restart mongodb
```
$ sudo service mongod restart
$ sudo service mongod status
```

* firewall settings
```
$ sudo ufw enable
$ sudo ufw deny  from 192.168.18.0/24 to any port 27017
$ sudo ufw allow from 192.168.18.0/24 to any port 27017
$ sudo ufw status
```

#### Note2: How to create multiple user on mongodb?

* create user
```
> use booking;
switched to db booking
> db.createUser(
  {
    user: "dbadmin",
    pwd: "StrongPassword",
    roles: [ { role: "readWrite", db: "booking" } ]
  }
)
> exit
bye
```

* vim /etc/mongod.conf 
```
security:
  authorization: enabled
```

* restart mongodb
```
$ sudo service mongod restart
$ sudo service mongod status
```