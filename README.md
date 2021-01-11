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
```
.
├── README.md
└── conf
    ├── core-site.xml
    ├── hbase-site.xml
    └── yarn-site.xml
```

* The core-site.xml file informs Hadoop daemon where NameNode runs in the cluster.

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


#### Spark Important Concept

![Spark Architecture](https://blog.knoldus.com/wp-content/uploads/2019/12/Image-1-1.png)

+ Spark Architecture
  * The central coordinator is called Spark Driver and it communicates with all the Workers.
  * Each Worker node consists of one or more Executor(s) who are responsible for running the Task. Executors register themselves with Driver. The Driver has all the information about the Executors at all the time.

+ Driver
  * 掌控Spark應用程式的程序，維護叢集的所有狀態。

+ Executor
  * 取得Driver分配的任務、執行，並回報狀態與結果。

#### Spark 生命週期(外部)
+ 啟動
    * Driver程序已放置到叢集中，並且開始執行使用者的程式碼。程式碼必須包含SparkSession以初始化Spark叢集，SparkSession接著會與叢集管理員溝通，要求在叢集啟動Spark executor 程序，使用者透過原先spark-submit 命令列參數可設定 executor 數量及相關設定。
    * 叢集管理員啟動executor程序作出回應，並發送包含executor位置的相關資訊給Driver程序，待一切都正確連結就建立了Spark叢集。

+ 執行
    * 有了Spark叢集之後，Spark接著會執行程式碼，Driver與工作節點會互相溝通、執行程式碼及移動資料，Driver負責排定任務至各工作節點，各節點則回報任務執行的成功或失敗狀態。

+ 完成
    * Spark完成後，Driver會回報成功或失敗狀態並關閉。


#### Spark 生命週期(內部)
+ SparkSession
    * 透過SparkSession的builder方法，此方法可實體化Spark及SQL Contexts並確保沒有context衝突。有了SparkSession就可以執行Spark程式碼。

+ SparkContext
    * SparkSession中的SparkContext物件代表與Spark叢集的連結，此類別可與某些低階API溝通，並且建立RDD、累加器、及廣播變數，使程式碼可在叢集上執行。

+ 邏輯指示
    * Spark如何將程式碼轉為實際在叢集上運行的指令

+ Spark工作
    * 一個Action對應一個Spark工作，Action總是會返回結果，每個工作又可拆分為一系列階段，階段數取決於有多少洗牌操作發生。

+ 階段
    * Spark的階段代表的是一群任務。 Spark會盡可能在同一階段包含越多工作，但在發生洗牌操作後，會啟動一個新的階段，洗牌代表資料重新分區。

+ 任務
    * Spark階段由任務組成，每個任務與在單一executor執行的資料區塊與轉換操作有關，例如一個資料只分布在大分區，則只會有一個任務；1000個小分區，則會有1000個可平行執行的任務。

+ 排程器
    * Spark排程器預設以FIFO方式執行，如果堆疊最前面的工作不需使用整個叢集，後面的工作可以立刻開始執行；反之，則會延遲後面的工作。

    * 亦可以將工作設定為公平分享模式，在此模式下，Spark以輪替方式在工作間賦予任務，所有工作可共享叢集資源，表示小型工作在大型工作正在執行時可立刻獲得資源，無需等待大型工作結束。

    * 在設定SparkContext時可以將spark.scheduler.mode property設為FAIR。