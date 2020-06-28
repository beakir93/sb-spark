
PUT _template/kirill_likhouzov_lab08
{
  "index_patterns": ["kirill_likhouzov_lab08-*"],
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas" : 1
  },
  "mappings": {
    "_doc": {
      "dynamic": true,
      "_source": {
        "enabled": true
      },
      "properties": {
        "uid": {
        "type": "keyword",
        "null_value": "NULL"
          },
          "gender_age": {
            "type": "keyword"
          },
          "date": {
            "type": "date"
          }
      }
    }
  }
}

import sys.process._
"cat /tmp/qwi7".!!

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions._

val conf = new SparkConf()
                            .setAppName("lab08")

val sparkSession = SparkSession.builder()
  .config(conf=conf)
  .getOrCreate()

var sc = sparkSession.sparkContext
val sqlContext = new SQLContext(sc)

val dataset = spark
    .read
    .json("file:///tmp/qwi7")
    .filter($"index".isNull)
    .select($"uid", $"gender_age", $"date")
    .repartition(1)

dataset.show(3)

%AddJar file:///data/home/kirill.likhouzov/Drivers/elasticsearch-spark-20_2.11-7.6.2.jar

import org.apache.spark.sql.functions._

val esOptions = 
    Map(
        "es.nodes" -> "10.0.1.9:9200/kirill_likhouzov_lab08", 
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true"   
    )

dataset
    .write
    .format("org.elasticsearch.spark.sql")
    .options(esOptions)
    .save("kirill_likhouzov_lab08-{date}/_doc")

val esDf = spark.read.format("es").options(esOptions).load("kirill_likhouzov_lab08-*")
esDf.printSchema
esDf.show(1, 200, true)

var df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaService.bootstrapServers)
        .option("enable.auto.commit", KafkaService.enableAutoCommit)
        .option("failOnDataLoss", KafkaService.failOnDataLoss)
        .option("startingOffsets", KafkaService.startingOffsets)
        .option("subscribe", topicName)
        .option("group.id", groupId)
        .load()
    
    df.writeStream
    .outputMode(OutputMode.Append) //Only mode for ES
    .format("") //es
    .queryName("ElasticSink" + topicName)
    .start(indexName + "/broadcast") //ES index

sc.stop
