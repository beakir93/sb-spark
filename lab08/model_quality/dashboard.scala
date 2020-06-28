
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions.{col, lit, current_timestamp, explode, to_timestamp, callUDF, regexp_replace
                                           , from_unixtime, collect_list}
import org.apache.spark.ml.{Pipeline, PipelineModel}

val conf = new SparkConf()
                            .setAppName("lab08")

val sparkSession = SparkSession.builder()
  .config(conf=conf)
  .getOrCreate()

var sc = sparkSession.sparkContext
val sqlContext = new SQLContext(sc)

val model_path = "/user/kirill.likhouzov/lab07/"

%AddJar file:///data/home/kirill.likhouzov/Drivers/elasticsearch-spark-20_2.11-7.6.2.jar

val dataset = spark
    .read
    .json("/labs/lab08/lab04_test5000_with_date_lab08.json")
    .repartition(1)

dataset.show(1)

val dataset_host = dataset.select($"uid"
                                ,$"date"
                                ,explode($"visits").as("web"))  
                        .withColumn("timestamp", $"web.timestamp")
                        .withColumn("url", $"web.url")
                        .withColumn("host", callUDF("parse_url", $"url", lit("HOST")))
                        .select($"uid"
                                ,$"date"
                                ,to_timestamp(from_unixtime($"timestamp" / 1000)).as("datetime_web")
                                ,regexp_replace($"host", "www.", "").as("host_not_www"))

dataset_host.show(3)

val test = dataset_host.groupBy("uid", "date")
                        .agg(collect_list("host_not_www").as("domains"))
                        .repartition(1)

print(test.count)
test.show(3)

val model = PipelineModel
                        .load(model_path)

val prediction  = model
                        .transform(test)
                        .select($"uid"
                                ,$"date"
                                ,$"gender_age_pred".as("gender_age"))
                        .repartition(1)

prediction.show(3)

val esOptions = 
    Map(
        "es.nodes" -> "10.0.1.9:9200/kirill_likhouzov_lab08", 
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true"   
    )

prediction
    .write
    .format("org.elasticsearch.spark.sql")
    .options(esOptions)
    .save("kirill_likhouzov_lab08-{date}/_doc")

val esDf = spark
                .read
                .format("es")
                .options(esOptions)
                .load("kirill_likhouzov_lab08-*")
esDf.printSchema
esDf.show(1, 200, true)

esDf.count

sc.stop

// ssh -i npl.pem -L 5601:10.0.1.9:5601 kirill.likhouzov@spark-de-master1.newprolab.com

// // удаление индекса
// curl -X DELETE http://kirill.likhouzov:password@10.0.1.9:9200/kirill_likhouzov_lab08-*

// // просмотр индекса
// curl http://kirill.likhouzov:password@10.0.1.9:9200/kirill_likhouzov_lab08-*

// // проверка дашборда
// curl -X GET 10.0.1.9:5601/api/kibana/dashboards/export?dashboard=6bd54520-b92c-11ea-8889-8de7ce1ad0f9

// PUT _template/kirill_likhouzov_lab08
// {
//   "index_patterns": ["kirill_likhouzov_lab08-*"],
//   "settings": {
//     "number_of_shards": 1,
//     "number_of_replicas" : 1
//   },
//   "mappings": {
//     "_doc": {
//       "dynamic": true,
//       "_source": {
//         "enabled": true
//       },
//       "properties": {
//         "uid": {
//         "type": "keyword"
//           },
//           "gender_age": {
//             "type": "keyword"
//           },
//           "date": {
//             "type": "date",
//             "format": "strict_date_optional_time||epoch_millis"
//           }
//       }
//     }
//   }
// }
