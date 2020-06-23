object test {
  def main(args: Array[String]) {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{SparkSession, SQLContext}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.hadoop.fs.{FileSystem, Path}
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
    import org.apache.spark.ml.{Pipeline, PipelineModel}
    import org.apache.spark.sql.types.{StructType, StructField, LongType, StringType, ArrayType}
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger
    import org.apache.spark.sql.DataFrame


    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setAppName("lab07")

    val sparkSession = SparkSession.builder()
      .config(conf=conf)
      .getOrCreate()

    var sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    println("SparkContext started".toUpperCase)

    val model_path = sc.getConf.get("spark.filter.model_path")

    System.out.println(s"model_path: $model_path")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val checkPointPath = "/user/kirill.likhouzov/laba04b/checkpoint"
    val checkPointPth = new Path(checkPointPath)
    if (fs.exists(checkPointPth))
      fs.delete(checkPointPth, true)

    val schema =
      StructType(
        Seq(
          StructField("uid", StringType),
          StructField(
            "visits",
            ArrayType(
              StructType(
                Seq(
                  StructField("timestamp", LongType),
                  StructField("url", StringType)
                )
              )
            )
          )
        )
      )

    val kafka_init = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("subscribe", "kirill_likhouzov")
      .option("consumer_timeout_ms", 50000)
      .load()
      .select(from_json(col("value").cast("string"), schema).as("value"))
      .select("value.*")
      .select(col("uid")
        ,explode(col("visits")).as("web"))

      .withColumn("timestamp", col("web.timestamp"))
      .withColumn("url", col("web.url"))
      .withColumn("host", callUDF("parse_url", col("url"), lit("HOST")))
      .select(col("uid")
        ,to_timestamp(from_unixtime(col("timestamp") / 1000)).as("datetime")
        ,regexp_replace(col("host"), "www.", "").as("host_not_www"))

      .withWatermark("datetime", "1 day")

    val test= kafka_init.groupBy("uid")
      .agg(collect_list("host_not_www").as("domains"))

    val model = PipelineModel.load(model_path)
    val df_out = model.transform(test)

    df_out
      .select($"uid", $"gender_age_pred".as("gender_age"))
      .toJSON
      .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.0.1.13:6667")
    .outputMode("update")
    .option("topic", "kirill_likhouzov_lab04b_out")
    .option("checkpointLocation", "/user/kirill.likhouzov/laba04b/checkpoint")
    .start

    sc.stop()
  }
}
