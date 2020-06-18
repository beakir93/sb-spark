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
    val cnf = sc.getConf.getAll

    System.out.println(s"spark conf: $cnf")
    System.out.println("-----")
    System.out.println(cnf(0))

    /*val model_path = sc.getConf.get("spark.filter.model_path")

    System.out.println(s"model_path: $model_path")
    */
  /*
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
      .option("consumer_timeout_ms", 30000)
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
      .withColumn("gender_age", when(col("prediction") === 6, "M:45-54")
        .when(col("prediction") === 4, "F:18-24")
        .when(col("prediction") === 9, "M:>=55")
        .when(col("prediction") === 7, "M:18-24")
        .when(col("prediction") === 1, "F:25-34")
        .when(col("prediction") === 8, "F:>=55")
        .when(col("prediction") === 5, "F:45-54")
        .when(col("prediction") === 2, "M:35-44")
        .when(col("prediction") === 3, "F:35-44")
        .when(col("prediction") === 0, "M:25-34")
        .otherwise(""))
      .select(col("uid"), col("gender_age"))
      .toJSON
      .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.0.1.13:6667")
    .outputMode("update")
    .option("topic", "kirill_likhouzov_lab04b_out")
    .option("checkpointLocation", "/user/kirill.likhouzov/laba04b/checkpoint")
    .start
*/
    sc.stop()
  }
}
