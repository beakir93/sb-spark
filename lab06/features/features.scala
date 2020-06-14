object features {
  def main(args: Array[String]) {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{SparkSession, SQLContext}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import sys.process._
    import org.apache.spark.sql.DataFrame
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setAppName("lab06")

    val sparkSession = SparkSession.builder()
      .config(conf=conf)
      .getOrCreate()

    var sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    val weblogs = sparkSession
      .read
      .parquet("/labs/laba03/weblogs.parquet")
      .repartition(1)

    val weblogs_explode = weblogs
      .select(col("uid")
        ,explode(col("visits")).alias("web"))

    val weblogs_url = weblogs_explode
      .withColumn("url", weblogs_explode("web.url"))
      .withColumn("timestamp", weblogs_explode("web.timestamp"))
      .withColumn("host", callUDF("parse_url", col("url"), lit("HOST")))
      .select(col("uid")
        , col("url")
        , regexp_replace(col("host"), "www.", "").alias("host_not_www")
        , col("timestamp")
        , from_unixtime(col("timestamp") / 1000).alias("datetime")
      )
      .repartition(10)
      .cache

    //feature times
    val time_feature_class = new time_feature
    val weblogs_feature_time = time_feature_class.transform(weblogs_url)
                                .repartition(100)
                                .cache

    //CountVectorized
    val weblogs_list_url = weblogs_url
      .groupBy("uid")
      .agg(collect_list("host_not_www").as("list_url"))
      .cache()

    val weblogs_res =
      weblogs_feature_time
        .join(weblogs_list_url, Seq("uid"), "left")
        .repartition(100)

    val users_items = sparkSession
      .read
      .parquet("/user/kirill.likhouzov/users-items/20200429")
      .repartition(100)
      .cache

    val res_t =
      weblogs_res
        .join(broadcast(users_items), Seq("uid"), "left")
        .repartition(100)
        .cache


    val vect_class = new vectorized
    val res = vect_class.transform(res_t)

    res.filter(col("uid") === "094b1e7e-97a6-4415-89f2-ca1eb72976b9").show(1, 1000, true)

    res
      .write
      .mode("overwrite")
      .parquet("/user/kirill.likhouzov/features")

    System.out.println(res.count)


    System.out.println("Wrote table")
    sc.stop()

  }
}
