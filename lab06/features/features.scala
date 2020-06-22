object features {
  def main(args: Array[String]) {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{SparkSession, SQLContext}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import sys.process._
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.expressions.Window
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
    val vect_class = new vectorized
    val weblogs_web = vect_class.transform(weblogs_url)

    val users_items = sparkSession
      .read
      .parquet("/user/kirill.likhouzov/users-items/20200429")
      .repartition(100)
      .cache

    val time_users_items =
      weblogs_feature_time
        .join(broadcast(users_items), Seq("uid"), "left")
        .repartition(100)
        .cache

    val res =
      time_users_items
        .join(weblogs_web, Seq("uid"), "left")
        .na.fill(0)
        .repartition(10)
        .cache

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
