object agg {
  def main(args: Array[String]) {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{SparkSession, SQLContext}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._

    import org.apache.hadoop.fs.{FileSystem, Path}

    import java.text.SimpleDateFormat
    import java.util.{Calendar, Date}
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setAppName("lab04b")

    val sparkSession = SparkSession.builder()
      .config(conf=conf)
      .getOrCreate()

    var sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    println("SparkContext started".toUpperCase)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val checkPointPath = "/user/kirill.likhouzov/laba04b/checkpoint"
    val checkPointPth = new Path(checkPointPath)
    if (fs.exists(checkPointPth))
      fs.delete(checkPointPth, true)


    sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("subscribe", "kirill_likhouzov")
      .option("consumer_timeout_ms", 30000)
      .load()
      .select(col("value").cast("String"))
      .select(json_tuple(col("value"), "event_type", "category", "item_id", "item_price", "uid", "timestamp")
        .as(List("event_type", "category", "item_id", "item_price", "uid", "timestamp")))
      .withColumn("datetime", from_unixtime(col("timestamp") / 1000))
      .groupBy(window(col("datetime"), "60 minutes"))
      .agg(min("timestamp").as("start_ts")
        ,max("timestamp").as("end_ts")
        ,min("datetime").as("start_dt")
        ,max("datetime").as("end_dt")
        ,sum(when(col("event_type") === "buy", col("item_price"))).as("revenue")
        ,approx_count_distinct(when(col("uid").isNull, "aaaaaoaoaoaoaoao0").otherwise(col("uid"))).as("visitors")
        ,sum(when(col("event_type") === "buy", 1).otherwise(0)).as("purchases")
        ,(sum(when(col("event_type") === "buy", col("item_price")))
                    / sum(when(col("event_type") === "buy", 1).otherwise(0))).as("aov")
      )
      .withColumn("start_ts_tms", unix_timestamp(col("start_dt")))
      .withColumn("start_ts_1h_tms", unix_timestamp(col("start_dt") + expr("INTERVAL 1 HOURS")))
      .select(col("start_ts_tms").as("start_ts")
              ,col("start_ts_1h_tms").as("end_ts")
              //,col("start_dt")
              //,col("end_dt")
              ,col("revenue")
              ,col("visitors")
              ,col("purchases")
              ,col("aov")
      )
      .toJSON
      .writeStream
      /*.format("console")
      .option("numRows", 1000)
      .option("truncate", false)*/
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("subscribe", "kirill_likhouzov_lab04b_out")
      .outputMode("update")
      .option("checkpointLocation", "/user/kirill.likhouzov/laba04b/checkpoint")
      .start
      .awaitTermination(100000)


    System.out.println("Writing view table".toUpperCase)

    sc.stop()

  }
}