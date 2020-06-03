object filter {
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
      .setAppName("lab04a")

    val sparkSession = SparkSession.builder()
      .config(conf=conf)
      .getOrCreate()

    var sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    println("SparkContext started".toUpperCase)

    val topic_name = sc.getConf.get("spark.filter.topic_name")
    var offset = sc.getConf.get("spark.filter.offset")
    val output_dir_prefix = sc.getConf.get("spark.filter.output_dir_prefix")

    println("topic_name: " + topic_name)
    System.out.println(s"offset: $offset".toUpperCase)
    System.out.println(s"output_dir_prefix: $output_dir_prefix")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outPutPath = new Path(s"$output_dir_prefix/")
    //if (fs.exists(outPutPath))
    //  fs.delete(outPutPath, true)

    System.out.println("Dropped table".toUpperCase)

    val today = new SimpleDateFormat("yMMdd").format(Calendar.getInstance().getTime())
    System.out.println("Today date: " + today)

    if (offset != "earliest" && offset != "latest") {
      offset = s"""{"$topic_name":{"0":$offset}}"""
    }
    else if (offset == "latest") {
      //TODO: у Артема в задании косяк, стартовый оффсет не может быть latest в батче
      offset = "earliest"
    }
    System.out.println("offset: " + offset)

    /****************************************************************************************/
    /*                                     logics                                           */
    /****************************************************************************************/
    val df = sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("subscribe", topic_name)
      .option("startingOffsets", offset)
      .option("endingOffsets", "latest")
      .option("consumer_timeout_ms", 30000)
      .load()
      .select(col("value").cast("String"))
      .select(json_tuple(col("value"), "event_type", "category", "item_id", "item_price", "uid", "timestamp")
        .as(List("event_type", "category", "item_id", "item_price", "uid", "timestamp")))
      .withColumn("date", date_format(to_date(from_unixtime(col("timestamp")/1000)), "yyyyMMdd"))
      .withColumn("date_part", col("date"))
      .repartition(1)

    df.show(3)
    val df_cnt = df.count()
    System.out.println(s"Count: $df_cnt")

    df
        .filter(col("event_type") === "view")
        .write
        .mode("overwrite")
        .partitionBy("date_part")
        .json(s"$output_dir_prefix/view")

    df
      .filter(col("event_type") === "buy")
      .write
      .mode("overwrite")
      .partitionBy("date_part")
      .json(s"$output_dir_prefix/buy")


    //val df= sparkSession.read.json("/labs/laba04/visits-g")
    //  .withColumn("date", date_format(to_date(from_unixtime(col("timestamp") / 1000)), "yyyyMMdd"))

    /*
    Без partitionBy
    val dt = df.select("date").distinct().collect().map(_(0))
    //TODO: точно ли надо делать так, или через partitionBy
    for( x <- dt ){
      println(x)

      val dfdt= df
        .filter(col("date") === x)
        .cache()

      dfdt
        .filter(col("event_type") === "buy")
        .write
        .mode("overwrite")
        .json(s"$output_dir_prefix/buy_$x")

      dfdt
        .filter(col("event_type") === "view")
        .write
        .mode("overwrite")
        .json(s"$output_dir_prefix/view_$x")

    }
    */
    System.out.println("Writing view table".toUpperCase)

    sc.stop()

  }
}