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
      .setAppName("lab04")

    val sparkSession = SparkSession.builder()
      .config(conf=conf)
      .getOrCreate()

    var sc = sparkSession.sparkContext
    println("SparkContext started".toUpperCase)

    val topic_name = sc.getConf.get("spark.filter.topic_name")
    val offset = sc.getConf.get("spark.filter.offset")
    val output_dir_prefix = sc.getConf.get("spark.filter.output_dir_prefix")

    println("topic_name: " + topic_name)
    System.out.println(s"offset: $offset".toUpperCase)
    System.out.println(s"output_dir_prefix: $output_dir_prefix".toUpperCase)


    val fs = FileSystem.get(sc.hadoopConfiguration)

    val outPutPath = new Path(s"$output_dir_prefix/")

    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)

    System.out.println("Dropped table".toUpperCase)

    val df = sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("subscribe", topic_name)
      .option("startingOffsets", offset)
      .option("consumer_timeout_ms", 30000)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //.as[(String, String)]

    val schema = new StructType()
      .add("event_type",StringType)
      .add("category",StringType)
      .add("item_id",StringType)
      .add("item_price",FloatType)
      .add("uid",StringType)
      .add("timestamp",StringType)

    val df_parse = df
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")
    df_parse.show(1)

    val today = new SimpleDateFormat("yMMdd").format(Calendar.getInstance().getTime())
    System.out.println("Today date: " + today)

    val buy_df = df_parse
      .filter(col("event_type") === "buy")
      .withColumn("date", lit(today))
    buy_df.show(3)

    buy_df
      .write
      .mode("overwrite")
      .parquet(s"$output_dir_prefix/buy_$today")

    System.out.println("Writing buy table".toUpperCase)

    val view_df = df_parse
      .filter(col("event_type") === "view")
    view_df.show(3)

    view_df
      .write
      .mode("overwrite")
      .parquet(s"$output_dir_prefix/view_$today")

    System.out.println("Writing view table".toUpperCase)

    sc.stop()
  }
}