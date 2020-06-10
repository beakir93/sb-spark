
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

val conf = new SparkConf()
                            .setAppName("lab6")
conf.set("spark.sql.pivotMaxValues", "110000")

val sparkSession = SparkSession.builder()
  .config(conf=conf)
  .getOrCreate()

var sc = sparkSession.sparkContext
val sqlContext = new SQLContext(sc)

val weblogs = spark
    .read
    .parquet("/labs/laba03/weblogs.parquet")
    .repartition(1)
    .cache()
weblogs.count

val weblogs_explode = weblogs
                            .select($"uid"
                                    ,explode($"visits").as("web"))
                            .cache()

weblogs_explode.show(1)

//TODO: зачем в задании указано про пол/возраст

val weblogs_url = weblogs_explode
                            .withColumn("url", weblogs_explode("web.url"))
                            .withColumn("timestamp", weblogs_explode("web.timestamp"))
                            .withColumn("host", callUDF("parse_url", $"url", lit("HOST")))
                            .select($"uid"
                                        , $"url"
                                        , regexp_replace($"host", "www.", "").as("host_not_www")
                                        , $"timestamp"
                                        , from_unixtime($"timestamp" / 1000).as("datetime")
                                    )
                            .repartition(10)

weblogs_url.show(1, 1000, true)

val weblogs_feature_day_week= 
    weblogs_url
                .withColumn("web_day", concat(lit("web_day_"), lower(date_format(col("datetime"), "E"))))
                .groupBy("uid")
                .pivot("web_day")
                .agg(count("*"))
                .na.fill(0)

print(weblogs_feature_day_week.count)
weblogs_feature_day_week.filter($"uid" === "cdbbe6d4-4d9f-47a9-ad3d-cb6a7ceb9bdf").show(1)

val weblogs_feature_hour= 
    weblogs_url
                .withColumn("web_hour", concat(lit("web_hour_"), hour(col("datetime"))))
                .groupBy("uid")
                .pivot("web_hour")
                .agg(count("*"))
                .na.fill(0)

print(weblogs_feature_hour.count)
weblogs_feature_hour.filter($"uid" === "cdbbe6d4-4d9f-47a9-ad3d-cb6a7ceb9bdf").show(1)

val web_fraction = weblogs_url
                            .withColumn("work_hour", when((hour(col("datetime")) >= 9) && (hour(col("datetime")) < 18), 1)
                                                            .otherwise(0))
                            .withColumn("evng_hour", when((hour(col("datetime")) >= 18) && (hour(col("datetime")) < 24), 1)
                                                            .otherwise(0))       
                            .groupBy("uid")
                            .agg(sum("work_hour").as("work_hour")
                                ,sum("evng_hour").as("evng_hour")
                                ,count("*").as("cnt"))

print(web_fraction.count)
web_fraction.show(1)

val weblogs_feature_time = weblogs_feature_day_week.as("a")
                                .join(weblogs_feature_hour.as("b"), $"a.uid" === $"b.uid", "left")
                                .join(web_fraction.as("c"), $"a.uid" === $"c.uid", "left")
                                .withColumn("web_fraction_work_hours", $"c.work_hour" / $"c.cnt")
                                .withColumn("web_fraction_evng_hours", $"c.evng_hour" / $"c.cnt")
                                .drop($"b.uid")
                                .drop($"c.uid")
                                .drop("work_hour", "evng_hour", "cnt")
                                
weblogs_feature_time.show(1, 100, true)

val weblogs_web = 
    weblogs_url
        .groupBy("uid")
        .pivot("host_not_www")
        .agg(count("*"))
        .na.fill(0)

weblogs_web.count

val weblogs_res = 
    weblogs_web.join(broadcast(weblogs_feature_time), Seq("uid"), "left")

weblogs_res.count

val users_items = spark
                        .read
                        .parquet("/user/kirill.likhouzov/users-items/20200429")
                        .repartition(100)

users_items.count

val res= 
        weblogs_res.join(users_items, Seq("uid"), "left")

res.count

res
    .write
    .mode("overwrite")
    .parquet("/user/kirill.likhouzov/features")

//weblogs_url.count

//weblogs_url.select($"uid").distinct.count

//weblogs_url.select($"host_not_www").distinct.count

sc.stop()
