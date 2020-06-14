import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, count, date_format, hour, lit, lower, sum, when}

class time_feature {

  def transform(weblogs_url: DataFrame): DataFrame = {
    val weblogs_feature_day_week =
      weblogs_url
        .withColumn("web_day", concat(lit("web_day_"), lower(date_format(col("datetime"), "E"))))
        .groupBy("uid")
        .pivot("web_day")
        .agg(count("*"))
        .na.fill(0)

    val weblogs_feature_hour =
      weblogs_url
        .withColumn("web_hour", concat(lit("web_hour_"), hour(col("datetime"))))
        .groupBy("uid")
        .pivot("web_hour")
        .agg(count("*"))
        .na.fill(0)

    val web_fraction = weblogs_url
      .withColumn("work_hour", when((hour(col("datetime")) >= 9) && (hour(col("datetime")) < 18), 1)
        .otherwise(0))
      .withColumn("evng_hour", when((hour(col("datetime")) >= 18) && (hour(col("datetime")) < 24), 1)
        .otherwise(0))
      .groupBy("uid")
      .agg(sum("work_hour").as("work_hour")
        , sum("evng_hour").as("evng_hour")
        , count("*").as("cnt"))

    val weblogs_feature_time = weblogs_feature_day_week.as("a")
      .join(weblogs_feature_hour.as("b"), col("a.uid") === col("b.uid"), "left")
      .join(web_fraction.as("c"), col("a.uid") === col("c.uid"), "left")
      .withColumn("web_fraction_work_hours", col("c.work_hour") / col("c.cnt"))
      .withColumn("web_fraction_evng_hours", col("c.evng_hour") / col("c.cnt"))
      .drop(col("b.uid"))
      .drop(col("c.uid"))
      .drop("work_hour", "evng_hour", "cnt")

    weblogs_feature_time
  }
}
