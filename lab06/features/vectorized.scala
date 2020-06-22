import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, broadcast, col, count, row_number, udf}

class vectorized {
  import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
  import org.apache.spark.ml.linalg.{SparseVector, Vectors}

  def transform(weblogs_url: DataFrame): DataFrame = {
    val top_weblogs_web_all =
      weblogs_url
        .filter(!col("host_not_www").isNull)
        .groupBy("host_not_www")
        .agg(count("*").as("cnt"))
        .orderBy(col("cnt").desc, col("host_not_www"))
        .limit(1000)
        .select(col("host_not_www"), col("cnt"), row_number.over(Window.orderBy(col("host_not_www"))).as("rn"))

    val users_web = weblogs_url
      .join(broadcast(top_weblogs_web_all), Seq("host_not_www"), "left") //чтобы не откинуть пользаков без доменов
      .groupBy(col("uid"))
      .pivot(col("rn"))
      .count()
      .na.fill(0)
      .drop(col("null"))

    val weblogs_web = users_web
      .select(col("uid"),
        array(users_web.columns.drop(1).map(col):_*).as("domain_features"))

    weblogs_web
  }
}
