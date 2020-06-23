object train {
  def main(args: Array[String]) {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{SparkSession, SQLContext}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.hadoop.fs.{FileSystem, Path}
    import org.apache.spark.ml.classification.{LogisticRegression, DecisionTreeClassifier}
    import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
    import org.apache.spark.ml.{Pipeline, PipelineModel}

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

    val weblogs = sparkSession
      .read
      .json("hdfs:///labs/laba07/weblogs_train_merged_labels.json")
      .repartition(1)

    val weblogs_explode = weblogs
      .select(col("uid")
        ,col("gender_age")
        ,explode(col("visits")).as("web"))
      .cache()

    val weblogs_url = weblogs_explode
      .withColumn("url", weblogs_explode("web.url"))
      .withColumn("timestamp", weblogs_explode("web.timestamp"))
      .withColumn("host", callUDF("parse_url", col("url"), lit("HOST")))
      .select(col("uid")
        , col("gender_age")
        , col("url")
        , regexp_replace(col("host"), "www.", "").as("host_not_www")
        , col("timestamp")
        , from_unixtime(col("timestamp") / 1000).as("datetime")
      )
      .repartition(10)

    val training = weblogs_url
      .groupBy("uid", "gender_age")
      .agg(collect_list("host_not_www").as("domains"))
      .cache()

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")
      .fit(training)

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(training)

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    /*
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
     */

    val ind_to_str = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("gender_age_pred")
      .setLabels(indexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, dt, ind_to_str))

    val model = pipeline.fit(training)

    model.write.overwrite().save(model_path)

    sc.stop()
  }
}
