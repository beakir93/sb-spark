object users_items {
  def main(args: Array[String]) {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{SparkSession, SQLContext}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import sys.process._
    import org.apache.hadoop.fs.{FileSystem, Path}

    import java.text.SimpleDateFormat
    import java.util.{Calendar, Date}

    import org.apache.spark.sql.DataFrame
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setAppName("lab05")

    conf.set("spark.sql.session.timeZone", "UTC")

    val sparkSession = SparkSession.builder()
      .config(conf=conf)
      .getOrCreate()

    var sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    println("SparkContext started".toUpperCase)

    val update_mode = sc.getConf.get("spark.users_items.update")
    val output_dir = sc.getConf.get("spark.users_items.output_dir")
    val input_dir = sc.getConf.get("spark.users_items.input_dir")

    println(s"update_mode: " + update_mode)
    System.out.println(s"output_dtr: $output_dir")
    System.out.println(s"input_dir: $input_dir")

//    val fs = FileSystem.get(sc.hadoopConfiguration)
//    val outPutPath = new Path(s"$output_dir_prefix/")
//      if (fs.exists(outPutPath))
//          fs.delete(outPutPath, true)
//  System.out.println("Dropped table".toUpperCase)

//    val today = new SimpleDateFormat("yMMdd").format(Calendar.getInstance().getTime())
//    System.out.println("Today date: " + today)

    def union_df_diff_col (df1: DataFrame, df2: DataFrame) = {

      def col_null (df1:DataFrame, df2:DataFrame) = {
        var df_tmp :DataFrame = df2
        for (col <- df1.columns) {
          if (!df_tmp.columns.contains(col)) {
            //println(col)
            df_tmp = df_tmp.withColumn(col, lit(null))
          }
        }
        df_tmp
      }

      val df_1_tmp = col_null(df1, df2)
      val df_2_tmp = col_null(df2, df1)

      val df_res = df_1_tmp.union(df_2_tmp)
      df_res
    }

    /****************************************************************************************/
    /*                                     logics                                           */
    /****************************************************************************************/

    val df_buy= sparkSession
                    .read
                    .json(s"$input_dir/buy")

    val df_view= sparkSession
                    .read
                    .json(s"$input_dir/view")

    val df_buy_view= df_buy
                    .union(df_view)
    df_buy_view.count

    val dt_max_arr= df_buy_view
                    .agg(max(col("date")))
                    .collect
                    .map(x => x(0))
    val dt_max = dt_max_arr(0)
    System.out.println("dt_max: " + dt_max)

    val df_pvt= df_buy_view
        .withColumn("item_type", when(col("event_type") === "buy",
          concat(lit("buy_"), lower(regexp_replace(regexp_replace(col("item_id"), "-", "_"), " ", "_"))))
          .otherwise(concat(lit("view_"), lower(regexp_replace(regexp_replace(col("item_id"), "-", "_"), " ", "_")))))
        .groupBy("uid")
        .pivot("item_type")
        .count
        .repartition(1)

    //df_pvt.show(3)

    if (update_mode.toInt == 1) {
      System.out.println("update_mode == 1")
      System.out.println("hdfs dfs -ls file:///data/home/labchecker2/checkers/logs/sb1laba05/kirill.likhouzov/".!!)
      System.out.println("hdfs dfs -ls file:///data/home/labchecker2/checkers/logs/sb1laba05/kirill.likhouzov/users-items".!!)

      val users_items_old = sparkSession
                    .read
                    .parquet(s"$output_dir/20200429")

      //users_items_old.show(3)
                    //TODO: заменить хардкод даты пути на чтение папок из hdfs

      val df_union = union_df_diff_col(df_pvt, users_items_old)
      df_union
              .na.fill(0)
              .write
              .mode("overwrite")
              .parquet(s"$output_dir/$dt_max")
    }
    else {
      df_pvt
        .na.fill(0)
        .write
        .mode("append")
        .parquet(s"$output_dir/20200429")
    }



    System.out.println("Writing view table".toUpperCase)

    sc.stop()

  }
}
