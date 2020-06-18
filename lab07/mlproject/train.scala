object train {
  def main(args: Array[String]) {
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.{SparkSession, SQLContext}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.hadoop.fs.{FileSystem, Path}

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
      .setAppName("lab07")

    val sparkSession = SparkSession.builder()
      .config(conf=conf)
      .getOrCreate()

    var sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    println("SparkContext started".toUpperCase)
    val cnf = sc.getConf.getAll

    System.out.println(s"spark conf: $cnf")
    System.out.println("-----")
    System.out.println(cnf(0))
    /*val topic_name = sc.getConf.get("spark.filter.topic_name")
    var offset = sc.getConf.get("spark.filter.offset")
    val output_dir_prefix = sc.getConf.get("spark.filter.output_dir_prefix")

    println("topic_name: " + topic_name)
    System.out.println(s"offset: $offset".toUpperCase)
    System.out.println(s"output_dir_prefix: $output_dir_prefix")
    System.out.println("Writing view table".toUpperCase)
*/
    sc.stop()

  }
}
