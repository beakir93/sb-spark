
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions._

val conf = new SparkConf().setAppName("lab3")
                            .setAll(Map("spark.cassandra.connection.host" -> "10.0.5.13",
                                        "spark.cassandra.connection.port" -> "9042",
                                        "spark.cassandra.auth.username" -> "kirill_likhouzov",
                                        "spark.cassandra.auth.password" -> ""))

val sparkSession = SparkSession.builder()
  .config(conf=conf)
  //.enableHiveSupport()
  .getOrCreate()

var sc = sparkSession.sparkContext
val sqlContext = new SQLContext(sc)

%AddJar file:///data/home/kirill.likhouzov/Drivers/postgresql-42.2.12.jar
%AddJar file:///data/home/kirill.likhouzov/Drivers/spark-cassandra-connector_2.11-2.4.3.jar
%AddJar file:///data/home/kirill.likhouzov/Drivers/elasticsearch-spark-20_2.11-7.6.2.jar

val weblogs = spark
                .read
                .json("/labs/laba03/weblogs.json")
                .repartition(1)
                .cache()

weblogs.count()

weblogs.show(1, 1000, true)

val domain_cats = spark
                    .read
                    .format("jdbc")
                    .option("url", "jdbc:postgresql://10.0.5.13:5432/labdata")
                    .option("dbtable", "domain_cats")
                    .option("user", "kirill_likhouzov")
                    .option("password", "")
                    .option("driver", "org.postgresql.Driver")
                    .load()
                    .repartition(1)
                    .cache()

domain_cats.count()

domain_cats.show(1, false)

val visits = spark
            .read
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes.wan.only","true")
            .option("es.nodes", "10.0.5.13")
            .option("es.port", "9200")
            .load("visits")
            .repartition(1)
            .cache()

visits.count()

visits.show(1, false)

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

val connector = CassandraConnector(sc.getConf)

val clients= spark.read
                .format("org.apache.spark.sql.cassandra")
                .options(Map("table" -> "clients", "keyspace" -> "labdata"))
                .load()
                .repartition(1)
                .cache()

clients.count()

clients.show(1, false)

val clients_ages = clients.withColumn("age_category",  when($"age" >= 18 && $"age" <= 24, "18-24")
                                                      .when($"age" >= 25 && $"age" <= 34, "25-34")
                                                      .when($"age" >= 35 && $"age" <= 44, "35-44")
                                                      .when($"age" >= 45 && $"age" <= 54, "45-54")
                                                      .when($"age" >= 55, ">=55")
                                                      .otherwise(""))
clients_ages.show(1, false)

val visits_cl = clients_ages.as("a")
                    .join(visits.as("b"), $"a.uid" === $"b.uid", "inner")
                    .select($"a.uid"
                            ,$"a.gender"
                            ,$"a.age_category"
                            ,concat(lit("web_"), lower(regexp_replace($"b.category", "-", "_"))).as("web_category")
                            ,$"b.timestamp")
                    .filter(!$"web_category".isNull)
                    .cache()

visits_cl.show(1, false)

val visits_web = 
    visits_cl
        .groupBy("uid")
        .pivot("web_category")
        .count()
        
visits_web.show(1, 100, true)

val weblogs_explode = weblogs
                            .select($"uid"
                                    ,explode($"visits").as("web"))
                            .cache()

weblogs_explode

val weblogs_url = weblogs_explode
                            .withColumn("url", weblogs_explode("web.url"))
                            .withColumn("timestamp", weblogs_explode("web.timestamp"))
                            .withColumn("host", callUDF("parse_url", $"url", lit("HOST")))
                            .select($"uid", $"url", $"host", $"timestamp")
                            .cache()

weblogs_url.show(1, 1000, true)

val weblogs_cl = clients_ages.as("a")
                    .join(weblogs_url.as("b"), $"a.uid" === $"b.uid", "inner")
                    .select($"a.uid"
                            ,$"a.gender"
                            ,$"a.age_category"
                            ,$"b.host"
                            ,$"b.timestamp")
                    .cache()

weblogs_cl.show(1, false)

val weblogs_cl_cat = weblogs_cl.as("a")
                        .join(domain_cats.as("b"), regexp_replace($"a.host", "www.", "") === $"b.domain", "left")
                        .select($"a.uid"
                                ,$"a.gender"
                                ,$"a.age_category"
                                ,$"a.host"
                                ,regexp_replace($"a.host", "www.", "").as("host_not_www")
                                ,$"a.timestamp"
                                ,concat(lit("shop_"), lower(regexp_replace($"b.category", "-", "_"))).as("shop_category"))
                        .filter(!$"shop_category".isNull)
                        .cache()

weblogs_cl_cat.show(5, false)

// // check join web category

// weblogs_cl_cat.filter($"category".isNull).select($"host").distinct().show(1, false)

// domain_cats.registerTempTable("domain_cats_tbl")

// sqlContext.sql("""select * from domain_cats_tbl where lower(domain) like '%velomotion.ru%'""").show(100)

val weblogs_web = 
    weblogs_cl_cat
        .groupBy("uid")
        .pivot("shop_category")
        .count()

weblogs_web.show(1, 100, true)

clients_ages.select($"uid").distinct().count() == clients_ages.count()

val res = clients_ages.as("a")
                        .join(visits_web.as("b"), $"a.uid" === $"b.uid", "left")
                        .join(weblogs_web.as("c"), $"a.uid" === $"c.uid", "left")
                        .select($"a.uid"
                                ,$"a.gender"
                                ,$"a.age_category".as("age_cat")
                                ,$"b.*"
                                ,$"c.*")
                        .drop($"b.uid")
                        .drop($"c.uid")
                        .cache()
                                
res.show(1, 1000, true)

res.count()

res.filter($"uid" === "d50e23dc-0cbd-4883-bd6c-23ce65b511cc").show(3, 1000, true)

res
    .write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.5.13:5432/kirill_likhouzov")
    .option("dbtable", "clients")
    .option("user", "kirill_likhouzov")
    .option("password", "")
    .option("driver", "org.postgresql.Driver")
    .save()

sc.stop()

/* 
spark-shell --driver-class-path /data/home/kirill.likhouzov/Drivers/postgresql-42.2.12.jar --jars /data/home/kirill.likhouzov/Drivers/postgresql-42.2.12.jar

val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:postgresql://10.0.5.13:5432/labdata").option("dbtable", "domain_cats").option("user", "kirill_likhouzov").option("password", "").load()

jdbcDF.write.format("orc").save("domain_cats.orc")

val domain_cats = sqlContext.read.format("orc").load("/user/kirill.likhouzov/domain_cats.orc")
/*

/*
spark-shell --driver-class-path /data/home/kirill.likhouzov/Drivers/elasticsearch-spark-20_2.11-7.6.2.jar --jars /data/home/kirill.likhouzov/Drivers/elasticsearch-spark-20_2.11-7.6.2.jar

val visits = spark.read.format("org.elasticsearch.spark.sql").option("es.nodes.wan.only","true").option("es.nodes", "10.0.5.13").option("es.port", "9200").load("visits")

visits.write.format("orc").save("visits.orc")

val visits = sqlContext.read.format("orc").load("/user/kirill.likhouzov/visits.orc")
*/
