import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

class vectorized {
  import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
  import org.apache.spark.ml.linalg.{SparseVector, Vectors}

  def transform(res_t: DataFrame): DataFrame = {
    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("list_url")
      .setOutputCol("domain_feature")
      //.setVocabSize(3)
      //.setMinDF(2)
      .fit(res_t)

    val extract_freq = udf((s1: SparseVector) => {
      s1.toDense.values.toVector
      //s1.values.toVector
    })

    val res = cvModel
      .transform(res_t)
      .withColumn("domain_features", extract_freq(col("domain_feature")))
      .drop(col("list_url"))
      .drop(col("domain_feature"))

    res
  }
}
