package thoughtworks.wordcount
import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession): Dataset[String] = {
      import spark.implicits._

      // Split on all non-words that are also not apostrophe
      val regex: String = "(?!')\\W+"

      dataSet.flatMap(_.split(regex)).map(_.trim.toLowerCase).filter(_.nonEmpty)
    }
    def countByWord(spark: SparkSession): Dataset[(String, Int)] = {
      import spark.implicits._
      dataSet.map((_, 1))
        .groupByKey(_._1)
        .reduceGroups((a, b) => (a._1, a._2 + b._2))
        .map(_._2)
        .orderBy("_1")
        .withColumnRenamed("_1","word")
        .withColumnRenamed("_2","count")
        .as[(String, Int)]
    }
  }
}
