package edu.neu.coe.csye7200.csv

import org.apache.spark
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class DataFormTest extends AnyFlatSpec with Matchers{
  val spark: SparkSession = SparkSession
    .builder()
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.


  behavior of "getDataForm"
  it should "get movie_metadata.csv" in {
    val path = "src/main/resources/movie_metadata.csv"
    val df = spark.read.option("delimiter", ",").option("header", "true").csv(path)
    val mean = df.describe("imdb_score").show()
    df.show()
  }
}
