package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, flatspec}


class MovieRatingTest extends flatspec.AnyFlatSpec with Matchers with BeforeAndAfter {

  implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("MovieRating")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  behavior of "Spark"

  it should "get the data frame" in {
    val movieRating = new MovieRating
    movieRating.getDF
  }

  it should "get the contentRating" in {
    val movieRating = new MovieRating
    movieRating.getContentRating
  }

  it should "get the mean&sdt.dev of imdb_score" in {
    val movieRating = new MovieRating
    movieRating.getMean
  }


}
