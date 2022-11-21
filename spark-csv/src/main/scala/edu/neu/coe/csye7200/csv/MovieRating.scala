package edu.neu.coe.csye7200.csv

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

class MovieRating{

  val spark = SparkSession
    .builder()
    .appName("Movie Rating")
    .config("spark.some.config.option","some-value")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val path = "src/main/resources/movie_metadata.csv"


  val df = spark.read.option("header","true").csv(path)


  def getDF(): sql.DataFrame = {
    df.show()
    df
  }

  def getContentRating: sql.DataFrame = {
    val col = df.select("content_rating").na.drop()
    col.show()
    col
  }

  def getMean: sql.DataFrame = {
    val col = df.describe("imdb_score")
    col.show()
    col
  }



}


