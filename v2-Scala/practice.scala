package FinalPractice

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object practice {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]");
    val sc = new SparkContext(conf);

    val twitter = sc.textFile("src/main/scala/FinalPractice/input.txt")
      .map(x => x.split(","))
      .map(following => (following(0).trim.toInt, following(1).trim.toInt))

    val influence = twitter
      .map(item => (item._2, 1))
      .reduceByKey((x,y) => x+y)

    val follows10 = twitter
      .filter(item => item._2 == 10)
      .map(item => (item._1, 'x'))

    influence.join(follows10).map(x => (x._1, x._2._1)).sortBy(x => x._1).take(10).foreach(println)







  }


}