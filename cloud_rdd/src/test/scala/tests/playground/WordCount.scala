package tests.playground

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import ru.main.activity.CloudLab

class WordCount extends FunSuite with BeforeAndAfter{


  private  implicit var sparkSession: SparkSession = _

  before {
    sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
  }

  test("testing the work functions") {

    val path = "D:\\intellij projects\\cloud_rdd\\src\\main\\resources\\input\\polyakov.txt"

    val rdd = CloudLab.readRDD(path, sparkSession)
    // rdd.foreach(println)

    val count = CloudLab.countWord("boop", rdd)
    //println(count)

    val wordNumber = CloudLab.findWord("dumb", rdd)
    //println(wordNumber)
  }
  test ("testing map and reduce"){
    val data1 = Array(1, 2, 5, 4, 5, 8, 8, 8, 9, 10, 1, 1, 1, 1, 11, 12, 14, 15, 16, 78, 1)

    val rdd1 = sparkSession.sparkContext.parallelize(data1)
    val keyValRdd = rdd1.map(x => (x, 1))
    val countRdd = keyValRdd.reduceByKey((x, y) => x + y)

    val data2 = Array("banana", "key", "question", "answer", "bomb", "knife", "ra-pa-pa","tanh", "tends")
    val rdd2 = sparkSession.sparkContext.parallelize(data2)
    val str = rdd2.reduce((x,y) => x + " || " + y)

  }
}
