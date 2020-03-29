package ru.main.activity

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CloudLab {
  // разбить текст на слова и удалить пустые строки
  def readRDD (path: String, spark: SparkSession)= {
    spark.sparkContext.textFile(path).map(x=> x.trim.replaceAll(" +", " ")).filter(x=> !x.equals("")).flatMap(x => x.trim.replaceAll(" +", " ").split(" "))
  }
  // посчитать количество вхождений заданного слова;
  def countWord(word: String, rdd: RDD[String]) = {
    val count = rdd.countByValue().get(word)
    if (count == None) {
      "слово не найдено"
    }
    else (
      count.toString
    )
  }
  // найти заданное слово
  def findWord(word: String, rdd: RDD[String]) ={
    var x = 0
    val filtered = rdd.map(y=> {
      x += 1
      (x, y)
    }).filter((x => x._2 == word))
    if (filtered.isEmpty()){
      "слово не найдено"
    }
    else
      filtered.first()._1.toString
  }
}
