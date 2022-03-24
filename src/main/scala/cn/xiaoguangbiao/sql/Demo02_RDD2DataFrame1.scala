package cn.xiaoguangbiao.sql

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-RDD2DataFrame
 */
object Demo02_RDD2DataFrame1 {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //TODO 1.加载数据
    val lines: RDD[String] = sc.textFile("data/input/person.txt")

    //TODO 2.处理数据
    val personRDD: RDD[Person] = lines.map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })

    //RDD-->DF
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF()

    //TODO 3.输出结果
    personDF.printSchema()
    personDF.show()

    //TODO 4.关闭资源
    spark.stop()
  }
  case class Person(id:Int,name:String,age:Int)
}
