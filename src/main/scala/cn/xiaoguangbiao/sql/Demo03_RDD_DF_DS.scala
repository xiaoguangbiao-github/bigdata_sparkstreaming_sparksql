package cn.xiaoguangbiao.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-RDD_DF_DS相互转换
 */
object Demo03_RDD_DF_DS {
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

    //转换1:RDD-->DF
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF()
    //转换2:RDD-->DS
    val personDS: Dataset[Person] = personRDD.toDS()
    //转换3:DF-->RDD,注意:DF没有泛型,转为RDD时使用的是Row
    val rdd: RDD[Row] = personDF.rdd
    //转换4:DS-->RDD
    val rdd1: RDD[Person] = personDS.rdd
    //转换5:DF-->DS
    val ds: Dataset[Person] = personDF.as[Person]
    //转换6:DS-->DF
    val df: DataFrame = personDS.toDF()



    //TODO 3.输出结果
    personDF.printSchema()
    personDF.show()
    personDS.printSchema()
    personDS.show()
    rdd.foreach(println)
    rdd1.foreach(println)

    //TODO 4.关闭资源
    spark.stop()
  }
  case class Person(id:Int,name:String,age:Int)
}
