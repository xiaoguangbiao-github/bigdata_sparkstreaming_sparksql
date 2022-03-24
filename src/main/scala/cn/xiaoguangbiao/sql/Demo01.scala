package cn.xiaoguangbiao.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL初体验
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //TODO 1.加载数据
    val df1: DataFrame = spark.read.text("data/input/text")
    val df2: DataFrame = spark.read.json("data/input/json")
    val df3: DataFrame = spark.read.csv("data/input/csv")

    //TODO 2.处理数据

    //TODO 3.输出结果
    df1.printSchema()
    df2.printSchema()
    df3.printSchema()
    df1.show()
    df2.show()
    df3.show()

    //TODO 4.关闭资源
    spark.stop()
  }
}
