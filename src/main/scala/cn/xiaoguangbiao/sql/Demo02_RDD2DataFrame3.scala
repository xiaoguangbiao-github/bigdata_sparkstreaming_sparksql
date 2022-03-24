package cn.xiaoguangbiao.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-RDD2DataFrame-自定义Schema
 */
object Demo02_RDD2DataFrame3 {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //TODO 1.加载数据
    val lines: RDD[String] = sc.textFile("data/input/person.txt")

    //TODO 2.处理数据
    val rowRDD: RDD[Row] = lines.map(line => {
      val arr: Array[String] = line.split(" ")
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })

    //RDD-->DF
    import spark.implicits._
    /*val schema: StructType = StructType(
          StructField("id", IntegerType, false) ::
          StructField("name", StringType, false) ::
          StructField("age", IntegerType, false) :: Nil)*/
    val schema: StructType = StructType(List(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("age", IntegerType, false)
    ))

    val personDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    //TODO 3.输出结果
    personDF.printSchema()
    personDF.show()

    //TODO 4.关闭资源
    spark.stop()
  }

}
