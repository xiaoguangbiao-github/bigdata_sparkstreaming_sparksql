package cn.xiaoguangbiao.sql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-支持的外部数据源
 * 支持的文件格式:text/json/csv/parquet/orc....
 * 支持文件系统/数据库
 */
object Demo06_DataSource {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //TODO 1.加载数据
    val df: DataFrame = spark.read.json("data/input/json")//底层format("json").load(paths : _*)
    //val df: DataFrame = spark.read.csv("data/input/csv")//底层format("csv").load(paths : _*)
    df.printSchema()
    df.show()
    //TODO 2.处理数据
    //TODO 3.输出结果
    df.coalesce(1).write.mode(SaveMode.Overwrite).json("data/output/json")//底层 format("json").save(path)
    df.coalesce(1).write.mode(SaveMode.Overwrite).csv("data/output/csv")
    df.coalesce(1).write.mode(SaveMode.Overwrite).parquet("data/output/parquet")
    df.coalesce(1).write.mode(SaveMode.Overwrite).orc("data/output/orc")
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    df.coalesce(1).write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","person",prop)//表会自动创建

    //TODO 4.关闭资源
    spark.stop()
  }
}
