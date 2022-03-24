package cn.xiaoguangbiao.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-使用SparkSQL-UDF将数据转为大写
 */
object Demo09_Hive {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境---需要增加参数配置和开启hivesql语法支持
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse")//指定Hive数据库在HDFS上的位置
      .config("hive.metastore.uris", "thrift://node2:9083")
      .enableHiveSupport()//开启对hive语法的支持
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //TODO 1.操作Hive
    spark.sql("show databases").show(false)
    spark.sql("show tables").show(false)
    spark.sql("CREATE TABLE person4 (id int, name string, age int) row format delimited fields terminated by ' '")
    spark.sql("LOAD DATA LOCAL INPATH 'file:///D:/person.txt' INTO TABLE person4")
    spark.sql("show tables").show(false)
    spark.sql("select * from person4").show(false)

    spark.stop()
  }
}
