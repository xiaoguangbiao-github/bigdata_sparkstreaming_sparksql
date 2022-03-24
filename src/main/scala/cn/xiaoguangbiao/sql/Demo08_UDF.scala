package cn.xiaoguangbiao.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-使用SparkSQL-UDF将数据转为大写
 */
object Demo08_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //TODO 1.加载数据
    val ds: Dataset[String] = spark.read.textFile("data/input/udf.txt")
    ds.printSchema()
    ds.show()
    /*
+-----+
|value|
+-----+
|hello|
| haha|
| hehe|
| xixi|
+-----`
     */
    //TODO 2.处理数据
    //需求:使用SparkSQL-UDF将数据转为大写
    //TODO ======SQL
    //TODO 自定义UDF函数
    spark.udf.register("small2big",(value:String)=>{
      value.toUpperCase()
    })
    ds.createOrReplaceTempView("t_word")
    val sql:String =
      """
        |select value,small2big(value) as bigValue
        |from t_word
        |""".stripMargin
    spark.sql(sql).show()
    /*
    +-----+--------+
    |value|bigValue|
    +-----+--------+
    |hello|   HELLO|
    | haha|    HAHA|
    | hehe|    HEHE|
    | xixi|    XIXI|
    +-----+--------+
     */

    //TODO ======DSL
    //TODO 自定义UDF函数
    import org.apache.spark.sql.functions._
    val small2big2: UserDefinedFunction = udf((value:String)=>{
      value.toUpperCase()
    })
    ds.select('value,small2big2('value).as("bigValue")).show()
    /*
      +-----+--------+
      |value|bigValue|
      +-----+--------+
      |hello|   HELLO|
      | haha|    HAHA|
      | hehe|    HEHE|
      | xixi|    XIXI|
      +-----+--------+
       */

    //TODO 3.输出结果
    //TODO 4.关闭资源
    spark.stop()
  }
}
