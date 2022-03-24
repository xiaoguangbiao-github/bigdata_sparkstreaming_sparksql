package cn.xiaoguangbiao.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-SQL和DSL两种方式实现WordCount
 */
object Demo05_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._


    //TODO 1.加载数据
    val df: DataFrame = spark.read.text("data/input/words.txt")
    val ds: Dataset[String] = spark.read.textFile("data/input/words.txt")
    df.printSchema()
    df.show()
    ds.printSchema()
    ds.show()
    /*
root
 |-- value: string (nullable = true)

+----------------+
|           value|
+----------------+
|hello me you her|
|   hello you her|
|       hello her|
|           hello|
+----------------+
     */
    //TODO 2.处理数据
    //df.flatMap(_.split(" "))//注意:df没有泛型,不能直接使用split
    val words: Dataset[String] = ds.flatMap(_.split(" "))
    words.printSchema()
    words.show()
    /*
    root
 |-- value: string (nullable = true)

+-----+
|value|
+-----+
|hello|
|   me|
|  you|
|  her|
|hello|
|  you|
|  her|
|hello|
|  her|
|hello|
+-----+
     */
    //TODO ===SQL===
    words.createOrReplaceTempView("t_words")
    val sql:String =
      """
        |select value,count(*) as counts
        |from t_words
        |group by value
        |order by counts desc
        |""".stripMargin
    spark.sql(sql).show()

    //TODO ===DSL===
    words.groupBy('value)
        .count()
        .orderBy('count.desc)
        .show()

    Thread.sleep(1000*600)
    //TODO 3.输出结果
    //TODO 4.关闭资源
    spark.stop()
  }
}
