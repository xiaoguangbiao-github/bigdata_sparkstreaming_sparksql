package cn.xiaoguangbiao.sql


import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-完成电影数据分析
 */
object Demo07_MovieDataAnalysis {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //TODO 1.加载数据
    val ds: Dataset[String] = spark.read.textFile("data/input/rating_100k.data")

    //TODO 2.处理数据
    val movieDF: DataFrame = ds.map(line => {
      val arr: Array[String] = line.split("\t")
      (arr(1), arr(2).toInt)
    }).toDF("movieId", "score")
    movieDF.printSchema()
    movieDF.show()
    /*
   +-------+-----+
  |movieId|score|
  +-------+-----+
  |    242|    3|
  |    302|    3|
     */

    //需求:统计评分次数>200的电影平均分Top10

    //TODO ======SQL
    //注册表
    movieDF.createOrReplaceTempView("t_movies")
    val sql: String =
      """
        |select movieId,avg(score) as avgscore,count(*) as counts
        |from t_movies
        |group by movieId
        |having counts > 200
        |order by avgscore desc
        |limit 10
        |""".stripMargin
    spark.sql(sql).show()
    /*
    +-------+------------------+------+
    |movieId|          avgscore|counts|
    +-------+------------------+------+
    |    318| 4.466442953020135|   298|
    |    483|  4.45679012345679|   243|
    |     64| 4.445229681978798|   283|
    |    603|4.3875598086124405|   209|
  .....
     */


    //TODO ======DSL
    import org.apache.spark.sql.functions._
    movieDF.groupBy('movieId)
      .agg(
        avg('score) as "avgscore",
        count("movieId") as "counts"
      ).filter('counts > 200)
      .orderBy('avgscore.desc)
      .limit(10)
      .show()

    //TODO 3.输出结果
    //TODO 4.关闭资源
    spark.stop()
  }
}
