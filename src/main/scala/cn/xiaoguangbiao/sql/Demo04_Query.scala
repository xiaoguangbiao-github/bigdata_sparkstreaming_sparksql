package cn.xiaoguangbiao.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示SparkSQL-SQL和DSL两种方式实现各种查询
 */
object Demo04_Query {
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
    personDF.printSchema()
    personDF.show()
    /*
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)

+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|zhangsan| 20|
|  2|    lisi| 29|
|  3|  wangwu| 25|
|  4| zhaoliu| 30|
|  5|  tianqi| 35|
|  6|    kobe| 40|
+---+--------+---+
     */

    //TODO ===========SQL==============
    //注册表名
    //personDF.registerTempTable("")//过期的
    //personDF.createOrReplaceGlobalTempView("")//创建全局的,夸SparkSession也可以用,但是生命周期太长!
    personDF.createOrReplaceTempView("t_person")//创建临时的,当前SparkSession也可以用

    //=1.查看name字段的数据
    spark.sql("select name from t_person").show()
    //=2.查看 name 和age字段数据
    spark.sql("select name,age from t_person").show()
    //=3.查询所有的name和age，并将age+1
    spark.sql("select name,age,age+1 from t_person").show()
    //=4.过滤age大于等于25的
    spark.sql("select name,age from t_person where age >= 25").show()
    //=5.统计年龄大于30的人数
    spark.sql("select count(*) from t_person where age > 30").show()
    //=6.按年龄进行分组并统计相同年龄的人数
    spark.sql("select age,count(*) from t_person group by age").show()
    //=7.查询姓名=张三的
    spark.sql("select name from t_person where name = 'zhangsan'").show()

    //TODO ===========DSL:面向对象的SQL==============
    //=1.查看name字段的数据
    //personDF.select(personDF.col("name"))
    personDF.select("name").show()
    //=2.查看 name 和age字段数据
    personDF.select("name","age").show()
    //=3.查询所有的name和age，并将age+1
    //personDF.select("name","age","age+1").show()//错误的:cannot resolve '`age+1`' given input columns: [age, id, name];;
    //注意$是把字符串转为了Column列对象
    personDF.select($"name",$"age",$"age" + 1).show()
    //注意'是把列名转为了Column列对象
    personDF.select('name,'age,'age + 1).show()
    //=4.过滤age大于等于25的
    personDF.filter("age >= 25").show()
    personDF.filter($"age" >= 25).show()
    personDF.filter('age >= 25).show()
    //=5.统计年龄大于30的人数
    val count: Long = personDF.where('age > 30).count() //where底层filter
    println("年龄大于30的人数为:"+count)
    //=6.按年龄进行分组并统计相同年龄的人数
    personDF.groupBy('age).count().show()
    //=7.查询姓名=张三的
    personDF.filter("name = 'zhangsan'").show()
    personDF.filter($"name"==="zhangsan").show()
    personDF.filter('name ==="zhangsan").show()
    personDF.filter('name =!="zhangsan").show()

    //TODO 3.输出结果
    //TODO 4.关闭资源
    spark.stop()
  }
  case class Person(id:Int,name:String,age:Int)
}
