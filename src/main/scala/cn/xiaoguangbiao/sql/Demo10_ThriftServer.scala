package cn.xiaoguangbiao.sql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * Author xiaoguangbiao
 * Desc 演示使用jdbc访问SparkSQL的ThriftServer
 */
object Demo10_ThriftServer{
  def main(args: Array[String]): Unit = {
    //0.加载驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    //1.获取连接
    val conn: Connection = DriverManager.getConnection(
      "jdbc:hive2://node2:10000/default", //看上去像是在使用Hive的server2,本质上使用Spark的ThriftServer
      "root",
      "123456"
    )

    //2.编写sql
    val sql = """select id,name,age from person"""

    //3.获取预编译语句对象
    val ps: PreparedStatement = conn.prepareStatement(sql)

    //4.执行sql
    val rs: ResultSet = ps.executeQuery()

    //5.处理结果
    while (rs.next()){
      val id: Int = rs.getInt("id")
      val name: String = rs.getString("name")
      val age: Int = rs.getInt("age")
      println(s"id=${id},name=${name},age=${age}")
    }

    //6.关闭资源
    if(rs != null) rs.close()
    if(ps != null) ps.close()
    if(conn != null) conn.close()
  }
}
