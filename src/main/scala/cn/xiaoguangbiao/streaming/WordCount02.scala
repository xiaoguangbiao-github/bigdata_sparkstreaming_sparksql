package cn.xiaoguangbiao.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 使用SparkStreaming接收node1:9999的数据并做WordCount+实现状态管理:
 * 如输入spark hadoop 得到(spark,1),(hadoop,1)
 * 再下一个批次在输入 spark spark,得到(spark,3)
 */
object WordCount02 {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //the time interval at which streaming data will be divided into batches
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5)) //每隔5s划分一个批次

    //The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint().
    //注意:state存在checkpoint中
    ssc.checkpoint("./ckp")

    //TODO 1.加载数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)

    //TODO 2.处理数据
    //定义一个函数用来处理状态:把当前数据和历史状态进行累加
    //currentValues:表示该key(如:spark)的当前批次的值,如:[1,1]
    //historyValue:表示该key(如:spark)的历史值,第一次是0,后面就是之前的累加值如1
    val updateFunc = (currentValues: Seq[Int], historyValue: Option[Int]) => {
      if (currentValues.size > 0) {
        val currentResult: Int = currentValues.sum + historyValue.getOrElse(0)
        Some(currentResult)
      } else {
        historyValue
      }
    }

    val resultDS: DStream[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))
      //.reduceByKey(_ + _)
      // updateFunc: (Seq[V], Option[S]) => Option[S]
      .updateStateByKey(updateFunc)

    //TODO 3.输出结果
    resultDS.print()

    //TODO 4.启动并等待结束
    ssc.start()
    ssc.awaitTermination() //注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来

    //TODO 5.关闭资源
    ssc.stop(stopSparkContext = true, stopGracefully = true) //优雅关闭
  }
}
