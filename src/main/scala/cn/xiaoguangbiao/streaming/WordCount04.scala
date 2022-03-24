package cn.xiaoguangbiao.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 使用SparkStreaming接收node1:9999的数据并做WordCount+窗口计算
 * 每隔5s计算最近10s的数据
 */
object WordCount04 {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //the time interval at which streaming data will be divided into batches
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))//每隔5s划分一个批次

    //TODO 1.加载数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)

    //TODO 2.处理数据
    val resultDS: DStream[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))
      //.reduceByKey(_ + _)
      //   windowDuration :窗口长度/窗口大小,表示要计算最近多长时间的数据
      //   slideDuration : 滑动间隔,表示每隔多长时间计算一次
      //   注意:windowDuration和slideDuration必须是batchDuration的倍数
      //  每隔5s(滑动间隔)计算最近10s(窗口长度/窗口大小)的数据
      //reduceByKeyAndWindow(聚合函数,windowDuration,slideDuration)
        //.reduceByKeyAndWindow(_+_,Seconds(10),Seconds(5))
      .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(5))
    //实际开发中需要我们掌握的是如何根据需求设置windowDuration和slideDuration
    //如:
    //每隔10分钟(滑动间隔slideDuration)更新最近24小时(窗口长度windowDuration)的广告点击数量
    // .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Minutes(60*24),Minutes(10))

    //TODO 3.输出结果
    resultDS.print()

    //TODO 4.启动并等待结束
    ssc.start()
    ssc.awaitTermination()//注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来

    //TODO 5.关闭资源
    ssc.stop(stopSparkContext = true, stopGracefully = true)//优雅关闭
  }
}
