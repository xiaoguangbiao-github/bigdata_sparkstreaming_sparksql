package cn.xiaoguangbiao.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 使用SparkStreaming接收node1:9999的数据并做WordCount+窗口计算
 * 模拟百度热搜排行榜每隔10s计算最近20s的热搜词
 */
object WordCount05 {
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
      //模拟百度热搜排行榜每隔10s计算最近20s的热搜词Top3
      //windowDuration: Duration,
      //slideDuration: Duration
      .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(10))
      //注意DStream没有提供直接排序的方法,所以需要直接对底层的RDD操作
    //DStream的transform方法表示对DStream底层的RDD进行操作并返回结果
    val sortedResultDS: DStream[(String, Int)] = resultDS.transform(rdd => {
      val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      val top3: Array[(String, Int)] = sortRDD.take(3)
      println("=======top3=====")
      top3.foreach(println)
      println("=======top3=====")
      sortRDD
    })

    //TODO 3.输出结果
    sortedResultDS.print()

    //TODO 4.启动并等待结束
    ssc.start()
    ssc.awaitTermination()//注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来

    //TODO 5.关闭资源
    ssc.stop(stopSparkContext = true, stopGracefully = true)//优雅关闭
  }
}
/*
31省新增本土确诊23例:河北20例 31省新增本土确诊23例:河北20例 31省新增本土确诊23例:河北20例 31省新增本土确诊23例:河北20例
特朗普签令禁止与8款中国应用交易 特朗普签令禁止与8款中国应用交易
纸张价格上涨直逼猪肉 纸张价格上涨直逼猪肉 纸张价格上涨直逼猪肉 纸张价格上涨直逼猪肉 纸张价格上涨直逼猪肉 纸张价格上涨直逼猪肉
多家航空公司发布进出京退改票方案 多家航空公司发布进出京退改票方案 多家航空公司发布进出京退改票方案
石家庄中小学幼儿园暂停线下教学
 */
