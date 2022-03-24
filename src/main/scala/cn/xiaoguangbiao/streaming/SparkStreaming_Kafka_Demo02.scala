package cn.xiaoguangbiao.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示使用spark-streaming-kafka-0-10_2.12中的Direct模式连接Kafka消费数据+手动提交offset
 */
object SparkStreaming_Kafka_Demo02 {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //the time interval at which streaming data will be divided into batches
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5)) //每隔5s划分一个批次
    ssc.checkpoint("./ckp")

    //TODO 1.加载数据-从Kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092", //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer], //key的反序列化规则
      "value.deserializer" -> classOf[StringDeserializer], //value的反序列化规则
      "group.id" -> "sparkdemo", //消费者组名称
      //earliest:表示如果有offset记录从offset记录开始消费,如果没有从最早的消息开始消费
      //latest:表示如果有offset记录从offset记录开始消费,如果没有从最后/最新的消息开始消费
      //none:表示如果有offset记录从offset记录开始消费,如果没有就报错
      "auto.offset.reset" -> "latest",
      //"auto.commit.interval.ms"->"1000",//自动提交的时间间隔
      "enable.auto.commit" -> (false: java.lang.Boolean) //是否自动提交
    )
    val topics = Array("spark_kafka") //要订阅的主题
    //使用工具类从Kafka中消费消息
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //位置策略,使用源码中推荐的
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) //消费策略,使用源码中推荐的
    )

    //TODO 2.处理消息
    //注意提交的时机:应该是消费完一小批就该提交一次offset,而在DStream一小批的体现是RDD
    kafkaDS.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        //消费
        rdd.foreach(record => {
          val topic: String = record.topic()
          val partition: Int = record.partition()
          val offset: Long = record.offset()
          val key: String = record.key()
          val value: String = record.value()
          val info: String = s"""topic:${topic}, partition:${partition}, offset:${offset}, key:${key}, value:${value}"""
          println("消费到的消息的详细信息为: "+info)
        })
        //获取rdd中offset相关的信息:offsetRanges里面就包含了该批次各个分区的offset信息
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //提交
        kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println("当前批次的数据已消费并手动提交")
      }
    })

    //TODO 3.输出结果

    //TODO 4.启动并等待结束
    ssc.start()
    ssc.awaitTermination() //注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来

    //TODO 5.关闭资源
    ssc.stop(stopSparkContext = true, stopGracefully = true) //优雅关闭
  }
}

//测试:
//1.准备kafka
// /export/server/kafka/bin/kafka-topics.sh --list --zookeeper node1:2181
// /export/server/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --replication-factor 1 --partitions 3 --topic spark_kafka
// /export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic spark_kafka
//2.启动程序
//3.发送数据
//4.观察结果