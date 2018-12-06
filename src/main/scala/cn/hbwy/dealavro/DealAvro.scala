/*
package cn.hbwy.dealavro

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.io._
object DealAvro {

    def main(args: Array[String]): Unit = {

      val session = SparkSession.builder().appName("KafkaToHi").master("local[2]").getOrCreate()

      val ssc = new StreamingContext(session.sparkContext, Seconds(5))
      val schemaBroadcast = Source.fromFile("H:/out/avrotest/EUTRANCELL-Q.avsc").mkString
      val topic = "EUTRANCELL-Q"
      val topicSet  = topic.split(",").toSet
      //设置kafka参数
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "10.216.9.141:9092,10.216.9.143:9092,10.216.9.144:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[ByteArrayDeserializer],
        "group.id" -> "wy_middle_test",
        "auto.offset.reset" -> "earliest",  //earliest
        "enable.auto.commit" -> "false"// 是否自动提交偏移量
      )

      val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte]](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, Array[Byte]](topicSet, kafkaParams)) //创建流


      kafkaStream.foreachRDD(rdd => {
        //获取偏移量
        //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //处理逻辑
        if(!rdd.isEmpty()){
          rdd.foreachPartition(partition => {
            for (avroRecord <- partition) {
              val data = avroRecord.value()
              val parser = new Schema.Parser
              val schema = parser.parse(schemaBroadcast) // ②解析为 Avro Schema 对象
              //print(schema)
              val record = AvroSerde.deserialize(data, schema) // ③反序列化为 GenericRecord 对象
                val scan_start_time=record.get("scan_start_time")
              println(scan_start_time)
              //            print(record.get("scan_start_time") + "," + record.get("timestamp") + "," + record.get("insert_time") + "\n")
            }
          })
        }
        //异步的将偏移量跟新到Kafka中
        //      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })
      ssc.start()
      ssc.awaitTermination()
    }



}
*/
