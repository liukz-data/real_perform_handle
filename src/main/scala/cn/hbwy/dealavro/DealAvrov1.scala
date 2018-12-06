/*
package cn.hbwy.dealavro

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.avro.generic.GenericRecord

import scala.io._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.util.AccumulatorV2

object DealAvrov1 {

    def main(args: Array[String]): Unit = {

      val session = SparkSession.builder().appName("KafkaToHi").master("local[2]").getOrCreate()
      println("aaaaaa")
      val ssc = new StreamingContext(session.sparkContext, Seconds(5))
    val schemaBroadcast = Source.fromFile("H:/out/avrotest/EUTRANCELL-Q.avsc").mkString
     //val schema: Schema = new Schema.Parser().parse(new File("H:/out/avrotest/EUTRANCELL-Q.avsc"))
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


     // val w1 = new SpecificDatumWriter[GenericRecord](schema)
     // val w2 = new DataFileWriter[GenericRecord](w1)
      //w2.create(schema,new File("H:/out/avrotest/outavro2.avro"))

      var w2:DataFileWriter[GenericRecord]=null
      var schema:Schema=null
      var i=0
     val sc= ssc.sparkContext
      val i1=sc.longAccumulator("i")
      kafkaStream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()) rdd.foreachPartition(f = partition => {
        for (avroRecord <- partition) {
          val data = avroRecord.value()
          if (i1.value == 0l) {
            println("i:" + i1.value)
            i1.add(1l)
            var parser = new Schema.Parser
            schema = parser.parse(schemaBroadcast)
            val w1 = new SpecificDatumWriter[GenericRecord](schema)
            w2 = new DataFileWriter[GenericRecord](w1)
            w2.create(schema, new File("H:/out/avrotest/outavro2.avro"))
          }

          val fenericRecord: GenericRecord = AvroSerde.deserialize(data, schema)
          //schema = parser.parse(schemaBroadcast)
          val scan_start_time = fenericRecord.get("scan_start_time")
          println(scan_start_time)
          val aaa = dealTime(scan_start_time.toString)
          println("----------------------------------------------")
          println("scan_start_time:" + scan_start_time)
          println("aaa:" + aaa)
          //w2.append(fenericRecord)
        }
      })
      })
      ssc.start()
      ssc.awaitTermination()
    }
    def dealTime(timeString:String):String={
      var d=new Date()
      val a=d.getTime
      var sim =new SimpleDateFormat("yyyy-MMdd-hh:mm:ss")
      val dateFormat=sim.parse(timeString)
      var sim1 =new SimpleDateFormat("yyyyMMddhhmm")
      sim1.format(a)
    }


}
*/
