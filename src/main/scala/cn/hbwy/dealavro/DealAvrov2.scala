package cn.hbwy.dealavro

import java.io.File
import java.text.SimpleDateFormat
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io._

object DealAvrov2{
  val schemaStr = Source.fromFile("H:/out/avrotest/EUTRANCELL-Q.avsc").mkString
  var parser = new Schema.Parser
  val schema = parser.parse(schemaStr)
  val w1 = new SpecificDatumWriter[GenericRecord](schema)
 val  w2 = new DataFileWriterImpl[GenericRecord](w1)
  //val w2Broadcast=sc.broadcast[DataFileWriter[GenericRecord]](w2)
  val dataFileWriter=w2.create(schema, new File("H:/out/avrotest/outavro2.avro"))
    def main(args: Array[String]): Unit = {
      val session = SparkSession.builder().appName("KafkaToHi").master("local[2]").getOrCreate()
      println("aaaaaa")
      val ssc = new StreamingContext(session.sparkContext, Seconds(5))
    //val schemaStr = Source.fromFile("H:/out/avrotest/EUTRANCELL-Q.avsc").mkString
     //val schema: Schema = new Schema.Parser().parse(new File("H:/out/avrotest/EUTRANCELL-Q.avsc"))
      val topic = "testavro1"
      val topicSet  = topic.split(",").toSet
      //设置kafka参数
      val kafkaParams = Map[String, Object](
        //"bootstrap.servers" -> "10.216.9.141:9092,10.216.9.143:9092,10.216.9.144:9092",
        "bootstrap.servers" -> "slaver1:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[ByteArrayDeserializer],
        "group.id" -> "testavro2_consumer",
        "auto.offset.reset" -> "earliest",  //earliest
        "enable.auto.commit" -> "false"// 是否自动提交偏移量
      )

      val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte]](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, Array[Byte]](topicSet, kafkaParams)) //创建流

      //var w2:DataFileWriter[GenericRecord]=null
      //var schema:Schema=null
      //var i=0
     val sc= ssc.sparkContext
      /*var parser = new Schema.Parser
      schema = parser.parse(schemaStr)
      val schemaBroadcast=sc.broadcast[Schema](schema)
      val w1 = new SpecificDatumWriter[GenericRecord](schema)
      w2 = new DataFileWriterImpl[GenericRecord](w1)
      //val w2Broadcast=sc.broadcast[DataFileWriter[GenericRecord]](w2)
      val dataFileWriter=w2.create(schema, new File("H:/out/avrotest/outavro2.avro"))
      val w2Broadcast=sc.broadcast[DataFileWriter[GenericRecord]](dataFileWriter)*/
     // val i1=sc.longAccumulator("i")
      kafkaStream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()) rdd.foreachPartition(f = partition => {
        for (avroRecord <- partition) {
          val data = avroRecord.value()
         /* /*val data = avroRecord.value()
          val schemaV=schemaBroadcast.value
          println("data:"+data)
          println("schemaV:"+schemaV)*/
          val w2V=w2Broadcast.value*/
          val fenericRecord: GenericRecord = AvroSerde.deserialize(data, schema)
          //schema = parser.parse(schemaBroadcast)
          val scan_start_time = fenericRecord.get("scan_start_time")
          println(scan_start_time)
          val aaa = dealTime(scan_start_time.toString)
          println("----------------------------------------------")
          println("scan_start_time:" + scan_start_time)
          println("aaa:" + aaa)
          //w2V.append(fenericRecord)
        }
      })
      })
      ssc.start()
      ssc.awaitTermination()
    }
    def dealTime(timeString:String):String={
      var sim =new SimpleDateFormat("yyyy-MM-dd hh:mm")
      val dateFormat=sim.parse(timeString)
      var sim1 =new SimpleDateFormat("yyyyMMddhhmm")
      sim1.format(dateFormat)
    }


}
