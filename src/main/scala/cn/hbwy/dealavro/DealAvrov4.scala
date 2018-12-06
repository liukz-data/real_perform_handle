package cn.hbwy.dealavro

import java.text.SimpleDateFormat
import cn.hbwy.dealavro.myfileutil.FileUtil
import com.twitter.bijection.Injection
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.avro.generic.GenericRecord
import scala.io._
import com.twitter.bijection.avro.GenericAvroCodecs
import scala.collection.mutable.ArrayBuffer

object DealAvrov4 {

      var schemaBroadcast = Source.fromFile("H:/out/avrotest/EUTRANCELL-Q.avsc").mkString
      var parser = new Schema.Parser
      var schema = parser.parse(schemaBroadcast)
      val recordInjection:Injection[GenericRecord, Array[Byte]]=GenericAvroCodecs.toBinary(schema)

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("KafkaToHi").master("local[3]").getOrCreate()
    val ssc = new StreamingContext(session.sparkContext, Seconds(5))
    val topic = "testavro1"
    val topicSet = topic.split(",").toSet
    //设置kafka参数
    val kafkaParams = Map[String, Object](
      //"bootstrap.servers" -> "10.216.9.141:9092,10.216.9.143:9092,10.216.9.144:9092",
      "bootstrap.servers" -> "slaver1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "testavro1",
      "auto.offset.reset" -> "earliest", //earliest
      "enable.auto.commit" -> "false" // 是否自动提交偏移量
    )
    val sc = ssc.sparkContext
    val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, Array[Byte]](topicSet, kafkaParams)) //创建流

    kafkaStream.map[(String,ArrayBuffer[Array[Byte]])](rdd=>{
      val data=rdd.value()
      val fenericRecord: GenericRecord = recordInjection.invert(data).get
      val scan_start_time=fenericRecord.get("scan_start_time")
      (scan_start_time.toString,ArrayBuffer[Array[Byte]](data))
    }).reduceByKey((data1,data2)=>{
      data1 ++=data2
      data1
    }).foreachRDD(rdd=>{
      rdd.foreach(tuple=>{
        val w1 = new SpecificDatumWriter[GenericRecord](schema)
        val w2 = new DataFileWriter[GenericRecord](w1)
        val tupleTime=dealTime(tuple._1)
        val fileLoad = FileUtil.createParentDirAndFile("H:/out/avrotest/load/outavro2_" + tupleTime + ".avro")
        if(fileLoad.exists()){
          fileLoad.delete()
        }
        w2.create(schema, fileLoad)
        tuple._2.foreach(byteAvro=>{
          val tryGenericRecord=recordInjection.invert(byteAvro)
          w2.append(tryGenericRecord.get)
        })
        w2.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def dealTime(timeString: String): String = {
    var sim = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val dateFormat = sim.parse(timeString)
    var sim1 = new SimpleDateFormat("yyyyMMddHHmm")
    sim1.format(dateFormat)
  }


}
