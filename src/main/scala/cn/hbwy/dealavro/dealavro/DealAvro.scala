package cn.hbwy.dealavro.dealavro

import java.text.SimpleDateFormat

import cn.hbwy.dealavro.myfileutil.FileUtil
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import log_util.Log4jUtil
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer
import scala.io._

object DealAvro {

  var parameter:Parameter={
    val parameter = new Parameter()
    parameter.initParameter("G:\\Users\\lkz\\IdeaProjects\\dealavro\\src\\main\\scala\\cn\\hbwy\\dealavro\\dealavro\\parameter.properties")
    parameter
  }
  //var schemaBroadcast = Source.fromFile("H:/out/avrotest/EUTRANCELL-Q.avsc").mkString
  var schemaBroadcast = Source.fromFile(parameter.getAvscFilePath).mkString

      var parser = new Schema.Parser
      var schema = parser.parse(schemaBroadcast)
      var recordInjection:Injection[GenericRecord, Array[Byte]]=GenericAvroCodecs.toBinary(schema)

  def main(args: Array[String]): Unit = {
    val log4jPath = parameter.getLog4jPath
    val kafkaParamsPath=parameter.getKafkaParamsPath
    val loadPath=parameter.getLoadPath
    val scan_start_timep=parameter.getScan_start_time
    val suffix=parameter.getSuffix
    val secondsDuration = parameter.getSecondsDuration
    val logger = Log4jUtil.getLogger(log4jPath,DealAvro.getClass)
    val session = SparkSession.builder().appName("KafkaToHi").master("local[3]").getOrCreate()

    val ssc = new StreamingContext(session.sparkContext, Seconds(secondsDuration))
    ssc.sparkContext.getConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //val topic = "testavro1"
    val topic = parameter.getTopics

    val topicSet = topic.split(",").toSet
    //设置kafka参数
   /* val kafkaParams = Map[String, Object](
      //"bootstrap.servers" -> "10.216.9.141:9092,10.216.9.143:9092,10.216.9.144:9092",
      "bootstrap.servers" -> "slaver1:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "group.id" -> "testavro1",
      "auto.offset.reset" -> "earliest", //earliest
      "enable.auto.commit" -> "false" // 是否自动提交偏移量
    )*/
    //val kafkaParams:Map[String,Object]=KafkaParameter.getKafkaParams("G:\\Users\\lkz\\IdeaProjects\\dealavro\\src\\main\\scala\\cn\\hbwy\\dealavro\\dealavro\\kafkaparameter.properties")
   val kafkaParams:Map[String,Object]=KafkaParameter.getKafkaParams(kafkaParamsPath)

    val sc = ssc.sparkContext
    val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, Array[Byte]](topicSet, kafkaParams)) //创建流
    logger.info("    Start Process DealAvro")
    kafkaStream.map[(String,ArrayBuffer[Array[Byte]])](rdd=>{
      val data=rdd.value()
      val fenericRecord: GenericRecord = recordInjection.invert(data).get
      val scan_start_time=fenericRecord.get(scan_start_timep)
      (scan_start_time.toString,ArrayBuffer[Array[Byte]](data))
    }).reduceByKey((data1,data2)=>{
      data1 ++=data2
      data1
    }).foreachRDD(rdd=>{
      rdd.foreach(tuple=>{
        val w1 = new SpecificDatumWriter[GenericRecord](schema)
        val w2 = new DataFileWriter[GenericRecord](w1)
        val tupleTime=dealTime(tuple._1)
        val loadFilePath=loadPath +"/"+ tupleTime + suffix
        val fileLoad = FileUtil.createParentDirAndFile(loadFilePath)
        if(fileLoad.exists()){
          fileLoad.delete()
        }
        w2.create(schema, fileLoad)
        tuple._2.foreach(byteAvro=>{
          val tryGenericRecord=recordInjection.invert(byteAvro)
          w2.append(tryGenericRecord.get)
        })
        w2.close()
        // session.sql("load data local inpath '"+loadFilePath+"' into table t1 partition(deal_date='"+tupleTime+"')")
        fileLoad.delete()

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
