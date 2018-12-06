package cn.hbwy.dealavro

import java.io.File
import java.text.SimpleDateFormat
import cn.hbwy.dealavro.myfileutil.FileUtil
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io._

object DealAvrov3 {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("KafkaToHi").master("local[2]").getOrCreate()
    val ssc = new StreamingContext(session.sparkContext, Seconds(5))
    val schemaBroadcast = Source.fromFile("H:/out/avrotest/EUTRANCELL-Q.avsc").mkString
    //val schema: Schema = new Schema.Parser().parse(new File("H:/out/avrotest/EUTRANCELL-Q.avsc"))
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
    val i1 = sc.longAccumulator("i")
    val jl = sc.longAccumulator("j")
    var lastRegion: String = null
    var fileLoad: File = null
    val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, Array[Byte]](topicSet, kafkaParams)) //创建流
    var w2: DataFileWriter[GenericRecord] = null
    var schema: Schema = null

    kafkaStream.foreachRDD(rdd => {
      println("--------------------------------------------------")
      println("执行了。。。。")
      println("--------------------------------------------------")
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(f = partition => {
          for (avroRecord <- partition) {
            val data = avroRecord.value()
            println("if i1.value:" + i1)
            println("if i1.value:" + i1.value)
            if (i1.value == 0) {
              println("i1.value:" + i1.value)
              var parser = new Schema.Parser
              schema = parser.parse(schemaBroadcast)
              val w1 = new SpecificDatumWriter[GenericRecord](schema)
              w2 = new DataFileWriter[GenericRecord](w1)
            }

            val fenericRecord: GenericRecord = AvroSerde.deserialize(data, schema)
            //schema = parser.parse(schemaBroadcast)
            val scan_start_time = fenericRecord.get("scan_start_time")
            val thisRegion = dealTime(scan_start_time.toString)
            if (lastRegion == null) {
              lastRegion = thisRegion
            }
            if (i1.value == 0) {
              println("进来了")
              fileLoad = FileUtil.createParentDirAndFile("H:/out/avrotest/load/outavro2_" + lastRegion + ".avro")
              w2.create(schema, fileLoad)
            }
            println(scan_start_time)
            println("lastRegion:" + lastRegion)
            if (lastRegion != null && lastRegion != thisRegion) {
              lastRegion=thisRegion
              println("thisRegion："+thisRegion)
              w2.close()
              val fileToPare=new File("H:/out/avrotest/praparetohive"+"/"+fileLoad.getName)
              if(fileToPare.exists()){
                fileToPare.delete()
              }

              FileUtils.moveFileToDirectory(fileLoad, new File("H:/out/avrotest/praparetohive"), true)
              fileLoad = FileUtil.createParentDirAndFile("H:/out/avrotest/load/outavro2_" + thisRegion + ".avro")
              val w1 = new SpecificDatumWriter[GenericRecord](schema)
              w2 = new DataFileWriter[GenericRecord](w1)
              w2.create(schema, fileLoad)
              // session.sql("load data local inpath 'H:/out/avrotest/praparetohive/outavro2_" + lastRegion + ".avro' into table t1 partition(deal_date='')")
            }
            println("----------------------------------------------")
            println("scan_start_time:" + scan_start_time)
            ///println("aaa:" + aaa)
            w2.append(fenericRecord)
            i1.add(1l)
          }

        })
      }else{
        println("进来else了")
        println("lastRegion:"+lastRegion)
        println("fileLoad:"+fileLoad)
        println("il:"+i1)
        println("il.value:"+i1.value)
        println("jl:"+jl)
        if(lastRegion!=null&&fileLoad!=null){
          println("进来if了")
          println("jl.value%10："+jl.value%10)
          if(jl.value%10==0){
            w2.close()
            val fileToPare=new File("H:/out/avrotest/praparetohive"+"/"+fileLoad.getName)
            if(fileToPare.exists()){
              fileToPare.delete()
            }
            FileUtils moveFileToDirectory(fileLoad, new File("H:/out/avrotest/praparetohive"), true)
            val w1 = new SpecificDatumWriter[GenericRecord](schema)
            w2 = new DataFileWriter[GenericRecord](w1)
            w2.create(schema, fileLoad)
            //session.sql("load  ")
          }
        }
      }
      jl.add(1)
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
