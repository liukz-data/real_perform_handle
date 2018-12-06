package cn.hbwy.dealavro.dealavro

import java.io.FileInputStream
import java.util.Properties

import scala.collection.JavaConverters._

object KafkaParameter {

  def getKafkaParams(kafkaParamsPath: String): Map[String, Object] = {
    //FileInputStream fileIn = new FileInputStream("G:\\Users\\lkz\\IdeaProjects\\dealavro\\src\\main\\scala\\cn\\hbwy\\dealavro\\dealavro\\kafkaparameter.properties");
    val fileIn = new FileInputStream(kafkaParamsPath)
    val pro = new Properties
    pro.load(fileIn)
    var kafaParamsMaps =Map[String, Object]()
    val kafkaKeys = pro.keys()
    for (kafkaKey <- kafkaKeys.asScala) {
     val a= kafkaKey.toString
      val kafkaValue = pro.getProperty(a)
      kafaParamsMaps +=(a->kafkaValue)
    }
    kafaParamsMaps
  }
}
