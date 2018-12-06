package cn.hbwy.dealavro.dealavro

object DealAvroStart {

  def main(args: Array[String]): Unit = {
    val parameter = new Parameter()
    parameter.initParameter(args(0))
    parameter.getAvscFilePath()
    DealAvro.parameter=parameter
    //DealAvro.domain()
  }
}
