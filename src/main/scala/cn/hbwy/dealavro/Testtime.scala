package cn.hbwy.dealavro

import java.text.SimpleDateFormat
import java.util.Date

object Testtime {
  def main(args: Array[String]): Unit = {
    val date=new Date()
    println(date.getTime)
    val sim=new SimpleDateFormat("yyyyMMddhhmmss")
    val f=sim.format(date.getTime)
    println(f)
    Thread.sleep(1000)
    val date1=new Date()
    println(date1.getTime)
    val sim1=new SimpleDateFormat("yyyyMMddhhmmss")
    val f1=sim1.format(date1.getTime)
    println(f1)
    val aa=f1.toLong-f.toLong
    println(aa%10)
  }
}
