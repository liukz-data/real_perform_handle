package cn.hbwy.dealavro

import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

object AvroSerde {
  def serialize(record: GenericRecord, schema: Schema): Array[Byte] = {
    val codecs = GenericAvroCodecs.toBinary[GenericRecord](schema)
    codecs.apply(record)
  }
  def deserialize(data: Array[Byte], schema: Schema): GenericRecord = {
    val codecs = GenericAvroCodecs.toBinary[GenericRecord](schema)
    codecs.invert(data).get
  }
}
