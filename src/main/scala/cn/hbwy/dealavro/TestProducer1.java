package cn.hbwy.dealavro;


import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import log_util.Log4jUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.Seconds;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class TestProducer1 {

    public static void main(String[] args) throws IOException, InterruptedException {
        Log4jUtil.getLogger("G:\\Users\\lkz\\IdeaProjects\\dealavro\\src\\main\\scala\\log_util\\log4j.properties", TestProducer1.class);
        Properties props = new Properties();
        //props.put("zookeeper.connect", "192.168.43.2:2181");
       // props.put("metadata.broker.list", "192.168.43.2:9092");
        props.put("bootstrap.servers", "192.168.43.3:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Schema schema = new Schema.Parser().parse(new File("H:/out/avrotest/EUTRANCELL-Q.avsc"));
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        Producer<byte[],byte[]> producer = new KafkaProducer<>(props);
        GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("scan_start_time", "2018-12-03 17:30");
            avroRecord.put("city_id", "zzzzzzzz" );
            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<byte[],byte[]> record = new ProducerRecord<>("testavro1", "aaaaa".getBytes() , bytes);
        while (true) {
            producer.send(record);
           Thread.sleep(250);

        }

    }
}
