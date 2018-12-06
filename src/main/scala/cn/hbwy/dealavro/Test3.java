package cn.hbwy.dealavro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Function1;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class Test3 {
    private static Injection<GenericRecord, byte[]> recordInjection;

    static {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            schema = parser.parse(new File("H:/out/avrotest/EUTRANCELL-Q.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        recordInjection = GenericAvroCodecs.toBinary(schema);
    }

    public static void main(String[] args) throws InterruptedException {
            SparkConf conf = new SparkConf()
                    .setAppName("kafka-spark")
                    .setMaster("local[*]");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

            Set<String> topics = Collections.singleton("iteblog");
            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", "www.iteblog.com:9092");
            ArrayList<String> arr=new ArrayList<>();
            arr.add("aaa00");
            arr.add("aaa01");
            arr.add("aaa02");
            arr.add("aaa03");
            arr.add("aaa04");

            sc.parallelize(arr,2).foreach(x->{

                System.out.println(recordInjection);;
            });
sc.stop();
/*ssc.start();

            ssc.awaitTermination();*/

        }

}
