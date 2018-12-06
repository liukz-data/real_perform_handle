package cn.hbwy.dealavro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class Test2 {

    public static  DataFileWriter<GenericRecord> writeInSchema() throws IOException {
        //指定定义的avsc文件。
        Schema schema = new Schema.Parser().parse(new File("H:/out/avrotest/EUTRANCELL-Q.avsc"));

        //创建GenericRecord,相当于Employee
        GenericRecord e1 = new GenericData.Record(schema);
        //设置javabean属性
        //e1.put("int_id", "ramu");
//        e1.put("id", 001);
//        e1.put("salary", 30000);
        //e1.put("age", 25);
//        e1.put("address", "chennai");

        //
        DatumWriter w1 = new SpecificDatumWriter(schema);
        DataFileWriter w2 = new DataFileWriter(w1);
        w2.create(schema,new File("H:/out/avrotest/outavro2.avro")) ;
       return w2;
    }
}
