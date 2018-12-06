package cn.hbwy.dealavro.dealavro;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JavaKafkaParameter {
    public JavaKafkaParameter(){}
    @Test
    public static Map<String,Object> getKafkaParams(String kafkaParamsPath) throws IOException {
        //FileInputStream fileIn = new FileInputStream("G:\\Users\\lkz\\IdeaProjects\\dealavro\\src\\main\\scala\\cn\\hbwy\\dealavro\\dealavro\\kafkaparameter.properties");
        FileInputStream fileIn = new FileInputStream(kafkaParamsPath);
        Properties pro =  new Properties();
        pro.load(fileIn);
        Map<String,Object> kafaParamsMaps = new HashMap<>();
        Set<Object> kafkaKeys = pro.keySet();
       for(Object kafkaKey:kafkaKeys){
          String key = kafkaKey.toString();
          String kafkaValue=pro.getProperty(key);
           kafaParamsMaps.put(key,kafkaValue);
       }
        return kafaParamsMaps;
    }
}
