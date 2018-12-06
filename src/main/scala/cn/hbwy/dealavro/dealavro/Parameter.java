package cn.hbwy.dealavro.dealavro;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Parameter {

    private String avscFilePath="H:/out/avrotest/EUTRANCELL-Q.avsc";
    private String  kafkaParamsPath="G:\\Users\\lkz\\IdeaProjects\\dealavro\\src\\main\\scala\\cn\\hbwy\\dealavro\\dealavro\\kafkaparameter.properties";
    private String scan_start_time="scan_start_time";
    private String loadPath="H:/out/avrotest/load";
    private String suffix="_outavro.avro";
    private String log4jPath="G:\\Users\\lkz\\IdeaProjects\\dealavro\\src\\main\\scala\\log_util\\log4j.properties";
    private String topics="testavro1";
    private int secondsDuration=5;
    public Parameter(){}
    public void initParameter(String paramsPath) {
        FileInputStream paramPath = null;
        Properties pros =  new Properties();
        try {
            paramPath = new FileInputStream(paramsPath);
            pros.load(paramPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        avscFilePath=pros.getProperty("avscFilePath");
        kafkaParamsPath=pros.getProperty("kafkaParamsPath");
        scan_start_time=pros.getProperty("scan_start_time");
        loadPath=pros.getProperty("loadPath");
        suffix=pros.getProperty("suffix");
        log4jPath=pros.getProperty("log4jPath");
        topics=pros.getProperty("topics");
        secondsDuration=Integer.parseInt(pros.getProperty("secondsDuration"));
    }
    public String getAvscFilePath() {
        return avscFilePath;
    }

    public String getKafkaParamsPath() {
        return kafkaParamsPath;
    }

    public String getScan_start_time() {
        return scan_start_time;
    }

    public String getLoadPath() {
        return loadPath;
    }

    public String getSuffix() {
        return suffix;
    }

    public String getLog4jPath() {
        return log4jPath;
    }

    public String getTopics() {
        return topics;
    }

    public int getSecondsDuration() {
        return secondsDuration;
    }
}
