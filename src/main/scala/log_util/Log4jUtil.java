package log_util;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Log4jUtil {
    public static Logger getLogger(String confPath,Class clazz){
        //PropertyConfigurator.configure("G:\\Users\\lkz\\IdeaProjects\\FtpToHdfs1\\src\\main\\scala\\cn\\hbwy\\FtpToHdfs\\sparkdeal\\FileToHive\\log4j.properties");
        PropertyConfigurator.configure(confPath);
        return Logger.getLogger(clazz);
    }
}
