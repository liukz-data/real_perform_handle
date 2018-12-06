package cn.hbwy.dealavro.mytimeutil;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {
    public static Long getCurrentTime(){
        Date date=new Date();
        SimpleDateFormat sim=new SimpleDateFormat("yyyyMMddhhmmss");
        String f=sim.format(date.getTime());
        return Long.parseLong(f);
    }
}
