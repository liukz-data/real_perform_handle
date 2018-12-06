package cn.hbwy.dealavro.myfileutil;

import java.io.File;
import java.io.IOException;

public class FileUtil {
public void aa(){}
    public static File createParentDirAndFile(String filePath) throws IOException {

        File file= new File(filePath);
        String parentDir=file.getParent();
        File parentDirFile=new File(parentDir);
        if(!parentDirFile.exists()){
            parentDirFile.mkdirs();
        }
        if(!file.exists()){
            file.createNewFile();
        }
        return file;

    }
}
