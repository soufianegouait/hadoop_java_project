package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
public class FileStatus {
    public static void main(String[] args) {
// TODO Auto-generated method stub
        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
            if(args.length==3){
                Path nomcomplet = new
                        Path(args[0], args[1]) ;
                org.apache.hadoop.fs.FileStatus infos =
                        fs.getFileStatus(nomcomplet);
                System.out.println(Long.toString(infos.getLen())+" octets");
                fs.rename(nomcomplet, new Path(args[0], args[2]));
            }else {
                System.out.println("error vous devez saisir les 3 parametres");
            }

        } catch (IOException e) {
// TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}