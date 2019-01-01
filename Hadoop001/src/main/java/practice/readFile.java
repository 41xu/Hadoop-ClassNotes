package practice;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class readFile {
    public static void main(String[] args) throws IOException{
        Configuration conf=new Configuration();
        conf.set("fs.default.name","hdfs://xusy:9000");
        FileSystem fs=FileSystem.get(conf);
        Path path =new Path("/practice/hello.txt");
        FSDataInputStream is = fs.open(path);
        IOUtils.copyBytes(is,System.out,4096,false);
        IOUtils.closeStream(is);
        System.out.println("File Created!\nFile Read over!");


    }
}
