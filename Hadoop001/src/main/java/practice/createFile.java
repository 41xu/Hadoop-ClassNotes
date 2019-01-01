package practice;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

public class createFile {
    public static void main(String[] args) throws IOException{
        Configuration conf=new Configuration();
        conf.set("fs.default.name","hdfs://xusy:9000");
        FileSystem fs=FileSystem.get(conf);
        Path path=new Path("/practice/hello.txt");
        FSDataOutputStream os=fs.create(path,true);
        os.writeBytes("Hello HDFS\n");
        os.close();
        System.out.println("File created!");
    }
}
