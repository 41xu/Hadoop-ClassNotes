package sample_test;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

public class create {
    public static void main(String[] args) throws IOException{
        Configuration conf=new Configuration();
        conf.set("fs.default.name","hdfs://xusy:9000");
        FileSystem fs=FileSystem.get(conf);
        Path path=new Path("/practice/hello2020.txt");
        FSDataOutputStream os=fs.create(path,true);
        os.writeBytes("hello 2020, hello HDFS\n");
        os.close();
        System.out.println("File create!");
    }
}
