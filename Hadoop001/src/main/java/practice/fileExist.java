package practice;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class fileExist {
    public static void main(String[] args)throws IOException, URISyntaxException {
        Configuration conf=new Configuration();
        URI uri=new URI("hdfs://xusy:9000");
        FileSystem fs=FileSystem.get(uri,conf);
        Path path=new Path("/practice/hello.txt");

        if (fs.exists(path)){
            System.out.println("file exist!");
        }
        else{
            System.out.println("file not exist!");
        }

        Path path1=new Path("/practice/hello1.txt");
        if(fs.exists(path1)){
            System.out.println("file exist!");
        }
        else{
            System.out.println("file not exist!");
        }

    }
}
