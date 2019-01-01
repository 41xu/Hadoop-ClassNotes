package 压缩解压缩;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

public class SimpleCompression {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();				//创建HDFS配置对象
		conf.set("fs.default.name", "hdfs://xusy:9000");	//通过配置对象设置HDFS文件系统
		FileSystem fs = FileSystem.get(conf);					//创建HDFS文件系统对象		
		FSDataOutputStream out = fs.create(new Path("/practice/hello.gz"), true);	//创建HDFS输出流：新建文件

		CompressionCodec codec = new GzipCodec();				//方法一：使用new创建压缩编码对象
		CompressionOutputStream cout = codec.createOutputStream(out);			//将普通输出流转换为压缩输出流
		
		byte[] b = "hello hadoop\n".getBytes();
		cout.write(b);			//压缩输出流没有writeBytes方法
		cout.close();		
		System.out.println("File Compress Over!");
	}

}
