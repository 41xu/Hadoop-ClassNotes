package 压缩解压缩;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;

public class SimpleUncompression {

	public static void main(String[] args) throws IOException {		
		Configuration conf = new Configuration();				//创建HDFS配置对象
		conf.set("fs.default.name", "hdfs://xusy:9000");	//通过配置对象设置HDFS文件系统
		FileSystem fs = FileSystem.get(conf);					//创建HDFS文件系统对象
		FSDataInputStream in = fs.open(new Path("/practice/hello.gz"));				//创建输入文件流
		FSDataOutputStream out = fs.create(new Path("/practice/hello2.txt"));		//创建输出文件流

		CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);	//方法二：使用ReflectionUtils创建压缩编码对象	
		CompressionInputStream cin = codec.createInputStream(in);				//创建压缩输入流对象
		
		//方法一（推荐）：利用IOUtils读写数据
		IOUtils.copyBytes(cin, out, 4096, false);	//输入流、输出流、缓存大小、是否关闭流
		IOUtils.closeStream(cin);
		IOUtils.closeStream(out);
		
		//方法二：自定义读写
		/*
		byte[] buff = new byte[4096];
		int length = 0;
		while ((length = cin.read(buff)) != -1) {
			System.out.println(new String(buff, 0, length));
		}
		cin.close();
		out.close();
		*/
		System.out.println("File Uncompress Over!");
	}

}
