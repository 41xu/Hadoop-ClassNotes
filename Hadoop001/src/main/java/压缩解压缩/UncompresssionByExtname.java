package 压缩解压缩;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.IOUtils;

public class UncompresssionByExtname {

	public static void main(String[] args) throws IOException {		
		Configuration conf = new Configuration();				//创建HDFS配置对象
		conf.set("fs.default.name", "hdfs://xusy:9000");	//通过配置对象设置HDFS文件系统
		FileSystem fs = FileSystem.get(conf);					//创建HDFS文件系统对象		
		
		String inFile = "/practice/hello.gz";
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);	//创建Codec工厂对象	
		CompressionCodec codec = factory.getCodec(new Path(inFile));	//工厂根据给定压缩文件获取相应的Codec对象
		if (codec == null) {										//找不到对应Codec则报错
			System.out.println("No codec found");
			System.exit(1);
		}		
		FSDataInputStream in = fs.open(new Path(inFile));			//创建输入文件流
		CompressionInputStream cin = codec.createInputStream(in);	//创建压缩输入流对象
		
		String outFile = CompressionCodecFactory.removeSuffix(inFile, codec.getDefaultExtension());	//设置解压缩后的文件名
		FSDataOutputStream out = fs.create(new Path(outFile));		//创建输出文件流
		
		IOUtils.copyBytes(cin, out, 4096, false);	//输入流、输出流、缓存大小、是否关闭流
		IOUtils.closeStream(cin);
		IOUtils.closeStream(out);
		System.out.println("Uncompressinon By Extname!");
	}

}
