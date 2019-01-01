package 压缩解压缩;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;

public class CodecPoolCompressor {

	public static void main(String[] args) throws IOException {		
		Configuration conf = new Configuration();				//创建HDFS配置对象
		conf.set("fs.default.name", "hdfs://xusy:9000");	//通过配置对象设置HDFS文件系统
		FileSystem fs = FileSystem.get(conf);					//创建HDFS文件系统对象		
		
		CompressionCodec codec = ReflectionUtils.newInstance(GzipCodec.class, conf);	//方法二：使用反射工具类创建GzipCodec对象
		Compressor compressor = CodecPool.getCompressor(codec);		//从缓冲池中为指定的CompressionCodec检索到一个Compressor实例
		
		FSDataInputStream in = fs.open(new Path("/practice/hello"));					//创建输入文件流
		FSDataOutputStream out = fs.create(new Path("/practice/hello4.gz") );			//创建输出文件流
		CompressionOutputStream cout = codec.createOutputStream(out, compressor);	//创建压缩输入流对象
		
		IOUtils.copyBytes(in, cout, 4096, false);
		IOUtils.closeStream(in);
		IOUtils.closeStream(cout);

		System.out.println("Test CodecPool Over!");
	}

}
