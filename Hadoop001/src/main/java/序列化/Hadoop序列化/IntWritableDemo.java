package 序列化.Hadoop序列化;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

public class IntWritableDemo {
	
	public static void main(String[] args) throws IOException {		
		IntWritable i = new IntWritable(100);	//初始化IntWritable
		IntWritable j = new IntWritable(200);
		IntWritable sum = new IntWritable();	//定义IntWritable		
		int s = i.get() + j.get();				//IntWritable 转成 int
		sum.set(s);								//int 转成 IntWritable	
		
		//1 输出到console
		System.out.println("sum is " + sum);	
		
		//2.1 输出到本地文件
		FileOutputStream fileOut = new FileOutputStream("IntWritable.ser");
		DataOutputStream dataOut= new DataOutputStream(fileOut);
		sum.write(dataOut);				//write方法写入DataOutputStream
		fileOut.close();
		dataOut.close();
		
		//2.2 从本地文件读取
		FileInputStream fileIn = new FileInputStream("IntWritable.ser");
		DataInputStream dataIn = new DataInputStream(fileIn);
		IntWritable in = new IntWritable();
		in.readFields(dataIn);
		System.out.println("read from local file : " + in.get());
		fileIn.close();
		dataIn.close();
		
		//3.1 输出到HDFS
		Configuration conf = new Configuration();				//创建HDFS配置对象
		conf.set("fs.default.name", "hdfs://xusy:9000");	//通过配置对象设置HDFS文件系统
		FileSystem fs = FileSystem.get(conf);					//创建HDFS文件系统对象
		Path path = new Path("/practice/IntWritable.ser");			//定义目标文件路径
		FSDataOutputStream os = fs.create(path, true);			//创建HDFS文件系统输出流
		os.writeInt(sum.get());
		os.close();
		
		//3.2 从HDFS读取
		FSDataInputStream is = fs.open(path);
		System.out.println("read from HDFS : " +is.readInt());
		is.close();
		
		System.out.println("--------over--------");
	}

}
