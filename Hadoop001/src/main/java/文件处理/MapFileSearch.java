package 文件处理;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileSearch {	
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://xusy:9000");
		FileSystem fs = FileSystem.get(conf);
		String path = "/practice/song.map";
		MapFile.Reader reader = new MapFile.Reader(fs, path, conf);
		
		IntWritable key = (IntWritable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Text value = (Text)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		key.set(100);	//设置key=99
		reader.get(key, value);		//读取该key的记录
		System.out.println(key.toString() + "\t" + value.toString());
		reader.getClosest(key, value);	//读取最接近该key的记录
		System.out.println(key.toString() + "\t" + value.toString());
		IOUtils.closeStream(reader);
	}

}
