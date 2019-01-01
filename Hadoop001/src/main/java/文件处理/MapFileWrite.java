package 文件处理;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;

public class MapFileWrite {
	private static final String[] DATA = {
		"One, two, buckle my shoe",
		"Three, four, shut the door",
		"Five, six, pick up sticks",
		"Seven, eight, lay them straight",
		"Nine, ten, a big fat hen"
	};
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://xusy:9000");
		FileSystem fs = FileSystem.get(conf);
		String path = "/practice/song.map";		//此处文件路径是String类型，不是Path，在该目录下生成data和index
		MapFile.Writer writer = new MapFile.Writer(conf, fs, path, IntWritable.class, Text.class);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		for(int i=0; i<100; i++) {			//必须按key顺序添加记录，循环变量递增
			key.set(i+1);
			value.set(DATA[i % DATA.length]);			
			System.out.printf("%s\t%s\n", key, value);
			writer.append(key, value);		
		}
		
		IOUtils.closeStream(writer);
	}

}
