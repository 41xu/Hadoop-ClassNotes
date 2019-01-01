package 文件处理;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;

public class SequenceFileWrite {	
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
		conf.set("fs.defaultFS", "hdfs://192.168.17.10:9000");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/test/song.seq");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		for(int i=0; i<100; i++) {
			key.set(100-i);
			value.set(DATA[i % DATA.length]);
			writer.append(key, value);
			System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
		}
		
		IOUtils.closeStream(writer);	
	}

}
