package 文件处理;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;

public class SequenceFileWrite2 {	
	private static final String[] DATA = {
		"2015-11-01 1",
		"2016-01-29 2",
		"2015-03-09 3",
		"2017-12-05 4",
		"2017-04-26 5"
	};
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://xusy:9000");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/data/sequence/date.seq");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, IntWritable.class);
		Text key = new Text();
		IntWritable value = new IntWritable();
		for(int i=0; i<DATA.length; i++) {
			String[] str = DATA[i].split(" ");
			key.set(str[0]);
			value.set(Integer.parseInt(str[1]));			
			writer.append(key, value);
			System.out.printf("%s\t%s\n", key, value);
			//System.out.printf("%s\n", value);
		}
		
		IOUtils.closeStream(writer);
	}

}
