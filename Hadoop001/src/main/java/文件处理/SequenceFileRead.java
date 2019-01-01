package 文件处理;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;

public class SequenceFileRead {	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://xusy:9000");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/practice/song.seq");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		
		Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		long position = reader.getPosition();		
		while(reader.next(key, value)) {
			String syncSeen = reader.syncSeen() ? "*" : " ";
			System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
			position = reader.getPosition();
		}
		
		IOUtils.closeStream(reader);
	}

}
