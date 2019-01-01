package 文件处理;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileRead {	
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://xusy:9000");
		FileSystem fs = FileSystem.get(conf);
		String path = "/practice/song.map";
		MapFile.Reader reader = new MapFile.Reader(fs, path, conf);
		
		WritableComparable<?> key = (WritableComparable<?>)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while(reader.next(key, value)) {	//因为按key排序，所以该列必须是WritableComparable
			System.out.println(key.toString() + "\t" + value.toString());
		}
		
		IOUtils.closeStream(reader);
	}

}
