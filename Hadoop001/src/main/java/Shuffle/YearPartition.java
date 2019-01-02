package Shuffle;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YearPartition {
    public static class YearMapper extends Mapper<Object, Text, Text, IntWritable>{
        public void map(Object key, Text value, Context context)throws IOException,InterruptedException{
            String[] line= value.toString().split(" ");
            Text year=new Text(line[0].substring(0,7));
            IntWritable tem=new IntWritable(Integer.parseInt(line[2]));
            context.write(year,tem);

        }
    }
    public static class YearReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reducer(Text key, Iterable<IntWritable> value) throws IOException,InterruptedException{

        }
    }
}
