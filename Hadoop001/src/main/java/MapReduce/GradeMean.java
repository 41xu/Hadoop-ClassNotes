package MapReduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GradeMean {
    public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
        private final static IntWritable grade=new IntWritable(0);
        private Text student = new Text();

        public void map(Object key, Text value, Context context)throws IOException,InterruptedException{
            StringTokenizer info=new StringTokenizer(value.toString()); //StringTokenizer默认用啦拆分字符串
            while(info.hasMoreTokens()){
                String name=info.nextToken();
                String grade_=info.nextToken();

                grade.set(Integer.parseInt(grade_));
                student.set(name);
                context.write(student,grade);
            }
        }
    }

    public static class IntMeanReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable mean= new IntWritable(0);

        public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
            int sum=0;
            int count=0;
            for (IntWritable val:values) {
                sum += val.get();
                count++;
            }
            mean.set(sum/count);
            context.write(key,mean);
        }
    }

    public static void main(String[] args)throws Exception{
        String hdfs="hdfs://xusy:9000";
        Configuration conf=new Configuration();
        conf.set("fs.default.name",hdfs);

        String jobName="GradeMean";
        Job job=Job.getInstance(conf,jobName);
        job.setJarByClass(GradeMean.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntMeanReducer.class);
        job.setReducerClass(IntMeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String dataDir="/data/Grade";
        String outputDir="/data/Grade/output";
        Path input=new Path(hdfs+dataDir);
        Path output=new Path(hdfs+outputDir);
        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output,true);
        }

        System.out.println("Job: "+jobName+" is running...");
        if(job.waitForCompletion(true)){
            System.out.println("success!!!");
            System.exit(0);
        }
        else{
            System.out.println("failed!!!");
            System.exit(1);
        }
    }
}
