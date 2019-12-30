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

public class GradeCal {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable grade = new IntWritable(0);
        private Text student = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer info = new StringTokenizer(value.toString());
            while (info.hasMoreTokens()) {
                String s = info.nextToken();
                String g = info.nextToken();

                grade.set(Integer.parseInt(g));
                student.set(s);
                context.write(student, grade);
            }
        }
    }


    public static class IntMeanReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static IntWritable mean = new IntWritable(0);

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum = sum + val.get();
                count++;
            }
            mean.set(sum / count);
            context.write(key, mean);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String hdfs = "hdfs://xusy:9000";
        conf.set("fs.default.name", hdfs);

        String jobName = "GradeCal";
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(GradeCal.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntMeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String dataDir = "/data/Grade";
        String outDir = "/data/Grade/output";
        Path inPath = new Path(hdfs + dataDir);
        Path outPath = new Path(hdfs + outDir);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath))
            fs.delete(outPath, true);

        System.out.println("Job: " + jobName + "is running");
        if (job.waitForCompletion(true)) {
            System.out.println("Success!");
            System.exit(0);
        } else {
            System.out.println("Failed!");
            System.exit(1);
        }

    }
}

