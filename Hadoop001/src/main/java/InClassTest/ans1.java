package InClassTest;

import java.io.IOException;
import java.net.Inet4Address;

import com.sun.org.apache.xml.internal.dtm.ref.DTMDefaultBaseIterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ans1 {
    public static class FlowMapper extends Mapper<Object,Text,Text,Text>{
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
            String[] strs=value.toString().split(" ");
            String data=strs[strs.length-2];
            Text KEY=new Text(strs[3]);
            if (data.equals("404")){
                context.write(KEY,value);
            }
        }

    }
    public static class FlowReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException{
            for(Text v: values){
                context.write(key,v);
            }
        }
    }
    public static void main(String[] args)throws Exception{
        //1.设置HDFS配置信息
        String namenode_ip="localhost";
        String hdfs="hdfs://"+namenode_ip+":9000";
        Configuration conf=new Configuration(); //Hadoop配置类
        conf.set("fs.defaultFS",hdfs);
        conf.set("mapreduce.app-submission.cross-platform","true"); //集群交叉提交
//        conf.set("mapreduce.framework.name","yarn");
        /*conf.set("hadoop.job.user","hadoop");
        conf.set("mapreduce.jobtracker.address",namenode_ip+":9001");
        conf.set("yarn.resourcemanager.hostname",namenode_ip);
        conf.set("yarn.resourcemanager.resource-tracker.address", namenode_ip + ":8031");
        conf.set("yarn.resourcemtanager.address", namenode_ip + ":8032");
        conf.set("yarn.resourcemanager.admin.address", namenode_ip + ":8033");
        conf.set("yarn.resourcemanager.scheduler.address", namenode_ip + ":8034");
        conf.set("mapreduce.jobhistory.address", namenode_ip + ":10020");*/
        //2.设置MapReduce作业配置信息
        String jobName = "Exp";					//定义作业名称
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(FlowMapper.class);			//指定作业类
        //job.setJar("export/WordCount.jar");			//指定本地jar包
        job.setMapperClass(FlowMapper.class);
        job.setCombinerClass(FlowReducer.class);		//指定Combiner类
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //3.设置作业输入和输出路径
        String dataDir = "/data/exp/data";		//实验数据目录
        String outputDir = "/data/exp/output2";	//实验输出目录
        Path inPath = new Path(hdfs + dataDir);
        Path outPath = new Path(hdfs + outputDir);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        //如果输出目录已存在则删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        //4.运行作业
        System.out.println("Job: " + jobName + " is running...");
        if(job.waitForCompletion(true)) {
            System.out.println("success!");
            System.exit(0);
        } else {
            System.out.println("failed!");
            System.exit(1);
        }

    }
}
