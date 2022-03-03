package com.atguigu.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DateDistinct {
    public static class DateDistinctMapper extends Mapper<Object, Text,Text, NullWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split(" ");
            String s1 = s[0];
            context.write(new Text(s1),NullWritable.get());
        }
    }
    public static class DateDistinctReducer extends Reducer<Text,NullWritable,Text,NullWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(DateDistinct.class);
        job.setMapperClass(DateDistinctMapper.class);
        job.setReducerClass(DateDistinctReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\MRTest\\job"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\MRTest\\result"));
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
