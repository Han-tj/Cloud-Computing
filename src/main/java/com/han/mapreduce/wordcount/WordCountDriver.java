package com.han.mapreduce.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

//        System.setProperty("hadoop.home.dir", "D:\\hadoop3.0.0\\hadoop-3.3.0");
        //1 获取job //alt+enter抛出异常
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        //3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4 设置map输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5 设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6 设置输入路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\86181\\Desktop\\test"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\86181\\Desktop\\output"));

        //7 提交job alt+enter 抛出异常
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);


    }
}