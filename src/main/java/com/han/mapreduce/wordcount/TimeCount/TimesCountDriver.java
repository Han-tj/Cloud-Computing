package com.han.mapreduce.wordcount.TimeCount;

import com.han.mapreduce.wordcount.TimeCountDriver;
import com.han.mapreduce.wordcount.TimeCountMapper;
import com.han.mapreduce.wordcount.TimeCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TimesCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

//        System.setProperty("hadoop.home.dir", "D:\\hadoop3.0.0\\hadoop-3.3.0");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass(TimesCountDriver.class);


        job.setMapperClass(TimesCountMapper.class);
        job.setReducerClass(TimesCountReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Times.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TimePercentage.class);

        //6 设置输入路径
        FileInputFormat.setInputPaths(job,new Path("E:\\cloud\\data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\cloud\\TimesCount5"));
        //FileInputFormat.setInputPaths(job,new Path(args[0]));
        //FileOutputFormat.setOutputPath(job,new Path(args[1]));
        conf.set("map.output.field.ignoreseparator", "true");
        conf.set("map.output.field.separator",",");

        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("mapred.textoutputformat.separator",",");


        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);


    }

}
