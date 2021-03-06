package com.han.mapreduce.wordcount.operatorCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OperatorsCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

//        System.setProperty("hadoop.home.dir", "D:\\hadoop3.0.0\\hadoop-3.3.0");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setJarByClass( OperatorsCountDriver.class);


        job.setMapperClass(OperatorsCountMapper.class);
        job.setReducerClass(OperatorsCountReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Operators.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OperatorPercentage.class);

        //6 设置输入路径
        FileInputFormat.setInputPaths(job,new Path("E:\\cloud\\data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\cloud\\OperatorsCount20"));
        //FileInputFormat.setInputPaths(job,new Path(args[0]));
        //FileOutputFormat.setOutputPath(job,new Path(args[1]));


        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);


    }

}
