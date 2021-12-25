package com.han.mapreduce.operatorCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OperatorCountDriver {
    private Text outK=new Text();
    private IntWritable outV=new IntWritable( 1);
    @Override
    //一行调用一次
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line=value.toString();

        String[] words= line.split("\t");
        //拿到一行的内容
        //循环写出
        System.out.println(words);
        outK.set(words[12]);
        context.write(outK,outV);
}
