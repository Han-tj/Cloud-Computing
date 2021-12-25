package com.han.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CallCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] words = line.split("\t");
        //电话号码+日期
        String K=words[1]+" "+words[0];
        //System.out.println(K);

        outK.set(K);
        context.write(outK,outV);

    }
}
