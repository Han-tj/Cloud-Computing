package com.han.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OperatorCountMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        String line = value.toString();


        String[] words = line.split("\t");
        String type=words[12];
        String k=words[12]+" "+words[4];
        System.out.println(k);

            outK.set(k);
            context.write(outK,outV);

    }
}
