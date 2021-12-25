package com.han.mapreduce.wordcount.operatorCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OperatorsCountMapper extends Mapper<LongWritable, Text,Text,Operators> {
    private Text outK = new Text();
    private Operators outV = new Operators();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Operators>.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split("\t");
        //通话类型
        String type=words[12];

        //operator
        String op=words[4];
        if(op.equals("1")){
            outV.setDianxin((Integer)1);
            outV.setYidong((Integer)0);
            outV.setLiantong((Integer)0);
        }
        else if(op.equals("2")){
            outV.setDianxin((Integer)0);
            outV.setYidong((Integer)1);
            outV.setLiantong((Integer)0);
        }
        else if(op.equals("3")){
            outV.setDianxin((Integer)0);
            outV.setYidong((Integer)0);
            outV.setLiantong((Integer)1);
        }

        //写入上下文
        outK.set(type);
        context.write(outK,outV);

    }
}
