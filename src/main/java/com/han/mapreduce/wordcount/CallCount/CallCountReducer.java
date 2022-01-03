package com.han.mapreduce.wordcount.CallCount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CallCountReducer extends Reducer<Text, IntWritable,Text, DoubleWritable>{
    DoubleWritable outv =new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double sum=0;
        for(IntWritable value:values){
            sum+= value.get();
        }
        sum=sum/29.0;
        outv.set(sum);
        context.write(key,outv);
    }
}
