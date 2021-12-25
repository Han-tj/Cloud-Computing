package com.han.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *KEYIN,reduce输入阶段类型，Text
 * VALUEIN,reduce阶段输入的value类型，IntWritable
 * KEYOUT，reduce阶段输出的类型，Text
 * VALUEOUT，reduce阶段输出的value类型，用int
 */
public class WordCountReducer extends Reducer <Text, IntWritable,Text,IntWritable>{

    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);
        context.write(key,outV);
    }
}
