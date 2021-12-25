package com.han.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *KEYIN,map输入阶段类型，偏移量
 * VALUEIN,map阶段输入的value类型，就是string，不过hadoop里用text
 * KEYOUT，map阶段输出的类型，就是单词，是Text
 * VALUEOUT，map阶段输出的value类型，用int
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text outK = new Text();//写在外面节约空间
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        //2 切割
        String[] words = line.split("");

        //3 循环写出
        for (String word : words) {
            //封装outK
            outK.set(word);

            //写出
            context.write(outK,outV);
        }
    }
}
