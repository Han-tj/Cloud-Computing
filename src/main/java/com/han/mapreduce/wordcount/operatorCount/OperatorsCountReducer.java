package com.han.mapreduce.wordcount.operatorCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OperatorsCountReducer extends Reducer<Text,Operators,Text,OperatorPercentage> {


    double sum;
    double dpercentage;
    double ypercentage;
    double lpercentage;
    @Override
    protected void reduce(Text key, Iterable<Operators> values, Reducer<Text, Operators, Text, OperatorPercentage>.Context context) throws IOException, InterruptedException {
        OperatorPercentage operatorPercentage=new OperatorPercentage();
        Operators operators=new Operators();
        for(Operators value:values){

            operators.setDianxin(operators.getDianxin()+value.getDianxin());
            operators.setLiantong(operators.getLiantong()+value.getLiantong());
            operators.setYidong(operators.getYidong()+ value.getYidong());
        }
        sum=operators.getDianxin()+operators.getYidong()+operators.getLiantong();
        System.out.println(operators.getDianxin());
        System.out.println(operators.getLiantong());
        System.out.println(operators.getYidong());
        System.out.println(sum);
        dpercentage=operators.getDianxin()/sum;
        ypercentage=operators.getYidong()/sum;
        lpercentage=operators.getLiantong()/sum;
        operatorPercentage.setDianxin(dpercentage);
        operatorPercentage.setYidong(ypercentage);
        operatorPercentage.setLiantong(lpercentage);
        context.write(key,operatorPercentage);
    }
}
