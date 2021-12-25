package com.han.mapreduce.wordcount.operatorCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OperatorsCountReducer extends Reducer<Text,Operators,Text,OperatorPercentage> {

    OperatorPercentage operatorPercentage=new OperatorPercentage();
    Operators operators=new Operators();
    double sum;
    double dpercentage;
    double ypercentage;
    double lercentage;
    @Override
    protected void reduce(Text key, Iterable<Operators> values, Reducer<Text, Operators, Text, OperatorPercentage>.Context context) throws IOException, InterruptedException {
        for(Operators value:values){
            System.out.println("in");
            Integer d= operators.getDianxin();
            Integer y=operators.getYidong();
            Integer l=operators.getLiantong();
            operators.setDianxin(operators.getDianxin()+value.getDianxin());
            operators.setLiantong(operators.getLiantong()+value.getLiantong());
            operators.setYidong(operators.getYidong()+ value.getYidong());
        }
        sum=operators.getDianxin()+operators.getYidong()+operators.getLiantong();
        dpercentage=(double)operators.getDianxin()/sum;
        ypercentage=(double)operators.getYidong()/sum;
        lercentage=(double)operators.getLiantong()/sum;
        operatorPercentage.setDianxin(dpercentage);
        operatorPercentage.setYidong(ypercentage);
        operatorPercentage.setLiantong(lercentage);
        context.write(key,operatorPercentage);
    }
}
