package com.han.mapreduce.wordcount.TimeCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TimesCountReducer extends Reducer<Text,Times,Text,TimePercentage> {
    private TimePercentage timePercentage=new TimePercentage();
    private Times times=new Times();
    double sum;

    @Override
    protected void reduce(Text key, Iterable<Times> values, Reducer<Text, Times, Text, TimePercentage>.Context context) throws IOException, InterruptedException {
        for(Times value:values){
            times.setTime1(times.getTime1()+ value.getTime1());
            times.setTime2(times.getTime2()+ value.getTime2());
            times.setTime3(times.getTime3()+ value.getTime3());
            times.setTime4(times.getTime4()+ value.getTime4());
            times.setTime5(times.getTime5()+ value.getTime5());
            times.setTime6(times.getTime6()+ value.getTime6());
            times.setTime7 (times.getTime7()+ value.getTime7());
            times.setTime8(times.getTime8()+ value.getTime8());
        }
        sum=times.getTime1()+ times.getTime2()+ times.getTime3()+ times.getTime4()+ times.getTime5()+ times.getTime6()+ times.getTime7()+ times.getTime8();
        timePercentage.setTp1((double)times.getTime1()/sum);
        timePercentage.setTp2((double)times.getTime2()/sum);
        timePercentage.setTp3((double)times.getTime3()/sum);
        timePercentage.setTp4((double)times.getTime4()/sum);
        timePercentage.setTp5((double)times.getTime5()/sum);
        timePercentage.setTp6((double)times.getTime6()/sum);
        timePercentage.setTp7((double)times.getTime7()/sum);
        timePercentage.setTp8((double)times.getTime8()/sum);
        context.write(key,timePercentage);

    }
}
