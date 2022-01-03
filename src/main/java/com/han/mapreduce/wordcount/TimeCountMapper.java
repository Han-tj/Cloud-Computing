package com.han.mapreduce.wordcount;

import com.han.mapreduce.wordcount.TimeCount.Times;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TimeCountMapper  extends Mapper<LongWritable,Text,Text,IntWritable>{
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(0);
    private Times times=new Times();
    int timeset=0;
    //比较大小
    //a<=b return true
    public boolean lessThan(String start,String end){
        String[] s=start.split(":");
        String[] e=end.split(":");
        int[] st=new int[3];
        int[] et=new int[3];
        for(int j=0;j<s.length;j++){
            st[j]=Integer.parseInt(s[j]);
            et[j]=Integer.parseInt(e[j]);

        }
        if(st[0]>et[0])
            return false;
        else if(st[0]<et[0]){
            return true;
        }
        else if(st[1]>et[1])
        {
            return false;
        }
        else if(st[1]<et[1])
        {
            return true;
        }
        else if(st[2]>et[2]){
            return false;
        }
        else return true;
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        int[] st=new int[3];
        int[] et=new int[3];
        String[] words = line.split("\t");
        String[] startTime=words[9].split(":");
        String[] endTime=words[10].split(":");

        for(int j=0;j<3;j++){
            st[j]=Integer.parseInt(startTime[j]);
            et[j]=Integer.parseInt(endTime[j]);

        }
        //找开始时间所在时间段
        int startset=st[0]/3+1;
        System.out.println(startset);
        //结束时间所在时间段
        int endset=et[0]/3+1;
        System.out.println(endset);
        //终止时间不为0
        if (!words[10].equals("00:00:00")){
            //key: 电话号码+“ ”+时间段（对应为1，2，3，4，5，6，7，8）
            //开始时间小于结束时间
           if(lessThan(words[9],words[10])){
               //起止在同一时间段
               if(startset==endset){
                   timeset=startset;
                   outK.set(words[1]);
                   if(startset==1){
                       times.setTime1(Integer.parseInt(words[11]));
                   }
                   context.write(outK,outV);
               }
               //起止不在同一时间段
               else{
                   //第一段
                   int v1=startset*3*60*60-st[0]*60*60-st[1]*60-st[2];
                   outK.set(words[1]+" "+Integer.toString(startset));
                   outV.set(v1);
                   context.write(outK,outV);
                   //中间满3小时部分
                   for(int n=1;n<endset-startset;n++){
                       outK.set(words[1]+" "+Integer.toString(startset+n));
                       outV.set(3*60*60);
                       context.write(outK,outV);
                   }
                   //最后一部分
                   int v2=et[0]*60*60+et[1]*60+et[2]-(endset-1)*3*60*60;
                   outK.set(words[1]+" "+Integer.toString(endset));
                   outV.set(v2);
                   context.write(outK,outV);

               }
           }
           //开始时间大于结束时间，即到第二天
            else{

               //第一段
               int v1=startset*3*60*60-st[0]*60*60-st[1]*60-st[2];
               outK.set(words[1]+" "+Integer.toString(startset));
               outV.set(v1);
               context.write(outK,outV);
               //这一天剩余其他段
               for(int n=startset+1;n<=8;n++){
                   outK.set(words[1]+" "+Integer.toString(n));
                   outV.set(3*60*60);
                   context.write(outK,outV);
               }
               //第二天
               for(int m=1;m<endset;m++){
                   outK.set(words[1]+" "+Integer.toString(m));
                   outV.set(3*60*60);
                   context.write(outK,outV);
               }
               //最后一部分
               int v2=et[0]*60*60+et[1]*60+et[2]-(endset-1)*3*60*60;
               outK.set(words[1]+" "+Integer.toString(endset));
               outV.set(v2);
               context.write(outK,outV);
           }


        }
        //终止时间为0，计算时长
        else{

            int part1=startset*3*60*60-st[0]*60*60-st[1]*60-st[2];
            //没有跨越时间段
            if(part1>Integer.parseInt(words[11])){
                outK.set(words[1]+" "+Integer.toString(startset));
                outV.set(Integer.parseInt(words[11]));
                context.write(outK,outV);
            }
            else{

                outK.set(words[1]+" "+Integer.toString(startset));
                outV.set(part1);
                context.write(outK,outV);
                int part2=Integer.parseInt(words[11])-part1;
                int z;
                for(z=1;z*60*60*3<part2;z++){
                    outK.set(words[1]+" "+Integer.toString(startset+z));
                    outV.set(60*60*3);
                    context.write(outK,outV);
                }
                outK.set(words[1]+" "+Integer.toString(startset+z));
                outV.set(part2-(z-1)*60*60*3);
                context.write(outK,outV);
            }

        }
    }
}
