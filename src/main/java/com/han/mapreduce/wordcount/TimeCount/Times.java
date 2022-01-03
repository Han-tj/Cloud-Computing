package com.han.mapreduce.wordcount.TimeCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Times implements Writable {
    int time1=0;
    int time2=0;
    int time3=0;
    int time4=0;
    int time5=0;
    int time6=0;
    int time7=0;
    int time8=0;

    public int getTime8() {

        return time8;
    }

    public void setTime8(int time8) {
        this.time8 = time8;
    }

    public int getTime7() {
        return time7;
    }

    public void setTime7(int time7) {
        this.time7 = time7;
    }

    public int getTime6() {
        return time6;
    }

    public void setTime6(int time6) {
        this.time6 = time6;
    }

    public int getTime5() {
        return time5;
    }

    public void setTime5(int time5) {
        this.time5 = time5;
    }

    public int getTime4() {
        return time4;
    }

    public void setTime4(int time4) {
        this.time4 = time4;
    }

    public int getTime3() {
        return time3;
    }

    public void setTime3(int time3) {
        this.time3 = time3;
    }

    public int getTime2() {
        return time2;
    }

    public void setTime2(int time2) {
        this.time2 = time2;
    }

    public int getTime1() {
        return time1;
    }

    public void setTime1(int time1) {
        this.time1 = time1;
    }

    @Override
    public String toString() {
        return time1 +
                "\t" + time2 +
                "\t"  + time3 +
               "\t" + time4 +
                "\t" + time5 +
                "\t" + time6 +
                "\t" + time7 +
                "\t" + time8
               ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(time1);
        dataOutput.writeInt(time2);
        dataOutput.writeInt(time3);
        dataOutput.writeInt(time4);
        dataOutput.writeInt(time5);
        dataOutput.writeInt(time6);
        dataOutput.writeInt(time7);
        dataOutput.writeInt(time8);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.time1= dataInput.readInt();
        this.time2= dataInput.readInt();
        this.time3= dataInput.readInt();
        this.time4= dataInput.readInt();
        this.time5= dataInput.readInt();
        this.time6= dataInput.readInt();
        this.time7= dataInput.readInt();
        this.time8= dataInput.readInt();
    }
}
