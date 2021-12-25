package com.han.mapreduce.wordcount.TimeCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimePercentage implements Writable {
    double tp1=0.0;
    double tp2=0.0;
    double tp3=0.0;
    double tp4=0.0;
    double tp5=0.0;
    double tp6=0.0;
    double tp7=0.0;
    double tp8=0.0;

    public double getTp8() {
        return tp8;
    }

    public void setTp8(double tp8) {
        this.tp8 = tp8;
    }

    public double getTp7() {
        return tp7;
    }

    public void setTp7(double tp7) {
        this.tp7 = tp7;
    }

    public double getTp6() {
        return tp6;
    }

    public void setTp6(double tp6) {
        this.tp6 = tp6;
    }

    public double getTp5() {
        return tp5;
    }

    public void setTp5(double tp5) {
        this.tp5 = tp5;
    }

    public double getTp4() {
        return tp4;
    }

    public void setTp4(double tp4) {
        this.tp4 = tp4;
    }

    public double getTp3() {
        return tp3;
    }

    public void setTp3(double tp3) {
        this.tp3 = tp3;
    }

    public double getTp2() {
        return tp2;
    }

    public void setTp2(double tp2) {
        this.tp2 = tp2;
    }

    public double getTp1() {
        return tp1;
    }

    public void setTp1(double tp1) {
        this.tp1 = tp1;
    }

    @Override
    public String toString() {
        return  tp1 +
                "\t" + tp2 +
                "\t" + tp3 +
                "\t" + tp4 +
                "\t" + tp5 +
                "\t" + tp6 +
                "\t" + tp7 +
                "\t" + tp8 ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(tp1);
        dataOutput.writeDouble(tp2);
        dataOutput.writeDouble(tp3);
        dataOutput.writeDouble(tp4);
        dataOutput.writeDouble(tp5);
        dataOutput.writeDouble(tp6);
        dataOutput.writeDouble(tp7);
        dataOutput.writeDouble(tp8);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tp1= dataInput.readDouble();
        this.tp2= dataInput.readDouble();
        this.tp3= dataInput.readDouble();
        this.tp4= dataInput.readDouble();
        this.tp5= dataInput.readDouble();
        this.tp6= dataInput.readDouble();
        this.tp7= dataInput.readDouble();
        this.tp8= dataInput.readDouble();

    }
}
