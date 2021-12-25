package com.han.mapreduce.wordcount.operatorCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OperatorPercentage implements Writable {
    private double dianxin;
    private double yidong;
    private double liantong;

    public double getYidong() {
        return yidong;
    }

    public void setYidong(double yidong) {
        this.yidong = yidong;
    }

    public double getLiantong() {
        return liantong;
    }

    public void setLiantong(double liantong) {
        this.liantong = liantong;
    }

    public double getDianxin() {
        return dianxin;
    }

    public void setDianxin(double dianxin) {
        this.dianxin = dianxin;
    }

    @Override
    public String toString() {
        return
                 dianxin +
                "\t" + yidong +
                "\t" + liantong ;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(dianxin);
        dataOutput.writeDouble(yidong);
        dataOutput.writeDouble(liantong);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dianxin=dataInput.readDouble();
        this.yidong=dataInput.readDouble();
        this.liantong=dataInput.readDouble();

    }
}
