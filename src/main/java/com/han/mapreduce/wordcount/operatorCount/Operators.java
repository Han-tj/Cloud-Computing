package com.han.mapreduce.wordcount.operatorCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Operators implements Writable {
    private Integer dianxin=0;//电信总数
    private Integer yidong=0;
    private Integer liantong=0;

    public Integer getYidong() {
        return yidong;
    }



    public void setYidong(Integer yidong) {
        this.yidong = yidong;
    }

    public Integer getLiantong() {
        return liantong;
    }

    public void setLiantong(Integer liantong) {
        this.liantong = liantong;
    }

    public Integer getDianxin() {
        return dianxin;
    }

    public void setDianxin(Integer dianxin) {
        this.dianxin = dianxin;
    }

    @Override
    public String toString() {
        return
                dianxin +
                        "\t" + yidong +
                        "\t" + liantong;
    }
//序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(dianxin);
        dataOutput.writeInt(yidong );
        dataOutput.writeInt(liantong);
    }
    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dianxin=dataInput.readInt();
        this.yidong= dataInput.readInt();
        this.liantong= dataInput.readInt();
    }
}
