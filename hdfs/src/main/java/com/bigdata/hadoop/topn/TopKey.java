package com.bigdata.hadoop.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopKey implements WritableComparable<TopKey> {

    private int year;
    private int month;
    private int day;
    private int temperature;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temperature = in.readInt();
    }

    @Override
    public int compareTo(TopKey that) {
        //这里实际可以做更多排序工作,减少 SortComparator 工作
        //目前只按 (年,月) (温度) 均正序排序
        int c1 = Integer.compare(this.year, that.year);
        if (c1 == 0){
            int c2 = Integer.compare(this.month, that.month);
            if (c2 == 0){
                return Integer.compare(this.day, that.day);
            }
            return c2;
        }
        return c1;
    }
}
