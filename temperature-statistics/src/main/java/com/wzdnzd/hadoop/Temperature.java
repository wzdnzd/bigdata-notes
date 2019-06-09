/**
 * @Author : wzdnzd
 * @Time : 2019-06-08
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Temperature implements WritableComparable<Temperature> {
    private int year = 0;
    private int month = 0;
    private int day = 0;
    private int temperature = Integer.MIN_VALUE;

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

    public Temperature() {
    }

    public Temperature(int year, int month, int day, int temperature) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.temperature = temperature;
    }

    @Override
    public int compareTo(Temperature o) {
        int i1 = Integer.compare(this.year, o.getYear());
        if (i1 == 0) {
            int i2 = Integer.compare(this.month, o.getMonth());
            if (i2 == 0) {
                int i3 = Integer.compare(this.day, o.getDay());
                if (i3 == 0)
                    return -Integer.compare(this.temperature, o.getTemperature());
                else return i3;
            } else return i2;
        }
        return i1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.day);
        dataOutput.writeInt(this.temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.temperature = dataInput.readInt();
    }
}
