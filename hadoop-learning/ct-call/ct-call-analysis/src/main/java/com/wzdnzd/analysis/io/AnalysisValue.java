/**
 * @Author : wzdnzd
 * @Time : 2019-07-09
 * @Project : bigdata
 */


package com.wzdnzd.analysis.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisValue implements Writable {
    private int totalCall;
    private long totalDuration;

    public AnalysisValue() {
    }

    public AnalysisValue(int totalCall, long totalDuration) {
        this.totalCall = totalCall;
        this.totalDuration = totalDuration;
    }

    public int getTotalCall() {
        return totalCall;
    }

    public void setTotalCall(int totalCall) {
        this.totalCall = totalCall;
    }

    public long getTotalDuration() {
        return totalDuration;
    }

    public void setTotalDuration(long totalDuration) {
        this.totalDuration = totalDuration;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.getTotalCall());
        dataOutput.writeLong(this.getTotalDuration());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.totalCall = dataInput.readInt();
        this.totalDuration = dataInput.readLong();
    }
}
