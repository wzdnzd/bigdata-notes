/**
 * @Author : wzdnzd
 * @Time : 2019-07-09
 * @Project : bigdata
 */


package com.wzdnzd.analysis.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisKey implements WritableComparable<AnalysisKey> {
    private String tel;
    private String date;

    public AnalysisKey() {
    }

    public AnalysisKey(String tel, String date) {
        this.tel = tel;
        this.date = date;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public int compareTo(AnalysisKey o) {
        int result = this.getTel().compareTo(o.getTel());
        result = result == 0 ? this.getDate().compareTo(o.getDate()) : result;

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.getTel());
        dataOutput.writeUTF(this.getDate());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tel = dataInput.readUTF();
        this.date = dataInput.readUTF();
    }
}
