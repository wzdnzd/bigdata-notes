/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.producer.bean;

import com.wzdnzd.constant.ConstantVal;

public class CallLog {
    private String caller;
    private String called;
    private String time;
    private String duration;

    public CallLog(String caller, String called, String time, String duration) {
        this.caller = caller;
        this.called = called;
        this.time = time;
        this.duration = duration;
    }

    public String getCaller() {
        return caller;
    }

    public void setCaller(String caller) {
        this.caller = caller;
    }

    public String getCalled() {
        return called;
    }

    public void setCalled(String called) {
        this.called = called;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        String delimiter = ConstantVal.DELIMITER.getVal();

        return caller + delimiter + called + delimiter + time + delimiter + duration;
    }
}
