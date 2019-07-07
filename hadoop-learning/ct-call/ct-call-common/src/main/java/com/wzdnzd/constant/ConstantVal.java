/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.constant;

import com.wzdnzd.bean.Values;

public enum ConstantVal implements Values {
    NAMESPACE("ct"),
    HBASE_TABLE(NAMESPACE.getVal() + ":call"),
    HBASE_CF_CALLER("caller"),
    HBASE_CF_CALLED("called"),
    HBASE_CF_DEFAULT("info"),
    KAFKA_TOPIC("ct-call"),
    DELIMITER("\t");

    private String name;
    public static final int REGION_NUM = 6;
    public static final String COPROCESSOR_CLASS_NAME = "com.wzdnzd.coprocessor.DataInsertCoprocessor";
    public static final String COPROCESSOR_JAR_PATH = "/hbase/coprocessor/ct-call-coprocessor-1.0.jar";

    ConstantVal(String name) {
        this.name = name;
    }

    @Override
    public void setVal(Object val) {
        this.name = (String) val;
    }

    @Override
    public String getVal() {
        return name;
    }
}
