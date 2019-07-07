/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.bean;

public abstract class DataObject implements Values {
    private String content;

    @Override
    public void setVal(Object val) {
        this.content = (String) val;
    }

    @Override
    public Object getVal() {
        return this.content;
    }
}
