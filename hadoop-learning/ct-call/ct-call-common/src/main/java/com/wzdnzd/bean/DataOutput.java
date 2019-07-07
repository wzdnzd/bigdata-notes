/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.bean;

import java.io.Closeable;

public interface DataOutput extends Closeable {
    public void setOutputPath(String path);

    public String getOutputPath();

    public void write(Object data);

    public void write(String data);
}
