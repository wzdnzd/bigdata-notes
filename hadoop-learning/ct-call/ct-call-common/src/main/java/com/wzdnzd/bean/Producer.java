/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.bean;

import java.io.Closeable;

public interface Producer extends Closeable {
    public void setInput(DataInput input);
    public void setOutput(DataOutput output);

    public void produce();
}
