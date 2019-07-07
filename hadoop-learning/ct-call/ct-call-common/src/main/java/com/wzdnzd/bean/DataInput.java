/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface DataInput extends Closeable {
    public void setInputPath(String path);

    public String getInputPath();

    public Object read() throws IOException;

    public <T extends DataObject> List<T> read(Class<T> clazz) throws IOException;
}
