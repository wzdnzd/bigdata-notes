/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.bean;

import java.io.Closeable;

public interface Consumer extends Closeable {
    public void consume();
}
