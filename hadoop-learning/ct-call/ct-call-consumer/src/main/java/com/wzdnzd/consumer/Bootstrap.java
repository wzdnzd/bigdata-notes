/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.consumer;

import com.wzdnzd.bean.Consumer;
import com.wzdnzd.consumer.bean.CallLogKafkaConsumer;

import java.io.IOException;

public class Bootstrap {
    public static void main(String[] args) throws IOException {
        Consumer consumer = new CallLogKafkaConsumer();
        consumer.consume();
        consumer.close();
    }
}
