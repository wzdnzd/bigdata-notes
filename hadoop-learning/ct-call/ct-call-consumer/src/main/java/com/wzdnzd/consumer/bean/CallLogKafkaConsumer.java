/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.consumer.bean;

import com.wzdnzd.bean.Consumer;
import com.wzdnzd.constant.ConstantVal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CallLogKafkaConsumer implements Consumer {
    @Override
    public void consume() {
        Properties properties = new Properties();
        String configFile = CallLogKafkaConsumer.class.getClassLoader().
                getResource("").getPath() + "kafka-consumer.properties";
        try {
            properties.load(new FileInputStream(configFile));
        } catch (IOException e) {
            throw new RuntimeException("illegal config file");
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(ConstantVal.KAFKA_TOPIC.getVal()));

        HBaseDao hBaseDao = new HBaseDao();
        try {
            hBaseDao.init();
        } catch (IOException e) {
            throw new RuntimeException("HBase init failed, please try again later");
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    hBaseDao.insert(record.value());
                } catch (IOException e) {
                    throw new RuntimeException("insert data to table failed");
                }
            }
        }
    }

    @Override
    public void close() {

    }
}
