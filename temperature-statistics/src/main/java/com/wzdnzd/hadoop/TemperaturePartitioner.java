/**
 * @Author : wzdnzd
 * @Time : 2019-06-08
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TemperaturePartitioner extends Partitioner<Temperature, IntWritable> {
    @Override
    public int getPartition(Temperature temperature, IntWritable intWritable, int i) {
        if (i <= 0)
            throw new IllegalArgumentException("argument 'i' must great than 0.");

        return temperature.getYear() % i;
    }
}
