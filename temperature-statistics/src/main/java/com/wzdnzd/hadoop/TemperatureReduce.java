/**
 * @Author : wzdnzd
 * @Time : 2019-06-08
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureReduce extends Reducer<Temperature, IntWritable, Text, Text> {
    Text outputKey = new Text();
    Text outputVal = new Text();

    @Override
    protected void reduce(Temperature key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemp = Integer.MIN_VALUE, secondTemp = Integer.MIN_VALUE;
        int maxTempDay = 0, secondTempDay = 0;

        for (IntWritable ignored : values) {
            int temperature = key.getTemperature();

            if (temperature > maxTemp) {
                // save the previous maximum values
                secondTemp = maxTemp;
                secondTempDay = maxTempDay;

                maxTemp = temperature;
                maxTempDay = key.getDay();
            } else if (secondTemp < temperature && temperature < maxTemp) {
                secondTemp = temperature;
                secondTempDay = key.getDay();
            }
        }

        outputKey.set(key.getYear() + "-" + key.getMonth() + "-" + maxTempDay);
        outputVal.set(maxTemp + "C");
        context.write(outputKey, outputVal);

        if (secondTempDay != 0) {
            outputKey.set(key.getYear() + "-" + key.getMonth() + "-" + secondTempDay);
            outputVal.set(secondTemp + "C");
            context.write(outputKey, outputVal);
        }
    }
}
