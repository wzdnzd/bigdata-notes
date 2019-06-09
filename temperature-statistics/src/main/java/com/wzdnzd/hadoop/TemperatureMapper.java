/**
 * @Author : wzdnzd
 * @Time : 2019-06-08
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TemperatureMapper extends Mapper<Object, Text, Temperature, IntWritable> {
    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    Temperature weather = new Temperature();
    IntWritable value = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] contents = value.toString().split("\t");
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(FORMAT.parse(contents[0].split(" ")[0]));
            weather.setYear(calendar.get(Calendar.YEAR));
            weather.setMonth(calendar.get(Calendar.MONTH) + 1);
            weather.setDay(calendar.get(Calendar.DAY_OF_MONTH));

            int temperature = Integer.parseInt(contents[1].substring(0, contents[1].length() - 1));
            weather.setTemperature(temperature);

            this.value.set(temperature);

            context.write(weather, this.value);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
