/**
 * @Author : wzdnzd
 * @Time : 2019-07-08
 * @Project : bigdata
 */


package com.wzdnzd.analysis.reducer;

import com.wzdnzd.analysis.io.AnalysisKey;
import com.wzdnzd.analysis.io.AnalysisValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnalysisReduce extends Reducer<AnalysisKey, Text, AnalysisKey, AnalysisValue> {
    @Override
    protected void reduce(AnalysisKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalCall = 0;
        long totalDuration = 0;

        for (Text value : values) {
            totalDuration += Long.parseLong(value.toString());
            totalCall += 1;
        }

        context.write(key, new AnalysisValue(totalCall, totalDuration));
    }
}
