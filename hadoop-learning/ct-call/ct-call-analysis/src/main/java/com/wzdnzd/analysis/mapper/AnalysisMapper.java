/**
 * @Author : wzdnzd
 * @Time : 2019-07-08
 * @Project : bigdata
 */


package com.wzdnzd.analysis.mapper;

import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

public class AnalysisMapper extends TableMapper {
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
