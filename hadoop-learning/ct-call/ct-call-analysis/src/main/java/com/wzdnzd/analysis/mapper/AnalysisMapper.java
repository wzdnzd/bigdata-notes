/**
 * @Author : wzdnzd
 * @Time : 2019-07-08
 * @Project : bigdata
 */


package com.wzdnzd.analysis.mapper;

import com.wzdnzd.analysis.io.AnalysisKey;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class AnalysisMapper extends TableMapper<AnalysisKey, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowKey = Bytes.toString(key.get());
        String[] contents = rowKey.split("_");

        Cell[] cells = value.rawCells();
        assert cells != null && cells.length > 0;

        Text duration = null;
        for (Cell cell : cells) {
            if ("duration".equals(CellUtil.cloneQualifier(cell))) {
                duration = new Text(Bytes.toString(CellUtil.cloneValue(cells[0])));
            }
        }
        if (duration != null) {
            // day
            context.write(new AnalysisKey(contents[1], contents[2].substring(0, 8)), duration);
            // month
            context.write(new AnalysisKey(contents[1], contents[2].substring(0, 6)), duration);
            // year
            context.write(new AnalysisKey(contents[1], contents[2].substring(0, 4)), duration);

            // day
            context.write(new AnalysisKey(contents[3], contents[2].substring(0, 8)), duration);
            // month
            context.write(new AnalysisKey(contents[3], contents[2].substring(0, 6)), duration);
            // year
            context.write(new AnalysisKey(contents[3], contents[2].substring(0, 4)), duration);
        }
    }
}