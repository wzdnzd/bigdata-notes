/**
 * @Author : wzdnzd
 * @Time : 2019-07-09
 * @Project : bigdata
 */


package com.wzdnzd.analysis.tool;

import com.wzdnzd.analysis.io.AnalysisKey;
import com.wzdnzd.analysis.io.AnalysisValue;
import com.wzdnzd.analysis.io.MySQLBeanOutputFormat;
import com.wzdnzd.analysis.mapper.AnalysisMapper;
import com.wzdnzd.analysis.reducer.AnalysisReduce;
import com.wzdnzd.constant.ConstantVal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class AnalysisBeanTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisBeanTool.class);

        Scan scan = new Scan();
        byte[] family = Bytes.toBytes(ConstantVal.HBASE_CF_CALLER.getVal());
        scan.addFamily(family);
        // scan.addColumn(family, Bytes.toBytes("duration"));

        TableMapReduceUtil.initTableMapperJob(ConstantVal.HBASE_TABLE.getVal(), scan,
                AnalysisMapper.class, AnalysisKey.class, Text.class, job);

        job.setReducerClass(AnalysisReduce.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);
        job.setOutputFormatClass(MySQLBeanOutputFormat.class);

        boolean flag = job.waitForCompletion(true);

        return flag ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
