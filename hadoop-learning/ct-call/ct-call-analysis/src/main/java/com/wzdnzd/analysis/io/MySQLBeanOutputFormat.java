/**
 * @Author : wzdnzd
 * @Time : 2019-07-09
 * @Project : bigdata
 */


package com.wzdnzd.analysis.io;

import com.wzdnzd.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class MySQLBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {
    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey, AnalysisValue> {
        private Connection connection = JDBCUtil.connect();

        @Override
        public void write(AnalysisKey analysisKey, AnalysisValue analysisValue) {
            PreparedStatement statement = null;
            try {
                String insertSQL = "insert into ct_call (telephone, call_date, total_call, total_duration ) values ( ?, ?, ?, ? )";
                statement = connection.prepareStatement(insertSQL);

                statement.setString(1, analysisKey.getTel());
                statement.setString(2, analysisKey.getDate());
                statement.setInt(3, analysisValue.getTotalCall());
                statement.setLong(4, analysisValue.getTotalDuration());

                statement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        return new MySQLRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) {

    }

    private FileOutputCommitter committer = null;

    private static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException {
        if (committer == null) {
            Path output = getOutputPath(taskAttemptContext);
            committer = new FileOutputCommitter(output, taskAttemptContext);
        }
        return committer;
    }
}
