/**
 * @Author : wzdnzd
 * @Time : 2019-06-13
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import com.sun.istack.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class Normalize {
    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text k = new Text("1");

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            context.write(k, values);
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        private static Text k = new Text();
        private static Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> vList = new ArrayList<>();

            float sum = 0f;
            String[] tokens;
            for (Text line : values) {
                vList.add(line.toString());

                tokens = PageRankJob.DELIMITER.split(line.toString());
                float f = Float.parseFloat(tokens[1]);
                sum += f;
            }

            for (String line : vList) {
                tokens = PageRankJob.DELIMITER.split(line);
                k.set(tokens[0]);

                float f = Float.parseFloat(tokens[1]);
                v.set(PageRankJob.FORMAT.format(f / sum));
                context.write(k, v);
            }
        }
    }

    static boolean run(@NotNull Configuration conf, @NotNull Map<String, String> map)
            throws IOException, InterruptedException, ClassNotFoundException {
        String input = map.get("prInput");
        String output = map.get("result");

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath))
            fs.delete(outputPath, true);


        Job job = Job.getInstance(conf, "Normalize");
        job.setJarByClass(Normalize.class);

        if (map.containsKey("jarPath"))
            job.setJar(map.get("jarPath"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);

    }
}
