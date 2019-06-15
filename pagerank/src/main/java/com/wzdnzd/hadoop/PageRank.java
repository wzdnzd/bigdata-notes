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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class PageRank {
    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text k = new Text();
        private static Text v = new Text();

        private String flag;

        @Override
        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = PageRankJob.DELIMITER.split(value.toString());

            if (flag.equals(PageRankJob.ADJACENCY)) {
                String row = tokens[0];
                for (int i = 1; i < tokens.length; i++) {
                    k.set(String.valueOf(i));
                    v.set("A:" + row + "," + tokens[i]);
                    context.write(k, v);
                }

            } else if (flag.equals(PageRankJob.PR)) {
                for (int i = 1; i <= PageRankJob.N; i++) {
                    k.set(String.valueOf(i));
                    v.set("B:" + tokens[0] + "," + tokens[1]);
                    context.write(k, v);
                }
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        private static Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Float> mapA = new HashMap<>();
            Map<Integer, Float> mapB = new HashMap<>();
            float pr = 0f;

            String content;
            for (Text line : values) {
                content = line.toString();
                String[] tokens = PageRankJob.DELIMITER.split(content.substring(2).trim());
                if (content.startsWith("A:")) {
                    mapA.put(Integer.parseInt(tokens[0]), Float.parseFloat(tokens[1]));
                }

                if (content.startsWith("B:")) {
                    mapB.put(Integer.parseInt(tokens[0]), Float.parseFloat(tokens[1]));
                }
            }

            for (int idx : mapA.keySet()) {
                float A = mapA.get(idx);
                float B = mapB.get(idx);
                pr += A * B;
            }

            v.set(PageRankJob.FORMAT.format(pr));
            context.write(key, v);
        }
    }

    static boolean run(@NotNull Configuration conf, @NotNull Map<String, String> map)
            throws IOException, ClassNotFoundException, InterruptedException {
        String adjInput = map.get("adjacency");
        String output = map.get("tmpOutput");
        String prInput = map.get("prInput");

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath))
            fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);

        if (map.containsKey("jarPath"))
            job.setJar(map.get("jarPath"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(adjInput), new Path(prInput));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);

        if (success) {
            fs.delete(new Path(prInput), true);
            fs.rename(outputPath, new Path(prInput));
        }

        return success;
    }
}
