/**
 * @Author : wzdnzd
 * @Time : 2019-06-13
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import com.sun.istack.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.Map;

class AdjacencyMatrix {
    private static Logger logger = Logger.getLogger(AdjacencyMatrix.class);

    public static class AdjacencyMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text k = new Text();
        private static Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = PageRankJob.DELIMITER.split(value.toString());
            k.set(tokens[0]);
            v.set(tokens[1]);

            context.write(k, v);
        }
    }

    public static class AdjacencyMatrixReducer extends Reducer<Text, Text, Text, Text> {
        private static Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float[] probabilities = new float[PageRankJob.N];
            Arrays.fill(probabilities, (1 - PageRankJob.d) / PageRankJob.N);

            float[] adj = new float[PageRankJob.N];
            int count = 0;
            for (Text val : values) {
                int idx = Integer.parseInt(val.toString());
                adj[idx - 1] = 1;
                count++;
            }

            if (count == 0) count = 1;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < adj.length; i++)
                sb.append(",").append(probabilities[i] + PageRankJob.d * adj[i] / count);


            v.set(sb.toString().substring(1));
            context.write(key, v);
        }
    }

    private static void upload(@NotNull FileSystem fs, @NotNull File src,
                               @NotNull String target, boolean overwrite) throws IOException {
        if (!src.exists())
            throw new IllegalArgumentException("file or directory not exists: " + src.getAbsolutePath());

        target = target.endsWith("/") ? target : target + "/";
        File[] files = src.isFile() ? new File[]{src} : src.listFiles();

        assert files != null && files.length > 0;

        boolean success;
        for (File file : files) {
            success = fileUpload(fs, file, new Path(target + file.getName()), overwrite);
            if (!success)
                logger.info("+++++ file " + file.getAbsolutePath() + " upload failed +++++");
        }
    }

    private static boolean fileUpload(@NotNull FileSystem fs, @NotNull File src,
                                      @NotNull Path target, boolean overwrite) throws IOException {
        if (!src.exists() || src.isDirectory() || (fs.exists(target) && !overwrite))
            return false;

        FSDataOutputStream outputStream = fs.create(target, overwrite);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(src));

        IOUtils.copyBytes(inputStream, outputStream, 1024, true);

        return true;
    }

    static boolean run(@NotNull Configuration conf, @NotNull Map<String, String> map)
            throws IOException, ClassNotFoundException, InterruptedException {
        String pageInput = map.get("pageInput");
        String prInput = map.get("prInput");
        String output = map.get("adjacency");

        String localPagePath = map.get("pageData");
        String localPRPath = map.get("prData");

        if (StringUtils.isBlank(pageInput) || StringUtils.isBlank(prInput) || StringUtils.isBlank(output)
                || StringUtils.isBlank(localPagePath) || StringUtils.isBlank(localPRPath))
            throw new IllegalArgumentException("invalid argument, 'pageInput' or 'prInput' or 'output' or 'pageData' or 'prData' cannot be null");

        FileSystem fs = FileSystem.get(conf);

        Path outputPath = new Path(output);
        if (fs.exists(outputPath))
            fs.delete(outputPath, true);

        upload(fs, new File(localPagePath), pageInput, false);
        upload(fs, new File(localPRPath), prInput, false);

        Job job = Job.getInstance(conf, "AdjacencyMatrix");
        job.setJarByClass(AdjacencyMatrix.class);

        if (map.containsKey("jarPath"))
            job.setJar(map.get("jarPath"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(AdjacencyMatrixMapper.class);
        job.setReducerClass(AdjacencyMatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(pageInput));
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }
}
