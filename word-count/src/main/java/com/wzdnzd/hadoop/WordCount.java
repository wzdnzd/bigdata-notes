/**
 * @Author : wzdnzd
 * @Time : 2019-06-06
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.*;

public class WordCount {
    private static Logger logger = Logger.getLogger(WordCount.class);

    public static void main(String[] args) {
        Configuration conf = new Configuration();

        try {
            String path = "/learn/data/wordcount";
            Path inputPath = new Path(path);
            Path outputPath = new Path("/learn/result/wordcount");


            FileSystem fs = FileSystem.get(conf);

            // fs.deleteOnExit(path);
            if (fs.exists(outputPath))
                fs.delete(outputPath, true);

            String dataPath = WordCount.class.getResource("/").getPath() + "data";
            File dir = new File(dataPath);

            if (dir.exists() && dir.isDirectory()) {
                File[] files = dir.listFiles();

                assert files != null && files.length > 0;

                for (File file : files) {
                    boolean success = fileUpload(fs, file, new Path(path + "/" + file.getName()));
                    if (!success)
                        logger.info("*****" + "\t" + "file " + file.getName() + " upload failed" + "\t" + "*****");
                }
            }

            Job job = Job.getInstance(conf, "WordCount");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(SumReducer.class);
            job.setReducerClass(SumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static boolean fileUpload(FileSystem fs, File src, Path target) throws IOException {
        if (fs == null || src == null || !src.exists() || target == null || fs.exists(target))
            return false;

        FSDataOutputStream outputStream = fs.create(target);
        InputStream inputStream = new BufferedInputStream(new FileInputStream(src));

        IOUtils.copyBytes(inputStream, outputStream, 1024, false);

        return true;
    }
}
