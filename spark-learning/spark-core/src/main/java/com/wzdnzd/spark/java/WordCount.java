/**
 * @Author : wzdnzd
 * @Time : 2019-07-04
 * @Project : bigdata
 */


package com.wzdnzd.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    private static SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");

    public static void main(String[] args) {
        // String dataPath = WordCount.class.getResource("/").getPath() + "/data/text1";
        String dataPath = "hdfs://hadoop-namenode-01:9000/learn/data/wordcount";

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // fix java.io.IOException: No FileSystem for scheme : hdfs
        sc.hadoopConfiguration().set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        sc.hadoopConfiguration().set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        JavaRDD<String> rdd = sc.textFile(dataPath).flatMap((FlatMapFunction<String, String>) line ->
                (Arrays.asList(line.replaceAll("[^a-z0-9A-Z\\-]", " ").split(" ")).iterator()));

        JavaPairRDD<String, Integer> pairs = rdd.filter((Function<String, Boolean>) s ->
                !"".equals(s)).mapToPair((PairFunction<String, String, Integer>) word ->
                new Tuple2<>(word.toLowerCase(), 1));

        pairs.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum).
                foreach((VoidFunction<Tuple2<String, Integer>>) stringIntegerTuple2 ->
                        System.out.println(stringIntegerTuple2._1 + "\t" + stringIntegerTuple2._2));

        sc.stop();
    }
}
