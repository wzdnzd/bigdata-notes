/**
 * @Author : wzdnzd
 * @Time : 2019-06-13
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class PageRankJob {
    static final Pattern DELIMITER = Pattern.compile("[\t,]");
    static final int N = 4;
    static final float d = 0.85f;

    static final String ADJACENCY = "adjacency";
    static final String PR = "pr";

    private static final int EPOCH = 15;

    static String scaleFloat(float f) {
        return new DecimalFormat("##0.000000").format(f);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String dataPath = PageRankJob.class.getResource("/").getPath() + "data";

        Configuration conf = new Configuration();

        Map<String, String> map = new HashMap<>();
        map.put("pageData", dataPath + "/page");
        map.put("prData", dataPath + "/init-pr");
        map.put("pageInput", "/learn/data/pagerank/pages");
        map.put("prInput", "/learn/data/pagerank/" + PR);
        map.put("adjacency", "/learn/data/pagerank/" + ADJACENCY);
        map.put("tmpOutput", "/learn/data/pagerank/tmp");
        map.put("result", "/learn/result/pagerank");

        boolean success = AdjacencyMatrix.run(conf, map);
        if (!success) System.exit(-1);

        for (int i = 0; i < EPOCH; i++) {
            success = PageRank.run(conf, map);
            if (!success) System.exit(-1);
        }
        System.exit(Normalize.run(conf, map) ? 0 : -1);
    }
}
