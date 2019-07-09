/**
 * @Author : wzdnzd
 * @Time : 2019-07-09
 * @Project : bigdata
 */


package com.wzdnzd.analysis;

import com.wzdnzd.analysis.tool.AnalysisBeanTool;
import org.apache.hadoop.util.ToolRunner;

public class Bootstrap {
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new AnalysisBeanTool(), args);
        System.out.println("+++++ " + result + " +++++");
    }
}
