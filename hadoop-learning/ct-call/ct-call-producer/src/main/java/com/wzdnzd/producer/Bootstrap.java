/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.producer;

import com.wzdnzd.producer.bean.LocalFileProducer;
import com.wzdnzd.producer.io.LocalFileDataInput;
import com.wzdnzd.producer.io.LocalFileDataOutput;

import java.io.IOException;

public class Bootstrap {
    public static void main(String[] args) throws IOException {
        if (args.length != 2)
            throw new IllegalArgumentException("illegal arguments, correct format is 'java -jar package.jar input output'");

        // String basePath = Bootstrap.class.getResource("/data").getPath();
        // String inputPath = basePath + "/contact.log";
        // String outputPath = basePath + "/call.log";

        String inputPath = args[0];
        String outputPath = args[1];

        LocalFileProducer producer = new LocalFileProducer();
        producer.setInput(new LocalFileDataInput(inputPath));
        producer.setOutput(new LocalFileDataOutput(outputPath));

        producer.produce();

        producer.close();
    }
}
