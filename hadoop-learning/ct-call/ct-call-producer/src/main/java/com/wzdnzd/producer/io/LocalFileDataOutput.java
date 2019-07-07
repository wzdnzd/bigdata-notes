/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.producer.io;

import com.wzdnzd.bean.DataOutput;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class LocalFileDataOutput implements DataOutput {
    private String outputPath;
    private PrintWriter writer = null;

    public LocalFileDataOutput(String path) {
        this.setOutputPath(path);
    }

    @Override
    public void setOutputPath(String path) {
        this.outputPath = path;
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getOutputPath() {
        return outputPath;
    }

    @Override
    public void write(Object data) {
        write(data.toString());
    }

    @Override
    public void write(String data) {
        writer.println(data);
        writer.flush();
    }


    @Override
    public void close() {
        if (writer != null)
            writer.close();
    }
}
