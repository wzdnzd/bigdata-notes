/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.producer.io;

import com.wzdnzd.bean.DataInput;
import com.wzdnzd.bean.DataObject;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class LocalFileDataInput implements DataInput {
    private String inputPath;
    private BufferedReader reader = null;

    public LocalFileDataInput(String path) {
        this.setInputPath(path);
    }

    @Override
    public void setInputPath(String path) {
        this.inputPath = path;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public String getInputPath() {
        return this.inputPath;
    }

    @Override
    public Object read() {
        return null;
    }

    @Override
    public <T extends DataObject> List<T> read(Class<T> clazz) throws IOException {
        List<T> list = new ArrayList<>();
        String line;

        while ((line = reader.readLine()) != null) {
            try {
                T t = clazz.getDeclaredConstructor().newInstance();
                t.setVal(line);
                list.add(t);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                e.printStackTrace();
            }
        }

        return list;
    }

    @Override
    public void close() throws IOException {
        if (reader != null)
            reader.close();
    }
}
