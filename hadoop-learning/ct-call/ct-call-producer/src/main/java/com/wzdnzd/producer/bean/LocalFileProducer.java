/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.producer.bean;

import com.wzdnzd.bean.DataInput;
import com.wzdnzd.bean.DataOutput;
import com.wzdnzd.bean.Producer;
import com.wzdnzd.utils.NumberFormatUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class LocalFileProducer implements Producer {
    private DataInput input;
    private DataOutput output;
    private volatile boolean flag = true;

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    @Override
    public void setInput(DataInput input) {
        this.input = input;
    }

    @Override
    public void setOutput(DataOutput output) {
        this.output = output;
    }

    @Override
    public void produce() {
        String format = "yyyyMMddHHmmss";

        try {
            List<Contact> contacts = input.read(Contact.class);

            if (contacts.size() <= 1)
                flag = false;

            Random random = new Random();

            while (flag) {
                int i1 = random.nextInt(contacts.size());
                int i2 = random.nextInt(contacts.size());

                while (i1 == i2)
                    i2 = random.nextInt(contacts.size());

                Contact caller = contacts.get(i1);
                Contact called = contacts.get(i2);

                long start = NumberFormatUtil.parser("20190101000000", format).getTime();
                long end = new Date().getTime();

                String callTime = NumberFormatUtil.format(new Date(start + (long) ((end - start) * Math.random())), format);
                String duration = NumberFormatUtil.format(random.nextInt(2 * 60 * 60), 4);

                CallLog callLog = new CallLog(caller.getPhoneNumber(), called.getPhoneNumber(), callTime, duration);
                output.write(callLog);
                Thread.sleep(500);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public DataInput getInput() {
        return this.input;
    }

    public DataOutput getOutput() {
        return this.output;
    }

    @Override
    public void close() throws IOException {
        if (input != null)
            input.close();

        if (output != null)
            output.close();
    }
}
