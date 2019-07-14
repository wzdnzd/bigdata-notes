/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.utils;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NumberFormatUtil {
    public static String format(int num, int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++)
            builder.append("0");

        DecimalFormat df = new DecimalFormat(builder.toString());
        return df.format(num);
    }

    public static String format(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    public static Date parser(String time, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);

        Date date = null;

        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return date;
    }
}
