/**
 * @Author : wzdnzd
 * @Time : 2019-06-08
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import org.apache.hadoop.io.WritableComparator;

public class TemperatureSort extends WritableComparator {
    public TemperatureSort() {
        super(Temperature.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        Temperature t1 = (Temperature) a;
        Temperature t2 = (Temperature) b;

        int i1 = Integer.compare(t1.getYear(), t2.getYear());
        if (i1 == 0) {
            int i2 = Integer.compare(t1.getMonth(), t2.getMonth());
            if (i2 == 0) {
                int i3 = Integer.compare(t1.getDay(), t2.getDay());
                if (i3 == 0)
                    return -Integer.compare(t1.getTemperature(), t2.getTemperature());
                else return i3;
            } else return i2;
        }
        return i1;
    }
}
