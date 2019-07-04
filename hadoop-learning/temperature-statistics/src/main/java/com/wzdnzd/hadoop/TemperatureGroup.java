/**
 * @Author : wzdnzd
 * @Time : 2019-06-08
 * @Project : bigdata
 */


package com.wzdnzd.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TemperatureGroup extends WritableComparator {
    public TemperatureGroup() {
        super(Temperature.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Temperature t1 = (Temperature) a;
        Temperature t2 = (Temperature) b;

        int i1 = Integer.compare(t1.getYear(), t2.getYear());
        if (i1 == 0)
            return Integer.compare(t1.getMonth(), t2.getMonth());
        return i1;
    }
}
