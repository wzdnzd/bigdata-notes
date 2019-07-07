/**
 * @Author : wzdnzd
 * @Time :  2019-07-07
 * @Project : bigdata
 */

package com.wzdnzd.coprocessor;

import com.wzdnzd.bean.BaseDao;
import com.wzdnzd.constant.ConstantVal;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class DataInsertCoprocessor implements RegionCoprocessor, RegionObserver {
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put,
                        WALEdit edit, Durability durability) throws IOException {
        Table table = c.getEnvironment().getConnection().getTable(TableName.valueOf(ConstantVal.HBASE_TABLE.getVal()));
        byte[] family = Bytes.toBytes(ConstantVal.HBASE_CF_CALLED.getVal());

        String row = Bytes.toString(put.getRow());
        String[] contents = row.split("_");

        if ("1".equals(contents[4])) {
            CoprocessorDao coprocessorDao = new CoprocessorDao();
            int regionNum = coprocessorDao.getRegionNum(contents[1], contents[3]);
            String rowKey = regionNum + "_" + contents[3] + "_" + contents[2] + "_" + contents[1] + "_0";

            Put data = new Put(Bytes.toBytes(rowKey));
            data.addColumn(family, Bytes.toBytes("caller"), Bytes.toBytes(contents[1]));
            data.addColumn(family, Bytes.toBytes("called"), Bytes.toBytes(contents[2]));
            data.addColumn(family, Bytes.toBytes("time"), Bytes.toBytes(contents[3]));

            List<Cell> cells = put.get(Bytes.toBytes(ConstantVal.HBASE_CF_CALLER.getVal()), Bytes.toBytes("duration"));
            if (cells != null && cells.size() > 0) {
                put.addColumn(family, Bytes.toBytes("duration"), cells.get(0).getValueArray());
            }

            table.put(data);
        }

        table.close();
    }

    private class CoprocessorDao extends BaseDao {
        int getRegionNum(String tel, String time) {
            return generateRegionNum(tel, time);
        }
    }
}


