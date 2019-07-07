/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.consumer.bean;

import com.wzdnzd.bean.BaseDao;
import com.wzdnzd.constant.ConstantVal;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDao extends BaseDao {
    public void init() throws IOException {
        connect();

        createNamespace(ConstantVal.NAMESPACE.getVal());
        boolean success = createTable(ConstantVal.HBASE_TABLE.getVal(), ConstantVal.COPROCESSOR_CLASS_NAME, true,
                ConstantVal.REGION_NUM, ConstantVal.HBASE_CF_CALLER.getVal(), ConstantVal.HBASE_CF_CALLED.getVal());

        if (!success) close();
    }

    public void insert(String value) throws IOException {
        if (value != null && !"".equals(value.trim())) {
            String[] contents = value.split(ConstantVal.DELIMITER.getVal());
            byte[] family = Bytes.toBytes(ConstantVal.HBASE_CF_CALLER.getVal());

            String rowKey = generateRegionNum(contents[0], contents[2]) + "_"
                    + contents[0] + "_" + contents[2] + "_" + contents[1] + "_1";

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(family, Bytes.toBytes("caller"), Bytes.toBytes(contents[0]));
            put.addColumn(family, Bytes.toBytes("called"), Bytes.toBytes(contents[1]));
            put.addColumn(family, Bytes.toBytes("time"), Bytes.toBytes(contents[2]));
            put.addColumn(family, Bytes.toBytes("duration"), Bytes.toBytes(contents[3]));

            insertData(ConstantVal.HBASE_TABLE.getVal(), put);
        }
    }
}
