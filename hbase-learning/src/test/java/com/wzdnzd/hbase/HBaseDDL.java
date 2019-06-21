/**
 * @Author : wzdnzd
 * @Time : 2019-06-20
 * @Project : bigdata
 */


package com.wzdnzd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class HBaseDDL {
    private static Logger logger = Logger.getLogger(HBaseDDL.class);

    private Admin admin = null;
    private Connection connection = null;
    private String family = "cf";
    private String table = "orders";

    @Before
    public void conn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        // conf.set("hbase.zookeeper.quorum", "hadoop-datanode-01,hadoop-datanode-02,hadoop-datanode-03");

        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    @Test
    public void createTable() throws IOException {
        TableName tableName = TableName.valueOf(table);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            logger.info("+++++ table " + table + " already exists, delete it +++++");
            admin.deleteTable(tableName);
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(family));

        tableDescriptorBuilder.setColumnFamily(columnDescBuilder.build());
        TableDescriptor descriptor = tableDescriptorBuilder.build();

        admin.createTable(descriptor);
    }

    @Test
    public void insertData() throws IOException {
        Table orders = connection.getTable(TableName.valueOf(this.table));
        Put put = new Put(Bytes.toBytes(1));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes("XiaoMing"));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(13));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("address"), Bytes.toBytes("BeiJing"));

        orders.put(put);
        logger.info("+++++ data has been insert into table " + table + " +++++");

        orders.close();
    }

    @Test
    public void getData() throws IOException {
        Table orders = connection.getTable(TableName.valueOf(this.table));
        Get get = new Get(Bytes.toBytes(1));
        Result result = orders.get(get);

        if (result != null) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                logger.info("+++++ " + Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell))
                        + "\t" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SS").format(cell.getTimestamp()));
            }
        }
    }

    @Test
    public void deleteData() throws IOException {
        Table orders = connection.getTable(TableName.valueOf(this.table));
        Delete delete = new Delete(Bytes.toBytes(1));
        orders.delete(delete);

        logger.info("+++++ data has been delete from table " + table + " +++++");
    }

    @Test
    public void dropTable() throws IOException {
        TableName orders = TableName.valueOf(table);
        admin.disableTable(orders);
        admin.deleteTable(orders);

        logger.info("+++++ table " + table + " has been drop +++++");
    }

    @After
    public void close() throws IOException {
        if (admin != null)
            admin.close();
        if (connection != null)
            connection.close();
    }
}
