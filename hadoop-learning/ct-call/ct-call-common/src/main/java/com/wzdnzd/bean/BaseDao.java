/**
 * @Author : wzdnzd
 * @Time :  2019-07-06
 * @Project : bigdata
 */

package com.wzdnzd.bean;


import com.wzdnzd.constant.ConstantVal;
import com.wzdnzd.utils.NumberFormatUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public abstract class BaseDao {
    private ThreadLocal<Connection> connHolder = new ThreadLocal<>();
    private Admin admin = null;

    protected Connection connect() throws IOException {
        Connection connection = connHolder.get();
        if (connection == null) {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();

            connHolder.set(connection);
        }
        return connection;
    }

    protected synchronized Admin getAdmin() {
        if (admin == null) {
            try {
                connect();
            } catch (IOException e) {
                throw new RuntimeException("cannot connect to HBase server, please try again later");
            }
        }
        return admin;
    }

    protected void createNamespace(String namespace) throws IOException {
        admin = getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        }
    }

    protected boolean createTable(String table, String className, boolean overwrite, int regionNum, String... families) throws IOException {
        TableName tableName = TableName.valueOf(table);
        admin = getAdmin();

        if (admin.tableExists(tableName))
            if (!overwrite) return false;

            else deleteTable(table);

        if (families == null || families.length == 0)
            families = new String[]{ConstantVal.HBASE_CF_DEFAULT.getVal()};

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        List<ColumnFamilyDescriptor> list = new ArrayList<>(families.length);

        for (String family : families) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            list.add(columnFamilyDescriptor);
        }

        tableDescriptorBuilder.setColumnFamilies(list);

        if (className != null && !"".equals(className)) {
            CoprocessorDescriptorBuilder coprocessorDescriptorBuilder = CoprocessorDescriptorBuilder.newBuilder(className);
            coprocessorDescriptorBuilder.setJarPath(ConstantVal.COPROCESSOR_JAR_PATH);

            tableDescriptorBuilder.setCoprocessor(coprocessorDescriptorBuilder.build());
        }

        TableDescriptor descriptor = tableDescriptorBuilder.build();

        if (regionNum <= 1)
            admin.createTable(descriptor);
        else
            admin.createTable(descriptor, generateKeySplits(regionNum));

        return true;
    }

    private byte[][] generateKeySplits(int num) {
        int splitKeyCount = num - 1;
        byte[][] bytes = new byte[splitKeyCount][];
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey = i + "|";
            list.add(Bytes.toBytes(splitKey));
        }

        list.toArray(bytes);

        return bytes;
    }

    protected int generateRegionNum(String tel, String date) {
        if (tel == null || tel.length() < 4 || date == null || date.length() < 6)
            return 0;

        int crc = (tel.substring(tel.length() - 4).hashCode()) ^ (date.substring(0, 6).hashCode());
        crc = crc >= 0 ? crc : -crc;

        return crc % ConstantVal.REGION_NUM;
    }

    protected void deleteTable(String table) throws IOException {
        admin = getAdmin();
        TableName tableName = TableName.valueOf(table);

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    protected void insertData(String table, Put put) throws IOException {
        TableName tableName = TableName.valueOf(table);
        Table tbl = connect().getTable(tableName);

        tbl.put(put);
    }

    protected List<String[]> getRowKeyRegion(String tel, String start, String end) {
        if (tel == null || tel.length() < 4 || start == null || start.length() < 6 || end == null || end.length() < 6)
            throw new IllegalArgumentException("illegal arguments, params 'tel' 'start', 'end' cannot be null");

        String format = "yyyyMM";

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(NumberFormatUtil.parser(start.substring(0, 6), format));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(NumberFormatUtil.parser(end.substring(0, 6), format));

        if (startCal.getTimeInMillis() >= endCal.getTimeInMillis())
            throw new IllegalArgumentException("param 'start' must be less than 'end'");

        List<String[]> list = new ArrayList<>();

        while (startCal.getTimeInMillis() < endCal.getTimeInMillis()) {
            String now = NumberFormatUtil.format(startCal.getTime(), format);
            int regionNum = generateRegionNum(tel, now);
            String rowKey = regionNum + "_" + tel + "_" + now;

            list.add(new String[]{rowKey, rowKey + "|"});

            startCal.add(Calendar.MONTH, 1);
        }

        return list;
    }

    protected void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connHolder.get() != null) {
            try {
                connHolder.get().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            connHolder.remove();
        }
    }
}
