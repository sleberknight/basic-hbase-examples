package com.nearinfinity.explore.hbase.testing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateTableTester {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor("test_table");
        tableDescriptor.addFamily(new HColumnDescriptor("fam1"));
        tableDescriptor.addFamily(new HColumnDescriptor("fam2"));
        admin.createTable(tableDescriptor);
    }
}
