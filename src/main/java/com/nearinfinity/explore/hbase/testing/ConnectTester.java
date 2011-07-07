package com.nearinfinity.explore.hbase.testing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class ConnectTester {


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor[] tables = admin.listTables();
        System.out.printf("Found %d table(s)\n", tables.length);
        for (HTableDescriptor table : tables) {
            System.out.printf("%s\n", table.getNameAsString());
        }
    }
}
