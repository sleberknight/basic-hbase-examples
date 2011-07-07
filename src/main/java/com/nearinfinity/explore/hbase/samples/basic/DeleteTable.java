package com.nearinfinity.explore.hbase.samples.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class DeleteTable {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        System.out.println("Disabling table...");
        admin.disableTable("my-table");

        System.out.println("Deleting table...");
        admin.deleteTable("my-table");
    }
}
