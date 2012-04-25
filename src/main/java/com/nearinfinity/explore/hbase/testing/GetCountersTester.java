package com.nearinfinity.explore.hbase.testing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class GetCountersTester {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "counters");
        long current = table.incrementColumnValue(Bytes.toBytes("20110705"), Bytes.toBytes("daily"),
                Bytes.toBytes("hits"), 1L);
        System.out.println("current = " + current);
    }
}
