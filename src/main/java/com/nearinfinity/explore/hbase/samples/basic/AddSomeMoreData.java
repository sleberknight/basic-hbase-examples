package com.nearinfinity.explore.hbase.samples.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class AddSomeMoreData {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "my-table");
        Put put = new Put(Bytes.toBytes("row1"));
        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("value1-a"));
        put.add(Bytes.toBytes("colfam3"), Bytes.toBytes("qual1"), Bytes.toBytes("value4"));
        table.put(put);
        table.flushCommits();
        table.close();
    }
}
