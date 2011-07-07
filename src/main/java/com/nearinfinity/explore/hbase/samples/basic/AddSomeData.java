package com.nearinfinity.explore.hbase.samples.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class AddSomeData {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "my-table");
        Put put = new Put(Bytes.toBytes("row1"));
        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("value1"));
        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("value2"));
        put.add(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"), Bytes.toBytes("value3"));
        table.put(put);
        table.flushCommits();
        table.close();
    }
}
