package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class CreateJohnDoeFakename {

    private static final String TABS = "\t\t\t\t";

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "fakenames");

        byte[] rowKey = Bytes.toBytes("doe-john-m-12345");
        Put put = new Put(rowKey);
        put.add(Bytes.toBytes("personal"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
        put.add(Bytes.toBytes("personal"), Bytes.toBytes("mi"), Bytes.toBytes("M"));
        put.add(Bytes.toBytes("personal"), Bytes.toBytes("surame"), Bytes.toBytes("Doe"));
        put.add(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes("john.m.doe@gmail.com"));

        table.put(put);
        table.flushCommits();

        Get get = new Get(rowKey);
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        System.out.printf("ROW%s%sCOLUMN+CELL\n", TABS, TABS);
        for (Cell cell : cells) {
            System.out.printf("%s%scolumn=%s:%s, timestamp=%d, value=%s\n",
                    Bytes.toString(CellUtil.cloneRow(cell)),
                    TABS,
                    Bytes.toString(CellUtil.cloneFamily(cell)),
                    Bytes.toString(CellUtil.cloneQualifier(cell)),
                    cell.getTimestamp(),
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }

        table.close();
    }
}
