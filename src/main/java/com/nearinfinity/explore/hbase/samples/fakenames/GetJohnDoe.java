package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class GetJohnDoe {

    private static final String TABS = "\t\t\t\t";

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "fakenames");

        Get get = new Get(Bytes.toBytes("doe-john-m-12345"));
        get.addFamily(Bytes.toBytes("personal"));
        get.setMaxVersions(3);
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            System.out.printf("%s%scolumn=%s:%s, timestamp=%d, value=%s\n",
                    Bytes.toString(CellUtil.cloneRow(cell)),
                    TABS,
                    Bytes.toString(CellUtil.cloneFamily(cell)),
                    Bytes.toString(CellUtil.cloneQualifier(cell)),
                    cell.getTimestamp(),
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }

//        NavigableMap<byte[], NavigableMap<byte[],NavigableMap<Long,byte[]>>> map = result.getMap();

        table.close();
    }
}
