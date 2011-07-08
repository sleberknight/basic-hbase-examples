package com.nearinfinity.explore.hbase.samples.fakenames;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetSomeFakePeople {

    private static final String TABS = "\t\t\t\t\t";

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "fakenames");

        Get get = new Get(Bytes.toBytes("cochran-julie-f-248495"));
        Result result = table.get(get);
        List<KeyValue> list = result.list();
        System.out.printf("ROW%s%sCOLUMN+CELL\n", TABS, TABS);
        for (KeyValue keyValue : list) {
            System.out.printf("%s%scolumn=%s:%s, timestamp=%d, value=%s\n",
                    Bytes.toString(keyValue.getRow()),
                    TABS,
                    Bytes.toString(keyValue.getFamily()),
                    Bytes.toString(keyValue.getQualifier()),
                    keyValue.getTimestamp(),
                    Bytes.toString(keyValue.getValue()));
        }
    }
}
