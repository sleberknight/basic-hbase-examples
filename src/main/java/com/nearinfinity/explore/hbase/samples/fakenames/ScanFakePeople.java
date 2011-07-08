package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanFakePeople {

    private static final byte[] ZERO = new byte[]{ 0x00 };

    // TODO Do various scans, optionally using some filters
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "fakenames");

        int count = 0;

        // People with last name smith and first name starting with 'a'
//        Scan scan = new Scan(Bytes.toBytes("smith-a"), Bytes.toBytes("smith-b"));

        // All people with last name smith
//        char dashPlusOne = '-' + 1;
//        Scan scan = new Scan(Bytes.toBytes("smith-"), Bytes.toBytes("smith" + dashPlusOne));

        // All people whose last name starts with smith or whose last name is after than smith (lexicographically)
//        Scan scan = new Scan(Bytes.toBytes("smith-"));

//        ResultScanner scanner = table.getScanner(scan);
//        for (Result result : scanner) {
//            System.out.println("result = " + result);
//            count++;
//        }
//        System.out.printf("Scanned %d results\n", count);


        // All people with last name smith paginated using a Filter
        Scan pageScan1 = new Scan(Bytes.toBytes("smith-"));
        Filter pageFilter = new PageFilter(10);
        pageScan1.setFilter(pageFilter);
        byte[] lastRowKeyOnPage = null;

        ResultScanner scanner1 = table.getScanner(pageScan1);
        for (Result result : scanner1) {
            System.out.println("result = " + result);
            lastRowKeyOnPage = result.getRow();
            count++;
        }
        System.out.printf("Scanned %d results\n", count);
        System.out.printf("Last row was %s\n", Bytes.toString(lastRowKeyOnPage));

        Scan pageScan2 = new Scan(Bytes.add(lastRowKeyOnPage, ZERO));
        pageScan2.setFilter(pageFilter);

        ResultScanner scanner2 = table.getScanner(pageScan2);
        for (Result result : scanner2) {
            System.out.println("result = " + result);
            lastRowKeyOnPage = result.getRow();
            count++;
        }
        System.out.printf("Scanned %d results\n", count);
        System.out.printf("Last row was %s\n", Bytes.toString(lastRowKeyOnPage));
    }
}
