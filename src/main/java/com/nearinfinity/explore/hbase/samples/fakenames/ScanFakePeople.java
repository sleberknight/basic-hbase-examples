package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class ScanFakePeople {

    private static final byte[] ZERO = new byte[]{ 0x00 };
    private static final String TABS = "\t\t\t\t\t\t";

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

        // First 25 people whose last name is smith, only retrieving email addresses and given name
        Scan scan = new Scan(Bytes.toBytes("smith-"));
        scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"));
        scan.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
        scan.setFilter(new PageFilter(25));
        ResultScanner scanner = table.getScanner(scan);
        System.out.printf("ROW%s%sCOLUMN+CELL\n", TABS, TABS);
        for (Result result : scanner) {
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
        }

        // All people with last name smith paginated using a Filter
//        Scan pageScan1 = new Scan(Bytes.toBytes("smith-"));
//        Filter pageFilter = new PageFilter(10);
//        pageScan1.setFilter(pageFilter);
//        byte[] lastRowKeyOnPage = null;
//
//        ResultScanner scanner1 = table.getScanner(pageScan1);
//        for (Result result : scanner1) {
//            System.out.println("result = " + result);
//            lastRowKeyOnPage = result.getRow();
//            count++;
//        }
//        System.out.printf("Scanned %d results\n", count);
//        System.out.printf("Last row was %s\n", Bytes.toString(lastRowKeyOnPage));
//
//        Scan pageScan2 = new Scan(Bytes.add(lastRowKeyOnPage, ZERO));
//        pageScan2.setFilter(pageFilter);
//
//        ResultScanner scanner2 = table.getScanner(pageScan2);
//        for (Result result : scanner2) {
//            System.out.println("result = " + result);
//            lastRowKeyOnPage = result.getRow();
//            count++;
//        }
//        System.out.printf("Scanned %d results\n", count);
//        System.out.printf("Last row was %s\n", Bytes.toString(lastRowKeyOnPage));

        table.close();
    }
}
