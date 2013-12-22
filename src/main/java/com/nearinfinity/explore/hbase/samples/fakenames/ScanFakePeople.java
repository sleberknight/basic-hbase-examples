package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class ScanFakePeople {

    private static final byte[] ZERO = new byte[]{0x00};
    private static final String TABS = "\t\t\t\t\t\t";

    public static void printScanResult(ResultScanner scanner) {
        int count = 0;
        for (Result result : scanner) {
            System.out.println("result = " + result);
            count++;
        }
        System.out.printf("Scanned %d results\n", count);
    }

    public static void scanPeopleHavingLastNameSmith(HTable table)
            throws IOException {

        // All people with last name smith
        char dashPlusOne = '-' + 1;
        Scan scan = new Scan(Bytes.toBytes("smith-"), Bytes.toBytes("smith" + dashPlusOne));
        ResultScanner scanner = table.getScanner(scan);
        printScanResult(scanner);
    }

    public static void scanPeopleHavingLastNameSmithAndFirstNameStartsWithLetterA(HTable table)
            throws IOException {

        // People with last name smith and first name starting with 'a'
        Scan scan = new Scan(Bytes.toBytes("smith-a"), Bytes.toBytes("smith-b"));
        ResultScanner scanner = table.getScanner(scan);
        printScanResult(scanner);
    }

    public static void scanPeopleHavingLastNameSmithOrAfterSmith(HTable table)
            throws IOException {

        // All people whose last name starts with smith or whose last name is
        // after than smith (lexicographically)
        Scan scan = new Scan(Bytes.toBytes("smith-"));
        ResultScanner scanner = table.getScanner(scan);
        printScanResult(scanner);
    }

    public static void scanPeopleFirst25SmithsReturningEmailsAndGivenName(HTable table)
            throws IOException {

        // First 25 people whose last name is smith, only retrieving email addresses
        // and given name
        Scan scan = new Scan(Bytes.toBytes("smith-"));
        scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"));
        scan.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
        scan.setFilter(new PageFilter(25));
        ResultScanner scanner = table.getScanner(scan);
        System.out.printf("ROW%s%sCOLUMN+CELL\n", TABS, TABS);
        int count = 0;
        for (Result result : scanner) {
            count++;
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
        System.out.printf("Scanned %d results\n", count);
    }

    public static void scanPeopleHavingLastNameSmithGetFirstTwoPages(HTable table)
            throws IOException {

        // All people with last name smith paginated using a Filter...

        // First 10 results
        Scan pageScan1 = new Scan(Bytes.toBytes("smith-"));
        Filter pageFilter = new PageFilter(10);
        pageScan1.setFilter(pageFilter);
        byte[] lastRowKeyOnPage = null;

        ResultScanner scanner1 = table.getScanner(pageScan1);
        int count = 0;
        for (Result result : scanner1) {
            System.out.println("result = " + result);
            lastRowKeyOnPage = result.getRow();
            count++;
        }
        System.out.printf("Scanned %d results\n", count);
        System.out.printf("Last row was %s\n", Bytes.toString(lastRowKeyOnPage));

        // Get next page of 10 results (starts with lastRowKeyOnPage)
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

    private static int getChoice() throws IOException {
        System.out.print("Choice? ");
        String line = readInputLine();
        int choice;
        try {
            choice = Integer.parseInt(line);
        } catch (NumberFormatException ex) {
            choice = -1;
        }
        return choice;
    }

    private static String readInputLine() throws IOException {
        Console console = System.console();
        String line;
        if (console == null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            line = reader.readLine();
        } else {
            line = console.readLine();
        }
        return line;
    }

    private static void printMenu() {
        System.out.println();
        System.out.println("Scans:");
        System.out.println("  1. People having last name 'Smith'");
        System.out.println("  2. People having last name 'Smith' and first name starting with 'A'");
        System.out.println("  3. People having last name 'Smith' or after 'Smith'");
        System.out.println("  4. First 25 people having last name 'Smith' (returning emails and given name only)");
        System.out.println("  5. First two pages of people having last name 'Smith'");
        System.out.println("  6. Quit");
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "fakenames");

        int choice = 0;
        while (choice != 6) {
            printMenu();
            choice = getChoice();

            switch (choice) {
                case 1:
                    scanPeopleHavingLastNameSmith(table);
                    break;
                case 2:
                    scanPeopleHavingLastNameSmithAndFirstNameStartsWithLetterA(table);
                    break;
                case 3:
                    scanPeopleHavingLastNameSmithOrAfterSmith(table);
                    break;
                case 4:
                    scanPeopleFirst25SmithsReturningEmailsAndGivenName(table);
                    break;
                case 5:
                    scanPeopleHavingLastNameSmithGetFirstTwoPages(table);
                    break;
                case 6:
                    System.out.println("Bye.");
                    break;
                case -1:
                    System.out.println("Please enter a positive number.");
                    break;
                default:
                    System.out.println("Invalid choice.");
                    break;
            }
        }

        table.close();
    }


}
