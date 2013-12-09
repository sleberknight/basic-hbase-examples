package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.commons.lang.StringUtils;
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
import java.util.Random;

import static com.nearinfinity.explore.hbase.samples.fakenames.FakenamesConstants.CREDITCARD_FAMILY;

public class AddCreditCardsToFakePeople {

    private static Random rand = new Random(System.currentTimeMillis());
    private static String[] expYears = {"2016", "2017", "2018", "2019", "2020"};

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "fakenames");

        String cardType = "MasterCard";
        String cardNumber = buildCardNumber();
        String expMonth = buildExpirationMonth();
        String expYear = buildExpirationYear();
        String expirationDate = String.format("%s/%s", expMonth, expYear);
        String cvv2 = buildCvv2();
        String qualifier = String.format("%s%s-%s", expYear, expMonth, cardNumber);
        String value = String.format("%s,%s,%s,%s", cardType, cardNumber, expirationDate, cvv2);

        Put put = new Put(Bytes.toBytes("cochran-julie-f-248495"));
        put.add(CREDITCARD_FAMILY, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
        table.flushCommits();

        Get get = new Get(Bytes.toBytes("cochran-julie-f-248495"));
        get.addFamily(CREDITCARD_FAMILY);
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String columnQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
            long columnTimestamp = cell.getTimestamp();
            System.out.printf("column=%s:%s, timestamp=%d, value=%s\n",
                    Bytes.toString(CREDITCARD_FAMILY), columnQualifier, columnTimestamp, columnValue);
        }

        table.close();
    }

    private static String buildExpirationYear() {
        int yearIndex = rand.nextInt(expYears.length);
        return expYears[yearIndex];
    }

    private static String buildExpirationMonth() {
        int month = rand.nextInt(12) + 1;
        return StringUtils.leftPad(String.valueOf(month), 2, '0');
    }

    private static String buildCvv2() {
        int cvv2 = rand.nextInt(1000);
        return StringUtils.leftPad(String.valueOf(cvv2), 3, '0');
    }

    private static String buildCardNumber() {
        return "4111" + buildLastThreeCreditCardNumberBlocks();
    }

    private static String buildLastThreeCreditCardNumberBlocks() {
        StringBuilder builder = new StringBuilder(16);
        for (int i = 0; i < 3; i++) {
            int block = rand.nextInt(10000);
            String blockStr = StringUtils.leftPad(String.valueOf(block), 4, '0');
            builder.append(blockStr);
        }
        return builder.toString();
    }
}
