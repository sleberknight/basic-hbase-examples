package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import static com.nearinfinity.explore.hbase.samples.fakenames.FakenamesConstants.*;

public class BulkLoadFakenamesData {

    public static void main(String[] args) throws Exception {
//        String dataFileName = "/data/fakenames/fakenames-0.csv";
        String dataFileName = "etc/fakenames-sample-1000.csv";

        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "fakenames");
        table.setAutoFlush(false, true);

        int currentRow = 1;
        File dataFile = new File(dataFileName);
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        String line = reader.readLine();
        while (line != null) {
            final String[] values = line.split(",");
            Map<String, byte[]> dataMap = new ByteValueHashMap<String>() {{
                putAsBytes("personNumber", values[0]);
                putAsBytes("gender", values[1]);
                putAsBytes("givenName", values[2]);
                putAsBytes("mi", values[3]);
                putAsBytes("surname", values[4]);
                putAsBytes("street", values[5]);
                putAsBytes("city", values[6]);
                putAsBytes("state", values[7]);
                putAsBytes("postalCode", values[8]);
                putAsBytes("country", values[9]);
                putAsBytes("email", values[10]);
                putAsBytes("telephone", values[11]);
                putAsBytes("maidenName", values[12]);
                putAsBytes("birthdate", reformatBirthdate(values[13]));
                putAsBytes("cardInfo", makeCreditCardInfo(values));
                putAsBytes("nationalId", values[18]);
                putAsBytes("ups", values[19]);
            }};

            String rowKey = String.format("%s-%s-%s-%s",
                    Bytes.toString(dataMap.get("surname")),
                    Bytes.toString(dataMap.get("givenName")),
                    Bytes.toString(dataMap.get("mi")),
                    Bytes.toString(dataMap.get("personNumber")))
                    .toLowerCase();

            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(PERSONAL_FAMILY, PERSONNUMBER_QUALIFIER, dataMap.get("personNumber"));
            put.add(PERSONAL_FAMILY, GENDER_QUALIFIER, dataMap.get("gender"));
            put.add(PERSONAL_FAMILY, GIVENNAME_QUALIFIER, dataMap.get("givenName"));
            put.add(PERSONAL_FAMILY, MI_QUALIFIER, dataMap.get("mi"));
            put.add(PERSONAL_FAMILY, SURNAME_QUALIFIER, dataMap.get("surname"));
            put.add(PERSONAL_FAMILY, MAIDENNAME_QUALIFIER, dataMap.get("maidenName"));
            put.add(PERSONAL_FAMILY, BIRTHDATE_QUALIFIER, dataMap.get("birthdate"));
            put.add(PERSONAL_FAMILY, NATIONALID_QUALIFIER, dataMap.get("nationalId"));
            put.add(PERSONAL_FAMILY, UPS_QUALIFIER, dataMap.get("ups"));
            put.add(CONTACTINFO_FAMILY, STREET_QUALIFIER, dataMap.get("street"));
            put.add(CONTACTINFO_FAMILY, CITY_QUALIFIER, dataMap.get("city"));
            put.add(CONTACTINFO_FAMILY, STATE_QUALIFIER, dataMap.get("state"));
            put.add(CONTACTINFO_FAMILY, POSTALCODE_QUALIFIER, dataMap.get("postalCode"));
            put.add(CONTACTINFO_FAMILY, COUNTRY_QUALIFIER, dataMap.get("country"));
            put.add(CONTACTINFO_FAMILY, EMAIL_QUALIFIER, dataMap.get("email"));
            put.add(CONTACTINFO_FAMILY, TELEPHONE_QUALIFIER, dataMap.get("telephone"));
            put.add(CREDITCARD_FAMILY, Bytes.toBytes(makeCreditCardQualifier(values)), dataMap.get("cardInfo"));
            table.put(put);

            if (currentRow % 1000 == 0) {
                System.out.printf("At line %d, rowKey: %s\n", currentRow, rowKey);
            }

            currentRow++;
            line = reader.readLine();
        }

        table.close();  // ensures we flush cached commits and cleanup
    }

    private static String makeCreditCardQualifier(String[] values) {
        // <reformatted-expirationdate>-<cardnumber>

        // (m|mm)/yyyy --> yyyymm
        String[] expirationDate = values[17].split("/");
        return expirationDate[1] + StringUtils.leftPad(expirationDate[0], 2, '0') + "-" + values[15];
    }

    private static String reformatBirthdate(String original) {
        // (m|mm)/(d|dd)/yyyy -> yyyymmdd
        String[] values = original.split("/");
        return values[2] + StringUtils.leftPad(values[0], 2, '0') + StringUtils.leftPad(values[1], 2, '0');
    }

    private static String makeCreditCardInfo(String[] values) {
        // cardtype,cardnumber,expiration,cvv2
        return String.format("%s,%s,%s,%s", values[14], values[15], values[17], values[16]);
    }

    private static class ByteValueHashMap<K> extends HashMap<K, byte[]> {
        void putAsBytes(K key, String value) {
            put(key, Bytes.toBytes(value));
        }
    }
}
