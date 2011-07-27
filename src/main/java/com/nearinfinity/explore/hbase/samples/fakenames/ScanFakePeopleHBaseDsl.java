package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.hadoop.hbase.filter.PageFilter;

import com.nearinfinity.hbase.dsl.ForEach;
import com.nearinfinity.hbase.dsl.HBase;
import com.nearinfinity.hbase.dsl.QueryOps;
import com.nearinfinity.hbase.dsl.Row;

public class ScanFakePeopleHBaseDsl {

    public static void main(String[] args) {
        HBase<QueryOps<String>, String> hBase = new HBase<QueryOps<String>, String>(String.class);
        hBase.scan("fakenames", "smith-")
            .select()
                .family("personal")
                    .col("givenName")
                .family("contactinfo")
                    .col("email")
            .where()
                .filter(new PageFilter(25))
            .foreach(new ForEach<Row<String>>() {
                public void process(Row<String> row) {
                    System.out.printf("%s\t%s\t%s\n", row.getId(),
                            row.family("personal").value("givenName", String.class),
                            row.family("contactinfo").value("email", String.class));
                }
            });
    }
}
