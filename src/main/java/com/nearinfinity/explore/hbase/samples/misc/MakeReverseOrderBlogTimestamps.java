package com.nearinfinity.explore.hbase.samples.misc;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class MakeReverseOrderBlogTimestamps {
    public static void main(String[] args) throws Exception {
        List<String> postDates = Arrays.asList(
                "20110320162535",
                "20110407145045",
                "20110423093020",
                "20110506134525",
                "20110615103825",
                "20110627162056",
                "20110707184035"
        );

        List<Long> descOrderPostDates = new ArrayList<Long>(postDates.size());
        for (String postDate : postDates) {
            descOrderPostDates.add(reverseOrderTimestamp(postDate));
        }
        Collections.sort(descOrderPostDates);

        System.out.println("Sorted row keys:");
        System.out.println(descOrderPostDates);

        System.out.println("Scanning order:");
        for (Long descOrderPostDate : descOrderPostDates) {
            System.out.println(postDate(descOrderPostDate));
        }
    }

    private static long reverseOrderTimestamp(String postDateAsString) throws ParseException {
        SimpleDateFormat sdf = createFormatter();
        Date postDate = sdf.parse(postDateAsString);
        long time = postDate.getTime();
        return Long.MAX_VALUE - time;
    }

    /*
    MAX_VALUE - timestamp = reversedTimestamp
    timestamp = (reversedTimestamp + MAX_VALUE) * -1
     */

    private static String postDate(Long reverseOrderTimestamp) {
        long timestamp = (reverseOrderTimestamp + Long.MAX_VALUE) * -1;
        Date postDate = new Date(timestamp);
        SimpleDateFormat sdf = createFormatter();
        return sdf.format(postDate);
    }

    private static SimpleDateFormat createFormatter() {
        return new SimpleDateFormat("yyyyMMddHHmmss");
    }
}
