package com.nearinfinity.explore.hbase.samples.fakenames;

import org.apache.hadoop.hbase.util.Bytes;

public class FakenamesConstants {
    public static final byte[] PERSONAL_FAMILY = Bytes.toBytes("personal");
    public static final byte[] CONTACTINFO_FAMILY = Bytes.toBytes("contactinfo");
    public static final byte[] CREDITCARD_FAMILY = Bytes.toBytes("creditcard");

    public static final byte[] PERSONNUMBER_QUALIFIER = Bytes.toBytes("personNumber");
    public static final byte[] GENDER_QUALIFIER = Bytes.toBytes("gender");
    public static final byte[] GIVENNAME_QUALIFIER = Bytes.toBytes("givenName");
    public static final byte[] MI_QUALIFIER = Bytes.toBytes("mi");
    public static final byte[] SURNAME_QUALIFIER = Bytes.toBytes("surname");
    public static final byte[] STREET_QUALIFIER = Bytes.toBytes("street");
    public static final byte[] CITY_QUALIFIER = Bytes.toBytes("city");
    public static final byte[] STATE_QUALIFIER = Bytes.toBytes("state");
    public static final byte[] POSTALCODE_QUALIFIER = Bytes.toBytes("postalCode");
    public static final byte[] COUNTRY_QUALIFIER = Bytes.toBytes("country");
    public static final byte[] EMAIL_QUALIFIER = Bytes.toBytes("email");
    public static final byte[] TELEPHONE_QUALIFIER = Bytes.toBytes("telephone");
    public static final byte[] MAIDENNAME_QUALIFIER = Bytes.toBytes("maidenName");
    public static final byte[] BIRTHDATE_QUALIFIER = Bytes.toBytes("birthdate");
    public static final byte[] NATIONALID_QUALIFIER = Bytes.toBytes("nationalId");
    public static final byte[] UPS_QUALIFIER = Bytes.toBytes("ups");
}
