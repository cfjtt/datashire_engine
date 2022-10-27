package com.eurlanda.common;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

/**
 * Created by zhudebin on 2017/3/10.
 */
public class JodaTimeTest {

    @Test
    public void test1() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        DateTime dateTime =  DateTime.parse("2012-01-01 02:10:01", formatter);

        System.out.println(dateTime.toDate());
    }
}
