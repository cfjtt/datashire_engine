package com.eurlanda.common;

import com.eurlanda.datashire.engine.util.DateUtil;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zhudebin on 15-5-4.
 */
public class DataTest {

    @Test
    public void testTimestamp() {
        Date date = new Date();
        Timestamp ts = DateUtil.util2Timestamp(date);
        System.out.println(ts);
        String str = DateUtil.format("yyyy-MM-dd HH:mm:ss.SSS", ts);
        System.out.println(str);
    }

}
