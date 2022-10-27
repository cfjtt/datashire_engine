package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.translation.extract.CassandraExtractManager;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zhudebin on 2017/5/10.
 */
public class CassandraExtractManagerTest {

    @Test
    public void testFilterStr() {
        CassandraExtractManager manager = new CassandraExtractManager(
                new String[]{"192.168.137.104"},
                9042, 1, "cassandra", "cassandra", "test", "mycluster");

        String[] filterStr = manager.genIncrementalFilterStringAndMaxValue(1, "words_1", "word", "1",0);
        System.out.println(filterStr[0] + "--------" + filterStr[1]);
    }

    @Test
    public void testFilterStrTimeStamp() {
        CassandraExtractManager manager = new CassandraExtractManager(
                new String[]{"192.168.137.104"},
                9042, 1, "cassandra", "cassandra", "test", "mycluster");

        String[] filterStr = manager.genIncrementalFilterStringAndMaxValue(2, "words3", "udate", "",0);
        System.out.println(filterStr[0] + "--------" + filterStr[1]);
    }

    @Test
    public void testSDF() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        String s = sdf.format(new Date());
        System.out.println(s);
    }
}
