package com.eurlanda.datashire.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Created by zhudebin on 16/2/22.
 */
public class Test4 {

    @Test
    public void test1() {
        byte[][] bytes = KeyValue.parseColumn("cf1:c1".getBytes());

        System.out.println(Bytes.toString(bytes[0]));
        System.out.println(Bytes.toString(bytes[1]));
    }
}
