package com.eurlanda.common;

import org.junit.Test;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.WeakHashMap;

/**
 * Created by zhudebin on 16/3/24.
 */
public class WeakHashMapTest {

    @Test
    public void test1() {
        WeakHashMap whm = new WeakHashMap();
        whm.put(null, "100");
        System.out.println(whm.get(null));

    }

    @Test
    public void test2() {
        HashMap map = new HashMap();
        map.put(null, "1000");
        System.out.println(map.get(null));
    }

    @Test
    public void test3() {
        Hashtable map = new Hashtable();
        map.put(null, "1000");
        System.out.println(map.get(null));
    }
}
