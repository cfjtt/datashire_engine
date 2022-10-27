package com.eurlanda.datashire.engine.util;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 14-4-24.
 */
public class ListUtil {

    public static <T> List<T> asList(T... ii) {
        List<T> list = new ArrayList<>();
        for (T i : ii) {
            list.add(i);
        }
        return list;
    }

    public static String mkString(List list, String start, String sep, String end) {
        StringBuilder sb = new StringBuilder();
        if(start != null) {
            sb.append(start);
        }
        boolean first = true;
        for(Object o : list) {
            if(first) {
                first = false;
            } else {
                sb.append(sep);
            }
            sb.append(o.toString());
        }

        if(end != null) {
            sb.append(end);
        }
        return sb.toString();
    }

    public static String mkString(byte[] list, String start, String sep, String end) {
        return mkString(Lists.newArrayList(list), start, sep, end);
    }
}
