package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.util.UUIDUtil;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhudebin on 14-6-13.
 */
public class TestTranslation {

    public static void main(String[] args) {

        testTranslate();
    }

    public static void testTranslate() {
//        67   33
//        TSquidFlow tsf = Translator.buildProxy("111", null, 3, 114, "");
//        TSquidFlow tsf = Translator.buildProxy("111", null, 3, 42, "{\"breakPoints\":[],\"dataViewers\":[],\"destinations\":[361]}");
        TSquidFlow tsf = Translator.buildProxy(UUIDUtil.genUUID().replace("-", "_"),
//                null, 3, 117, "");
                null, 3, 1986
//                ,""
                ,"{\"breakPoints\":[],\"dataViewers\":[34961],\"destinations\":[]}"
        );
//                null, 3, 167, "{\"hello\":[1]}");
        System.out.println(tsf);

    }

    public static void testMap() {
        Map<Date, Double> m = new HashMap<>();

        Date date = new Date();
        Date d1 = new Date(date.getTime()+1);
        Date d2 = new Date(date.getTime()+1);
        Date d3 = new Date(date.getTime()+2);
        Date d4 = new Date(date.getTime()+3);
        m.put(d1, 1d);
        m.put(d2, 2d);
        m.put(d3, 3d);
        m.put(d4, 4d);

        System.out.println(m.size());

    }
}
