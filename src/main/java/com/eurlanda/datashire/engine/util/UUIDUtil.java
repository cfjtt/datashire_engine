package com.eurlanda.datashire.engine.util;

import java.util.UUID;

/**
 * Created by zhudebin on 14-4-26.
 */
public class UUIDUtil {

    /**
     * 生成UUID
     * @return
     */
    public static String genUUID() {
        return UUID.randomUUID().toString();
    }

}
