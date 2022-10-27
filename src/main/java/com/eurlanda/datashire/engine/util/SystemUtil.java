package com.eurlanda.datashire.engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

/**
 * 系统公用方法集合
 * Created by zhudebin on 14-7-12.
 *
 */
public class SystemUtil {

    private static Logger logger = LoggerFactory.getLogger(SystemUtil.class);

    public static SystemUtil instance = new SystemUtil();

    static {
        logger.info("初始化系统参数=== 时区{}", ConfigurationUtil.SYSTEM_TIME_ZONE());
        TimeZone.setDefault(TimeZone.getTimeZone(ConfigurationUtil.SYSTEM_TIME_ZONE()));
    }

    /**
     * 显示系统信息
     * 内存，CPU
     */
    public static void showSystemInfo() {

    }

    public static void showCPUInfo() {

    }

}
