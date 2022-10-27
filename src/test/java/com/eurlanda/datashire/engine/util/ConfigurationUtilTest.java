package com.eurlanda.datashire.engine.util;

import org.junit.Test;

import java.net.URL;

/**
 * Created by zhudebin on 16/1/5.
 */
public class ConfigurationUtilTest {

    @Test
    public void testPath() {
        String str = ConfigurationUtil.getInnerHbaseHost();
        System.out.println(str);
    }

    @Test
    public void testPath2() {
        ConfigurationUtil.class.getResourceAsStream("/conf/configuration.properties");
        URL url = ConfigurationUtil.class.getResource("/conf/configuration.properties");
        System.out.println(url.getPath());

    }
}
