package com.eurlanda;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.List;

/**
 * User: ZhangJuntao
 * Date: 2014/4/2
 * Time: 8:44
 */
public class ClientCaseTest {
    @Before
    public void setUpTest() throws Exception {
    }

    @Test
    public void test() {
        List<String> squidList = new ArrayList<>();
        squidList.add("hans");
        for (String s : squidList) {
            System.out.println(s);
        }
    }

    @Test
    public void testSquidFlow() throws Exception {
        System.out.println(InetAddress.getLocalHost().getHostAddress());
        System.out.println(ArrayUtils.toString(InetAddress.getAllByName("Hans-PC")));
        System.out.println(ArrayUtils.toString(NetworkInterface.getNetworkInterfaces()));
    }

    @Test
    public void testTTrainSquid() throws Exception {
    }


}
