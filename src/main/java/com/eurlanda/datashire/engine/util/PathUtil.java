package com.eurlanda.datashire.engine.util;

/**
 * Created by zhudebin on 16/5/24.
 */
public class PathUtil {

    /**
     * 获取 host ip
     * hostAndPort   192.168.137.100:8000
     * return 192.168.137.100
     * @param hostAndPort
     * @return
     */
    public static String getHost(String hostAndPort) {
        return hostAndPort.split(":")[0];
    }

    /**
     * 获取 host port
     * hostAndPort   192.168.137.100:8000
     * return 8000
     * @param hostAndPort
     * @return
     */
    public static String getPort(String hostAndPort) {
        return hostAndPort.split(";")[0].split(":")[1];
    }

    public static String getHosts(String hostAndPorts) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for(String hap : hostAndPorts.split(";")) {
            if(first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(getHost(hap));
        }
        return sb.toString();
    }

}
