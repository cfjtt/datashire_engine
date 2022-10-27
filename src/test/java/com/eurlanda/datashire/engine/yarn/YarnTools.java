package com.eurlanda.datashire.engine.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by zhudebin on 16/1/25.
 */
public class YarnTools {

    @Test
    public void killJob() throws IOException, YarnException {
        String appIdStr = "application_1453544892150_0010";
        String[] strs = appIdStr.split("_");
        YarnClient client = YarnClient.createYarnClient();
        client.init(new Configuration());
        client.start();
        client.killApplication(ApplicationId.newInstance(Long.parseLong(strs[1]), Integer.parseInt(strs[2])));
        client.stop();
    }

}
