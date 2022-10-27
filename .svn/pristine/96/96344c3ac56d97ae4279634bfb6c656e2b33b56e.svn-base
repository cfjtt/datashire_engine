package com.eurlanda.datashire.common.rpc.server;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.common.rpc.IEngineService;
import com.eurlanda.datashire.engine.service.RpcServerFactory;
import org.apache.avro.AvroRemoteException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 14-5-17.
 */
public class CrawlerRPCClient {

    public static void main(String[] args) {
          launch();
        System.exit(1);
    }

    public static void launch() {

        List<Map<String,Object>> profileList = new ArrayList<Map<String,Object>>();

        Map<String,Object> params = new HashMap<String, Object>();
        params.put("initurls", "工业4.0");
        params.put("profilename", "profile-weibo-" + 1);
        params.put("searchBegin", "2009-11-11");
        params.put("searchEnd", "2014-10-10");
        params.put("squidId", 1);
        params.put("repositoryId", 1);
        params.put("username", "zjweii@qq.com");
        params.put("password", "asd0.123");
        profileList.add(params);

        String strProfile = JSON.toJSONString(profileList);
        try {
            RpcServerFactory.getCrawlerService().crawlWeibo(strProfile);
        } catch (AvroRemoteException e) {
            e.printStackTrace();
        }

    }

    public static void stop(String taskId) {
        IEngineService iEngineService = RpcServerFactory.getEngineService();
        try {
            iEngineService.shutdownSFTask(taskId, "");
            System.out.println(taskId + " 关闭成功");
            System.exit(0);
        } catch (AvroRemoteException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
