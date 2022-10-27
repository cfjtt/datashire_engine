package com.eurlanda.datashire.engine.concurrentJob;

import com.alibaba.fastjson.JSON;
import util.JsonUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Eurlanda on 2017/7/10.
 */
public class ConcurrentRun {

    private static final String PATH = "/squidflow";

    /**
     * 读取文件
     *
     * @return
     */
    public static List<Map<String, Object>> getRunSquidFlow() {
        List<Map<String,Object>> squidflowList = new ArrayList<>();
        BufferedReader reader = null;
        try {
            InputStream is = ConcurrentRun.class.getResourceAsStream(PATH);
            reader = new BufferedReader(new InputStreamReader(is,"utf-8"));
            if (reader != null) {
                String str = null;
                while ((str = reader.readLine()) != null) {
                    Map<String,Object> map = JSON.parseObject(str, Map.class);
                    squidflowList.add(map);
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if(reader!=null){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return squidflowList;
    }

    public static void main(String[] args) {
        ExecutorService service = Executors.newCachedThreadPool();
        List<Map<String,Object>> paramMaps = null;
        try {
            paramMaps = getRunSquidFlow();
            for (Map<String, Object> paramMap : paramMaps) {
                int squidFlowId = (Integer) paramMap.get("squidFlowId");
                int repositoryId = (Integer) paramMap.get("repositoryId");
                Map<String,Object> map = new HashMap<>();
                if(paramMap.containsKey("breakPoints")){
                    map.put("breakPoints",paramMap.get("breakPoints"));
                }
                if(paramMap.containsKey("dataViewers")){
                    map.put("dataViewers",paramMap.get("dataViewers"));
                }
                if(paramMap.containsKey("destinations")){
                    map.put("destinations",paramMap.get("destinations"));
                }
                String params = "";
                if(map.size()>0){
                    params = JsonUtil.toGsonString(map);
                }
                service.execute(new ConcurrentLaunchEngineJob(squidFlowId, repositoryId, params));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
