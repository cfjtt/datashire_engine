package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.TDebugSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.util.UUIDUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.CustomJavaSparkContext;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SquidFlow 基类
 * Created by Juntao.Zhang on 2014/4/18.
 */
public abstract class AbstractSquidTest {
    protected transient CustomJavaSparkContext sc;
    private static List<String> emotionDic = new ArrayList<String>();
    private static Object model = null;

    static {

        // 启动 RMI
//        EngineServer.startRPCServer();
    }

    @Before
    public void setUp() {
        sc = new CustomJavaSparkContext("local", "test");
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
        System.clearProperty("spark.driver.port");
    }

    protected static TSquidFlow createSquidFlow(int repositoryId, int squidFlowId) {
        String taskId = UUIDUtil.genUUID();
        TSquidFlow tsf = new TSquidFlow();
        tsf.setId(squidFlowId);
        tsf.setRepositoryId(repositoryId);
        tsf.setTaskId(taskId);
        return tsf;
    }

    protected static ArrayList<Integer> keyList(Integer... inKeys) {
        ArrayList<Integer> inList = new ArrayList<Integer>();
        Collections.addAll(inList, inKeys);
        return inList;
    }
    protected static HashSet<Integer> keySet(Integer... inKeys) {
        HashSet<Integer> in = new HashSet<>();
        Collections.addAll(in, inKeys);
        return in;
    }

    protected static Object getNaiveBayesModel() throws Exception {
        if (model == null) {
            initNaiveBayesModel();
        }
        return model;
    }

    protected synchronized static void initNaiveBayesModel() throws Exception {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("./data/model"));
        model = ois.readObject();
    }

    protected static List<String> getEmotionDic() {
        if (CollectionUtils.isEmpty(emotionDic)) {
            initEmotionDic();
        }
        return emotionDic;
    }

    protected synchronized static void initEmotionDic() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("dic\\emotion_dic.dic"), "UTF-8"));
            String line = "";
            do {
                if (StringUtils.isNotBlank(line)) {
                    emotionDic.add(line.trim());
                }
                line = br.readLine();
            } while (line != null);

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static TDebugSquid createTDebugSquid(TSquid pre, TSquid next) {
        TDebugSquid squid = new TDebugSquid();
        squid.setPreviousSquid(pre);
        squid.setSquidId(1111);
        return squid;
    }

    protected static void printMap(Map m) {
        Set<Map.Entry> es = m.entrySet();
        System.out.print("{");
        for(Map.Entry e : es) {
            System.out.print(",[" + e.getKey() + "," + e.getValue() + "]");
        }
        System.out.println("}");
    }
}
