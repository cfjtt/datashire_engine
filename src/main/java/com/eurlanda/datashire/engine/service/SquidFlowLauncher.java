package com.eurlanda.datashire.engine.service;

import com.alibaba.fastjson.JSONObject;
import com.eurlanda.datashire.engine.dao.SFJobHistoryDao;
import com.eurlanda.datashire.engine.entity.SFJobHistory;
import com.eurlanda.datashire.engine.entity.TJobContext;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.enumeration.ERRCode;
import com.eurlanda.datashire.engine.enumeration.JobStatusEnum;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.exception.EngineExceptionType;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.EngineLogFactory;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.ConvertUtils;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.scheduler.ApplicationEventListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.SparkSession;

import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.*;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * squid flow spark作业启动入口
 * Created by Hans on 2014/4/3.
 */

public class SquidFlowLauncher {

    public static boolean local = false;
    public static List<TSquidFlow> runningSquidFlows = new ArrayList<>();
    private static Log log = LogFactory.getLog(SquidFlowLauncher.class);

    // 是否是批处理作业引擎标识, fasle 为流式, true 为批处理
    private static boolean isBatch = false;
    private static final int maxTasks = getParallelNum();
    //队列允许最大数
    private static final int queueSize = getMaxQueueSize();
    // 通过线程池来控制最大运行数
    private static final ExecutorService executor = Executors.newFixedThreadPool(maxTasks);
    //线程池专门用来发送状态
    private static final ExecutorService executorStatus = Executors.newCachedThreadPool();
    private static CustomJavaSparkContext customJavaSparkContext;
    //    private static SQLContext sqlContext;
    private static boolean success = true;
    // 任务ID与调度池的对应关系
    private static ConcurrentMap<String, String> task2pool = new ConcurrentHashMap<>();

    private static Set<String> pools;
    //队列(系统的资源池)（表示当前正在排队的任务）
    private static BlockingQueue<TSquidFlow> waitQueue = new LinkedBlockingQueue<>(queueSize);
    //个人的资源池(表示当前用户正在运行的task任务数,和系统允许的用户允许的任务数相比较)
    private static ConcurrentHashMap<Integer,Integer> personalTasksMap = new ConcurrentHashMap<>();
    private SquidFlowLauncher() {
    }

    static {
        local = ConfigurationUtil.isLocal();
        if (local) {
            System.out.println("本地启动成功..........");
        }
    }

    /**
     * private static CustomJavaSparkContext getCustomJavaSparkContext(){
     * if (customJavaSparkContext == null || !success) {
     * synchronized (SquidFlowLauncher.class) {
     * if (customJavaSparkContext == null || !success) {
     * if (customJavaSparkContext != null) {
     * customJavaSparkContext.stop();
     * }
     * if (local) {
     * customJavaSparkContext = getLocalCustomJavaSparkContext();
     * } else {
     * <p>
     * SparkConf conf = new SparkConf();
     * conf
     * //                                .setMaster(getSparkMasterUrl())
     * .setAppName("DataShire")
     * //                                .setSparkHome(getSparkHomeDir())
     * //                                .set("spark.executor.memory", getSparkExecutorMemory())
     * //                                .setJars(new String[]{getSparkJarLocation()})
     * .set("spark.scheduler.mode", "FAIR")
     * //.set("spark.shuffle.manager", "sort")
     * // 推测执行
     * //                                .set("spark.speculation","true")
     * // todo 调度池将根据并发数自动生成
     * .set("spark.scheduler.allocation.file", getSchedulerLocation());
     * //                        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
     * //                        conf.registerKryoClasses(new Class[]{DataCell.class, ScanPlan.class, PhoenixRDDPartition.class});
     * //                        conf.set("spark.kryoserializer.buffer.mb", "20");
     * customJavaSparkContext = new CustomJavaSparkContext(conf);
     * }
     * success = true;
     * }
     * }
     * }
     * return customJavaSparkContext;
     * }
     */

    public static void initSparkContext() {
        log.info("========== 开始初始化 spark ===============");
        CustomJavaSparkContext cjsc = getYarnCustomJavaSparkContext();
        if (cjsc != null) {
            int slices = 3;
            List<Integer> list = new ArrayList<>(1000);
            for (int i = 1; i <= 1000; i++) {
                list.add(i);
            }
            JavaRDD<Integer> rdd = cjsc.parallelize(list, slices).map(new Function<Integer, Integer>() {
                @Override
                public Integer call(Integer v1) throws Exception {
                    Random random = new Random();
                    int x = random.nextInt(2) - 1;
                    int y = random.nextInt(2) - 1;
                    if (x * x + y * y < 1) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            int count = rdd.reduce(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });
            System.out.println("========== result =========" + count);
            log.info("============ init sparkContext success =============");
        } else {
            log.error("============ init sparkContext fail =============");
        }
    }

    private static CustomJavaSparkContext getYarnCustomJavaSparkContext() {
        if (customJavaSparkContext == null || !success || customJavaSparkContext.sc().isStopped()) {
            synchronized (SquidFlowLauncher.class) {
                if (customJavaSparkContext == null || !success || customJavaSparkContext.sc().isStopped()) {
                    if (customJavaSparkContext != null) {
                        log.info("sparkContext 是否停止: " + customJavaSparkContext.sc().isStopped());
                        customJavaSparkContext.stop();
                    }
                    if (local) {
                        customJavaSparkContext = getLocalCustomJavaSparkContext();
                    } else {
                        try {
                            if (StringUtils.isEmpty(getSparkProxyUser().trim())) {
                                customJavaSparkContext = genYarnCustomJavaSparkContext();
                            } else {
                                UserGroupInformation
                                        proxyUser =
                                        UserGroupInformation.createProxyUser(getSparkProxyUser(),
                                                UserGroupInformation.getCurrentUser());
                                customJavaSparkContext =
                                        proxyUser
                                                .doAs(new PrivilegedExceptionAction<CustomJavaSparkContext>() {
                                                    @Override
                                                    public CustomJavaSparkContext run()
                                                            throws Exception {
                                                        return genYarnCustomJavaSparkContext();
                                                    }
                                                });
                            }
                        } catch (Exception e) {
                            log.error("init spark fail", e);
                            throw new RuntimeException("init spark fail", e);
                        }

                    }
                    success = true;
                }
            }
        }
        customJavaSparkContext.setLogLevel("INFO");
        return customJavaSparkContext;
    }

    /**
     * 生成yarn上运行的sparkContext
     *
     * @return
     */
    private static CustomJavaSparkContext genYarnCustomJavaSparkContext() {
        SparkConf conf = new SparkConf();
        conf.set("spark.driver.memory", getSparkDriverMemory())
                .set("spark.master", "yarn")
                .set("spark.submit.deployMode", "client")
                .setAppName("datashire")
                .set("spark.executor.instances", getSparkExecutorInstances())
                //            .set("spark.jars","file:/Users/zhudebin/soft/ds/testPro/spark5/lib/spark-examples-1.5.2-hadoop2.6.0-cdh5.4.7.jar")
                .set("spark.executor.memory", getSparkExecutorMemory())
                .set("spark.executor.cores", getSparkExecutorCores())
                .set("spark.yarn.queue", getSparkYarnQueue())
//                .set("spark.executor.extraLibraryPath", SPARK_EXECUTOR_EXTRALIBRARYPATH())
//                .set("spark.executor.extraClassPath", SPARK_EXECUTOR_EXTRALIBRARYPATH()+ "*")
                // scheduler
                .set("spark.scheduler.mode", "FAIR")
                .set("spark.shuffle.manager", "sort")
//                .set("spark.sql.shuffle.partitions", "200")

                // 调整
//                .set("spark.yarn.scheduler.reporterThread.maxFailures", Integer.MAX_VALUE + "")
                .set("spark.yarn.max.executor.failures", Integer.MAX_VALUE + "")
                .set("spark.shuffle.io.connectionTimeout", 3 * 24 * 60 * 60 + "s")
                .set("spark.default.parallelism", "200")


                //            .set("spark.scheduler.allocation.file", "/Users/zhudebin/Documents/iworkspace/spark_dev/spark_yarn2/src/main/resources/fairscheduler.xml")
                .set("spark.executor.userClassPathFirst", "true")
                .set("spark.driver.userClassPathFirst", "true")
                .set("spark.yarn.jars", getSparkYarnEngineJarsDir() + "*")
                // 设置日志 分别设置不同的日志配置文件,是为了放置 driver和executor同一个节点,写入同一个日志文件
                .set("spark.yarn.dist.jars", getSparkJarLocation())
                .set("spark.driver.extraJavaOptions", getSparkDriverExtraJavaOptions())
                .set("spark.executor.extraJavaOptions", getSparkExecutorExtraJavaOptions());
        // .set("spark.driver.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m -Dlog4j.configuration=file:" + ConfigurationUtil.getSparkDriverLogLocation() + "/log4j_driver.properties")
        // .set("spark.executor.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=512m -Dlog4j.configuration=file:" + ConfigurationUtil.getSparkDriverLogLocation() + "/log4j_executor.properties");
//                .setJars(new String[] { getSparkJarLocation() });

        if (spark_driver_host().trim().length() > 0) {
            conf.set("spark.driver.host", spark_driver_host());
        }

        if ("true".equals(getSparkDynamicAllocationEnabled().trim())) {
            log.info("============ 启动动态资源配置模式 ============= ");
            conf.set("spark.dynamicAllocation.enabled", "true");
            // 设置动态资源获取其他配置参数
            if (!StringUtils.isEmpty(getSparkDynamicAllocationConfig())) {
                JSONObject json = JSONObject.parseObject(getSparkDynamicAllocationConfig().trim());
                for (Map.Entry<String, Object> entry : json.entrySet()) {
                    log.info("\t" + "===" + entry.getKey() + ":" + entry.getValue());
                    conf.set(entry.getKey(), entry.getValue().toString());
                }
            }
        }
        conf.set("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer");

//        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        // 设置一些其他参数
        JSONObject configJson = JSONObject.parseObject(getSparkConfig().trim());
        for (Map.Entry<String, Object> entry : configJson.entrySet()) {
            log.info("\t" + "===" + entry.getKey() + ":" + entry.getValue());
            conf.set(entry.getKey(), entry.getValue().toString());
        }

//        conf.set("spark.sql.codegen", "false");

        SparkSession.Builder builder = SparkSession.builder().config(conf);
        if (isBatch && ConfigurationUtil.enableHive()) {   // 只有批处理和设置允许hive时才连接hive
            builder.enableHiveSupport();
        }
        SparkSession sparkSession = builder.getOrCreate();
        sparkSession.sparkContext().addSparkListener(new ApplicationEventListener() {
            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd event) {
                log.error("spark application 异常中断..................");
                String eventLog = ConvertUtils.logEventToString(event);
                log.error("spark application 异常中断..................log:" + eventLog);
//                System.exit(-1);
            }
        });

        CustomJavaSparkContext cjsc = new CustomJavaSparkContext(sparkSession.sparkContext());
//        CustomJavaSparkContext cjsc = new CustomJavaSparkContext(conf);
        cjsc.setLogLevel("INFO");
        return cjsc;
    }

    /**
     * 生成本地运行的sparkContext
     *
     * @return
     */
    private static CustomJavaSparkContext getLocalCustomJavaSparkContext() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[4]").setAppName("DataShire-local-test");
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //                        conf.set("spark.kryo.registrator", "com.eurlanda.datashire.engine.spark.MyRegistrator");
        conf.set("spark.default.parallelism", "8");
        conf.set("spark.shuffle.manager", "sort");
        conf.set("spark.sql.shuffle.partitions", "20");
        // 设置一些其他参数
        JSONObject configJson = JSONObject.parseObject(getSparkConfig().trim());
        for (Map.Entry<String, Object> entry : configJson.entrySet()) {
            log.info("\t" + "===" + entry.getKey() + ":" + entry.getValue());
            conf.set(entry.getKey(), entry.getValue().toString());
        }

        SparkSession.Builder builder = SparkSession.builder().config(conf);
        if(ConfigurationUtil.enableHive()) {
            //builder.enableHiveSupport();
        }
        SparkSession sparkSession = builder.getOrCreate();

        CustomJavaSparkContext cjsc = new CustomJavaSparkContext(sparkSession.sparkContext());
        cjsc.setLogLevel("INFO");
        return cjsc;
    }

    public static SparkSession getSparkSession() throws InterruptedException {
        if (customJavaSparkContext == null) {
            synchronized (SquidFlowLauncher.class) {
                if (customJavaSparkContext == null) {
                    customJavaSparkContext = getYarnCustomJavaSparkContext();
                }
            }
        }
        SparkSession session = SparkSession.builder().sparkContext(customJavaSparkContext.scc()).getOrCreate();
        return session;
    }

    /**
     * 停止一个运行的squidflow
     *
     * @param taskId
     */
    public static void stopSquidFlow(String taskId) {
        String pool = task2pool.get(taskId);
        if (pool == null) {
            //从等待队列中将任务删除
            if (waitQueue.size() > 0) {
                Iterator<TSquidFlow> squidFlowIterator = waitQueue.iterator();
                while (squidFlowIterator.hasNext()) {
                    TSquidFlow squidFlow = squidFlowIterator.next();
                    if (squidFlow.getTaskId().equals(taskId)) {
                        squidFlowIterator.remove();
                    }
                }
            }
            log.info("该任务已停止，或者没有启动");
        } else {
            customJavaSparkContext.cancelJobGroup(task2pool.get(taskId));
            log.info("关闭该任务成功：" + taskId);
        }
    }

    /**
     * 启动一个squidflow
     *
     * @param squidFlow
     * @throws EngineException
     */
    public static void launch(final TSquidFlow squidFlow) throws EngineException {
        if (squidFlow == null) {
            throw new EngineException(EngineExceptionType.SQUID_FLOW_NULL);
        } else if (squidFlow.getSquidList().size() == 0) {
            log.info("翻译后的squidflow 为空，" + squidFlow.getRepositoryId());
            throw new EngineException(EngineExceptionType.SQUID_FLOW_NULL);
        }
        log.info("正在运行的squidflow 总数 :" + runningSquidFlows.size());

        for (TSquidFlow sf : runningSquidFlows) {
            log.info("\t |正在运行的squidflow :" + sf.getName());
        }
        // 将squidflow加入到正在运行时集合
        runningSquidFlows.add(squidFlow);
        //追加运行数
        if(squidFlow.getUserId()>0) {
            if (personalTasksMap.containsKey(squidFlow.getUserId())) {
                personalTasksMap.put(squidFlow.getUserId(), personalTasksMap.get(squidFlow.getUserId()) + 1);
            } else {
                personalTasksMap.put(squidFlow.getUserId(), 1);
            }
        }
        // 获取池
        executor.execute(new Runnable() {
            private final Log log = LogFactory.getLog(this.getClass());
            private final SFJobHistoryDao sfJobHistoryDao = ConstantUtil.getSFJobHistoryDao();

            @Override
            public void run() {
                try {
                    String pool = task2pool.get(squidFlow.getTaskId());
                    CustomJavaSparkContext cjsc = getYarnCustomJavaSparkContext();
                    TJobContext tJobContext = squidFlow.getJobContext();
                    tJobContext.setSparkSession(getSparkSession());
//                    cjsc.clearJobGroup();
                    cjsc.setLocalProperty("spark.scheduler.pool", pool);
                    String maxrunningtask_key = "spark.datashire.scheduler.maxrunningtask";

                    /*if(StringUtils.isNotEmpty(ConfigurationUtil.getProperty(maxrunningtask_key))) {
                        cjsc.setLocalProperty(maxrunningtask_key,
                                ConfigurationUtil.getProperty(maxrunningtask_key));
                    }*/
                    if(StringUtils.isNotEmpty(squidFlow.getMaxRunningTask()+"")) {
                        log.info("单个任务允许最大的Task数为:"+squidFlow.getMaxRunningTask());
                        cjsc.setLocalProperty(maxrunningtask_key,
                                squidFlow.getMaxRunningTask()+"");
                    }
                    cjsc.setJobGroup(pool, pool + "_datashire_[" + squidFlow.getId() + "]_" + squidFlow.getTaskId(), true);
                    squidFlow.run(cjsc);
                } catch (Exception e) {
                    sfJobHistoryDao.updateStatus(squidFlow.getTaskId(), squidFlow.getId(), JobStatusEnum.FAIL);
                    log.error("squidflow 作业运行异常.....", e);
                    // 判断是否有 reportSquid,并且没有运行成功，需要对这些squid推送异常状态
//                    for(TSquid tSquid : squidFlow.getSquidList()) {
//                        if(tSquid.getType() == TSquidType.REPORT_SQUID &&
//                                !tSquid.isFinished()) {
//                            try {
//                                RpcServerFactory.getReportService()
//                                        .onSquidflowFailed(squidFlow.getTaskId(),
//                                                squidFlow.getRepositoryId(),
//                                                tSquid.getSquidId(), "作业运行异常："+ EngineLogFactory.exceptionStackMessage(e));
//                            } catch (Exception e1) {
//                                EngineLogFactory.logError(tSquid, "推送squidflow运行失败消息异常", e1);
//                            }
//                        }
//                    }
                } finally {
                    logEnd(squidFlow);
                    // 释放池
                    releasePool(squidFlow.getTaskId());
                    //释放运行数
                    releaseParallelNum(squidFlow.getUserId());
                    log.info("释放调度池 ： taskId=" + squidFlow.getTaskId());
                    // 运行完毕，移除
                    runningSquidFlows.remove(squidFlow);
                    log.info("移除正在运行的squidflow ： squidflowid=" + squidFlow.getId() + ",name=" + squidFlow.getName());
                }
            }
        });
    }

    /**
     * 获取调度池，获取调度池的唯一入口
     *
     * @param taskId
     */
    private static synchronized String getPool(String taskId) {
        if (pools == null) {
            synchronized (SquidFlowLauncher.class) {
                if (pools == null) {
                    pools = new HashSet<>(maxTasks);
                    for (int i = 1; i <= maxTasks; i++) {
                        pools.add("p" + i);
                    }
                }
            }
        }
        log.debug("调度池总数为：" + pools.size());
        Collection<String> usedPools = task2pool.values();
        log.debug("已使用调度池数量为：" + usedPools.size());
        for (String pool : usedPools) {
            log.debug("已使用调度池为：" + pool);
        }
        for (String pool : pools) {
            if (!usedPools.contains(pool)) {
                task2pool.put(taskId, pool);
                return pool;
            }
        }
        log.error("分配调度池异常，调度池全部用完。。。。。。");
        return "default";
    }

    /**
     * 新的获取调度池的方式
     *
     * @return
     */
    public static synchronized String getNewPool(final TSquidFlow squidFlow,boolean isAddQueue){
        //pools初始化
        if (pools == null) {
            synchronized (SquidFlowLauncher.class) {
                if (pools == null) {
                    pools = new HashSet<>(maxTasks);
                    for (int i = 1; i <= maxTasks; i++) {
                        pools.add("p" + i);
                    }
                }
            }
        }
        //如果没有达到并发数,取资源
        if(!isAddQueue) {
            log.debug("调度池总数为：" + pools.size());
            Collection<String> usedPools = task2pool.values();
            log.debug("已使用调度池数量为：" + usedPools.size());
            for (String pool : usedPools) {
                log.debug("已使用调度池为：" + pool);
            }
            for (String pool : pools) {
                if (!usedPools.contains(pool)) {
                    task2pool.put(squidFlow.getTaskId(), pool);
                    return pool;
                }
            }
        }

        //将任务追加到等待队列中(1.没有资源 2.达到最大并行数)
        if(squidFlow!=null) {
            if (!waitQueue.contains(squidFlow)) {
                //等待队列中不存在，加入等待队列中，并推送状态
                //log.info("系统没有资源，将入等待队列：" + squidFlow.getTaskId());
                boolean flag = waitQueue.offer(squidFlow);
                //队列已满，直接返回
                if (!flag) {
                    //不单独放在一个线程里面，会报dead lock
                    executorStatus.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                //log.debug("发送队列已满状态");
                                RpcServerFactory.getServerService().onTaskFinish(squidFlow.getId()+"", ERRCode.WAITQUEUE_ISMAX.getValue());
                            } catch (Exception e) {
                                e.printStackTrace();
                                log.error(e);
                            }
                        }
                    });
                } else {
                    //log.info("开始发送等待状态:"+squidFlow.getTaskId());
                    executorStatus.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                //log.info("正在发送等待状态:"+squidFlow.getTaskId());
                                RpcServerFactory.getServerService().onTaskFinish(squidFlow.getId()+"", ServerRpcUtil.STATUS_WAIT);
                                //log.info("发送等待状态完毕");
                            } catch (Exception e) {
                                e.printStackTrace();
                                log.error(e);
                            }
                        }
                    });
                }
            }
        }
        return "null";
    }

    /**
     * 判断是否达到允许的最大并行数
     * @param squidFlow
     * @return
     */
    public static synchronized boolean isParallelMax(TSquidFlow squidFlow){
        boolean flag = true;
        if(personalTasksMap.containsKey(squidFlow.getUserId())){
            if(personalTasksMap.get(squidFlow.getUserId()) < squidFlow.getMaxParallelNum()){
                flag = false;
            }
        } else {
            flag = false;
        }
        return flag;
    }
    /**
     * 定时将等待队列中的元素加入到运行任务中(100毫秒运行一次)
     */
    static {

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
             while(true){
                 TSquidFlow squidFlow = waitQueue.peek();
                 if(squidFlow==null){
                     break;
                 } else {
                     //添加判断是否达到最大运行数
                     boolean flag = SquidFlowLauncher.isParallelMax(squidFlow);
                     String pool = getNewPool(squidFlow,flag);
                     if (com.eurlanda.datashire.utility.StringUtils.isNull(pool)) {
                         break;
                     } else {
                         waitQueue.poll();
                         //log.debug("作业开始运行");
                         //向客户端推送开始运行状态
                         try {
                             //log.debug("发送开始运行状态");
                             RpcServerFactory.getServerService().onTaskFinish(squidFlow.getId()+"", ServerRpcUtil.STATUS_START);
                         } catch (Exception e) {
                             e.printStackTrace();
                             //log.error(e);
                         }
                         launch(squidFlow);
                     }
                 }
             }
            }
        }, 100, 100);
    }

    /**
     * 释放调度池，释放的唯一入口
     *
     * @param taskId
     */
    private static synchronized void releasePool(String taskId) {
        task2pool.remove(taskId);
    }

    /**
     * 释放用户的运行数
     * @param userId
     */
    private static synchronized void releaseParallelNum(int userId){
        if(personalTasksMap.containsKey(userId)){
            if(personalTasksMap.get(userId)>1){
                personalTasksMap.put(userId,personalTasksMap.get(userId)-1);
            } else {
                personalTasksMap.remove(userId);
            }
        }
    }

    private static void logEnd(TSquidFlow squidFlow) {
        try {
            SFJobHistoryDao sfJobHistoryDao = ConstantUtil.getSFJobHistoryDao();
            SFJobHistory sfJobHistory = sfJobHistoryDao.getByTaskId(squidFlow.getTaskId());
            if (sfJobHistory != null) {
                if (sfJobHistory.getStatus() == JobStatusEnum.RUNNING.value) {
                    sfJobHistoryDao.updateStatus(squidFlow.getTaskId(), squidFlow.getId(), JobStatusEnum.SUCCESS);
                    EngineLogFactory.logInfo(squidFlow, "squidflow run success");
                    // 推送 任务运行状况 - 成功
                    ServerRpcUtil.sendSquidFlowSuccess(squidFlow);
                } else if (sfJobHistory.getStatus() == JobStatusEnum.FAIL.value) {
                    EngineLogFactory.logError(squidFlow, "squidflow run failed");
                    // 推送 任务运行状况 - 失败
                    ServerRpcUtil.sendSquidFlowFail(squidFlow);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            EngineLogFactory.logError(squidFlow, e);
        }
    }

    /**
     * 设置为批处理作业引擎
     */
    public static void setBatchJob() {
        isBatch = true;
    }
}
