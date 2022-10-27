package com.eurlanda.datashire.engine.util;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhudebin on 14-1-7.
 */
public class ConfigurationUtil {

    private static Properties p = new Properties();
    private static Map<String, String> splitColumnInfo;

    static {
        try {
            p.load(ConfigurationUtil.class.getResourceAsStream("/conf/configuration.properties"));
//            getSplitColumnInfo();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static Properties getQuartProperties(){
        Properties p = new Properties();
        try {
            p.load(ConfigurationUtil.class.getResourceAsStream("/conf/quartz.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return p;
    }
    public static Map<String, String> getSplitColumnInfo() {
        if(splitColumnInfo == null) {
            splitColumnInfo = new HashMap<>();
            Connection con = null;

            try {
                con = ConstantUtil.getSysDataSource().getConnection();
                Statement statement = con.createStatement();
                ResultSet rs = statement.executeQuery("select value from ds_sys_server_parameter where name='SPLIT_COLUMN_FILTER'");
                if(rs != null) {
                    while (rs.next()) {
                        String info = rs.getString(1);
                        JSONObject json = (JSONObject) JSONObject.parse(info);
                        for(String key : json.keySet()) {
                            splitColumnInfo.put(key, json.getString(key));
                        }
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if(con != null) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return splitColumnInfo;
    }

    public static String getProperty(String key,String defaultValue) {
        return p.getProperty(key,defaultValue).trim();
    }
    public static String getProperty(String key) {
        return p.getProperty(key).trim();
    }

    public static String getWebServicePort() {
        return getProperty("WEB_SERVICE.PORT");
    }
    public static String getWebServiceUrl() {
        return getProperty("WEB_SERVICE.URL");
    }
    public static String getWebServiceToken() {
        return getProperty("WEB_SERVICE.TOKEN");
    }
    public static String getWebServiceIp() {
        return getProperty("WEB_SERVICE.IP");
    }
    public static String getInnerHbaseHost() {
        throw new RuntimeException("现在没有默认的数据库, 2.0取消默认库后改变的");
//        return getProperty("INNER_HBASE_HOST");
    }

    public static Integer getInnerHbasePort() {
        throw new RuntimeException("现在没有默认的数据库, 2.0取消默认库后改变的");
//        return Integer.valueOf(getProperty("INNER_HBASE_PORT"));
    }

    public static String getSparkMasterUrl() {
        return getProperty("SPARK_MASTER_URL");
    }

    public static String getSparkJarLocation() {
        return getProperty("SPARK_JAR_LOCATION");
    }

    public static String getSparkDriverExtraJavaOptions() {
        return getProperty("spark_driver_extraJavaOptions");
    }

    public static String getSparkExecutorExtraJavaOptions() {
        return getProperty("spark_executor_extraJavaOptions");
    }

    public static String getSparkYarnEngineJarsDir() {
        return getProperty("SPARK_YARN_ENGINE_JARS_DIR");
    }

    public static String getSparkHomeDir() {
        return getProperty("SPARK_HOME_DIR");
    }

    public static String getSparkYarnQueue() {
        return getProperty("SPARK_YARN_QUEUE");
    }

    public static String getSparkExecutorCores() {
        return getProperty("SPARK_EXECUTOR_CORES");
    }

    public static String getSparkExecutorInstances() {
        return getProperty("SPARK_EXECUTOR_INSTANCES");
    }

    public static String getSparkDriverMemory() {
        return getProperty("SPARK_DRIVER_MEMORY");
    }

    public static String getSparkProxyUser() {
        return getProperty("SPARK_PROXY_USER");
    }

    public static String getEngineRpcServerIp() {
        return getProperty("ENGINE_RPC_SERVER_IP");
    }

    public static String getServerRpcServerIp() {
        return getProperty("SERVER_RPC_SERVER_IP");
    }

    public static int getServerRpcServerPort() {
        return Integer.parseInt(getProperty("SERVER_RPC_SERVER_PORT"));
    }

    public static int getEngineRpcServerPort() {
        return Integer.parseInt(getProperty("ENGINE_RPC_SERVER_PORT"));
    }

    public static String getReportRpcServerIp() {
        return getProperty("REPORT_RPC_SERVER_IP");
    }

    public static int getReportRpcServerPort() {
        return Integer.parseInt(getProperty("REPORT_RPC_SERVER_PORT"));
    }

    public static String getCrawlerRpcServerIp() {
        return getProperty("CRAWLER_RPC_SERVER_IP");
    }

    public static int getCrawlerRpcServerPort() {
        return Integer.parseInt(getProperty("CRAWLER_RPC_SERVER_PORT"));
    }

    public static String getKafkaBrokerList() {
        return getProperty("KAFKA_BROKER_LIST");
    }

    public static String getKafkaLogTopic() {
        return getProperty("KAFKA_LOG_TOPIC");
    }

    public static String getKafkaZookeeperAddress() {
        return getProperty("KAFKA_ZOOKEEPER_ADDRESS");
    }

    public static String getSchedulerLocation() {
        return getProperty("SCHEDULER_LOCATION");
    }

    public static int getParallelNum() {
        return Integer.parseInt(getProperty("PARALLEL_NUM"));
    }
    public static int getMaxQueueSize(){
        return Integer.parseInt(getProperty("WAIT_QUEUE_SIZE"));
    }
    public static String getSparkExecutorMemory() {
        return getProperty("SPARK_EXECUTOR_MEMORY");
    }
    public static String getSparkDynamicAllocationEnabled() {
        return getProperty("SPARK_DYNAMICALLOCATION_ENABLED");
    }
    public static String getSparkConfig() {
        return getProperty("SPARK_CONFIG");
    }
    public static String getSparkDynamicAllocationConfig() {
        return getProperty("SPARK_DYNAMICALLOCATION_CONFIG");
    }
    public static int getSqlPageSize() {
        return Integer.parseInt(getProperty("SQL_PAGE_SIZE"));
    }

    public static boolean isLocal() {
        return Boolean.parseBoolean(getProperty("IS_LOCAL"));
    }

    public static boolean enableHive() {
        return Boolean.parseBoolean(getProperty("ENABLE_HIVE"));
    }

    public static boolean startThriftServer() {
        return Boolean.parseBoolean(getProperty("START_THRIFTSERVER"));
    }

    public static boolean isStartSchedule() {
        return Boolean.parseBoolean(getProperty("IS_START_SCHEDULE"));
    }

    public static String getInnerDataMiningMySQLHost() {
        return getProperty("INNER_MYSQL_HOST");
   }

    public static Integer getInnerDataMiningMySQLPort() {
        return Integer.valueOf(getProperty("INNER_MYSQL_PORT"));
   }

    public static String getInnerDataMiningMySQLDatabaseName() {
        return getProperty("INNER_DataMining_MYSQL_DATABASENAME");
    }

    public static String getInnerDataMiningMySQLUserName() {
        return getProperty("INNER_MYSQL_USERNAME");
    }

    public static String getInnerDataMiningMySQLUserPassword() {
        return getProperty("INNER_MYSQL_PASSWORD");
    }

    public static String version() {
        return "2.0.0";
    }

    /**
     * hbase 默认分片数
     * @return
     */
    public static int HBASE_EXTRACT_PARTITIONS_DEFAULT() {
        return Integer.parseInt(p.getProperty("HBASE_EXTRACT_PARTITIONS_DEFAULT"));
    }

    public static String spark_driver_host() {
        return p.getProperty("SPARK_DRIVER_HOST");
    }

    public static String CLOUD_HDFS_FS_DEFAULTFS() {
        return p.getProperty("CLOUD_HDFS_FS_DEFAULTFS");
    }

    public static String CLOUD_HDFS_PRIVATE_DATASHIRE_SPACE() {
        return p.getProperty("CLOUD_HDFS_PRIVATE_DATASHIRE_SPACE");
    }

    public static String CLOUD_HDFS_PUBLIC_DATASHIRE_SPACE() {
        return p.getProperty("CLOUD_HDFS_PUBLIC_DATASHIRE_SPACE");
    }

    public static String TRAIN_HDFS_DATASHIRE_SPACE(){
        return p.getProperty("train_file_host");
    }
    public static String TRAIN_HDFS_REAL_HOSTS(){
        return p.getProperty("train_file_real_host");
    }
    public static String TRAIN_DB_DATASHIRE_SPACE(){
        return p.getProperty("train_db_host");
    }
    public static String TRAIN_DB_REAL_HOSTS(){
        return p.getProperty("train_db_real_host");
    }
    public static String CLOUD_DB_IP_PORT() {
        return p.getProperty("CLOUD_DB_IP_PORT");
    }

    public static String CLOUD_DB_PRIVATE_DATASHIRE__IP_PORT() {
        return p.getProperty("CLOUD_DB_PRIVATE_DATASHIRE__IP_PORT");
    }

    public static String CLOUD_DB_PUBLIC_DATASHIRE__IP_PORT() {
        return p.getProperty("CLOUD_DB_PUBLIC_DATASHIRE__IP_PORT");
    }

    public static String getDbURLByRepositoryId(int repositoryId){
        String dbUrl = "";
        int p_num = repositoryId % Integer.parseInt(ConfigurationUtil.getProperty("CLOUD_DB_NUM"));
        dbUrl = ConfigurationUtil.getProperty("CLOUD_DB_IP_PORT"+p_num);
        return dbUrl;
    }
    /**
     * 获取系统默认时区
     * @return
     */
    public static String SYSTEM_TIME_ZONE() {
        return p.getProperty("SYSTEM_TIME_ZONE");
    }

    /**
     * 是否保存日志到数据库
     * @return
     */
    public static boolean isLogToDB() {
        return Boolean.parseBoolean(getProperty("IS_LOG_TO_DB"));
    }

    public static String getThriftServerPort() {
        return getProperty("HIVE_SERVER2_THRIFT_PORT");
    }
}
