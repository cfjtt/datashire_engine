package com.eurlanda.datashire.engine.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileFilter;

/**
 * Created by zhudebin on 16/8/25.
 */
public class DeployUtil {

    private static Log log = LogFactory.getLog(DeployUtil.class);

    public static void main(String[] args) throws Exception {
        // 将 依赖的jar包 上传到 配置文件中的hdfs://ehadoop/user/datashire/spark200-ds-jars/
        if(args.length != 1) {
//            throw new IllegalArgumentException("请输入参数 jar包地址");
            args = new String[]{"/Users/zhudebin/Documents/iworkspace/datashire_svn/datashire_engine/trunk/spark-yarn-libs"};
        }
        // 创建文件夹
        HdfsUtil.mkdir(ConfigurationUtil.getSparkYarnEngineJarsDir());

        File file = new File(args[0]);
        if(file.isDirectory()) {
            File[] files = file.listFiles(new FileFilter() {
                @Override public boolean accept(File pathname) {
                    return (! pathname.getName().startsWith(".")) && pathname.isFile();
                }
            });
            for(File f : files) {
                HdfsUtil.uploadFile(f.getAbsolutePath(), ConfigurationUtil.getSparkYarnEngineJarsDir());
                log.info("上传文件:" + f.getAbsolutePath());
            }
        }

        try {
            HdfsUtil.setAllACL(ConfigurationUtil.getSparkYarnEngineJarsDir());
        } catch (Exception e) {
            log.error("授权失败...");
        }
        log.info("===== 成功初始上传jars 到hdfs =====");
    }
}
