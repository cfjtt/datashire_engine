package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.dao.ApplicationStatusDao;
import com.eurlanda.datashire.engine.dao.SFJobHistoryDao;
import com.eurlanda.datashire.engine.dao.SFJobModuleLogDao;
import com.eurlanda.datashire.engine.dao.SSquidFlowDao;
import com.eurlanda.datashire.engine.dao.ScheduleDao;
import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.dao.TransformationInputsDao;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mail.javamail.JavaMailSender;

import javax.sql.DataSource;

/**
 * Created by zhudebin on 14-4-26.
 */
public class ConstantUtil {

    public static ApplicationContext context;

    public static void init() {
        // spring
        context = new ClassPathXmlApplicationContext(new String[] {"conf/applicationContext.xml"});
    }

    private static ApplicationContext getContext() {
        return context;
    }

    private static <T> T getBean(String beanName, Class<T> tClass) {
        return getContext().getBean(beanName, tClass);
    }

    private static <T> T getBean(Class<T> tClass) {
        return getContext().getBean(tClass);
    }

    public static DataSource getSysDataSource() {
//        return SimpleBeanFactory.getBeanFactory().getSysDataSource();
        return getBean("dataSource_sys", DataSource.class);
    }

    public static SquidFlowDao getSquidFlowDao() {
        return getBean(SquidFlowDao.class);
    }

    public static JdbcTemplate getJdbcTemplate() {
        return getBean("sysJdbcTemplate", JdbcTemplate.class);
    }

    /**
     *
     * @return
     */
    public static DataSource getSysDataMiningDataSource() {
        return getBean("dataSource_sys_DataMining", DataSource.class);
    }

    /**
     * 获取webCloud的链接信息
     * @return
     */
    public static DataSource getWebCloudDataSource(){
        return getBean("dataSource_web_cloud",DataSource.class);
    }
    /**
     *
     * @return
     */
    public static JdbcTemplate getDataMiningJdbcTemplate(){
        return getBean("dmJdbcTemplate", JdbcTemplate.class);
    }

    /**
     * 获取webCloud jdbc模板
     * @return
     */
    public static JdbcTemplate getWebCloudJdbcTemplate(){
        return getBean("webCloudJdbcTemplate",JdbcTemplate.class);
    }
    public static SSquidFlowDao getSSquidFlowDao() {
        return getBean(SSquidFlowDao.class);
    }

    public static ApplicationStatusDao getApplicationStatusDao() {
        return getBean(ApplicationStatusDao.class);
    }

    public static JavaMailSender getJavaMailSender() {
        return getBean(JavaMailSender.class);
    }

    public static SFJobHistoryDao getSFJobHistoryDao() {
        return getBean(SFJobHistoryDao.class);
    }

    public static ScheduleDao getScheduleDao() {
        return getBean(ScheduleDao.class);
    }

    public static TransformationInputsDao getTransformationInputsDao() {
        return getBean(TransformationInputsDao.class);
    }

    public static SFJobModuleLogDao getSFJobModuleLogDao() {
        return getBean(SFJobModuleLogDao.class);
    }
}
