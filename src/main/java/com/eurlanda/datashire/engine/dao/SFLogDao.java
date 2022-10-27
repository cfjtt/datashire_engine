
package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.engine.entity.SFLog;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Juntao.Zhang on 2014/5/28.
 */
public class SFLogDao {
    private Log logger = LogFactory.getLog(SFJobHistoryDao.class);

    private DataSource dataSource;

    public SFLogDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void insert(List<SFLog> logs) {
        if (CollectionUtils.isEmpty(logs)) return;
        try {
            Connection conn = dataSource.getConnection();
            PreparedStatement pst = conn.prepareStatement(
                    "UPSERT INTO DS_SYS.SF_LOG" +
                            "(id,job_id,repository_id,repository_name,project_id,project_name," +
                            "squid_flow_id,squid_flow_name,squid_id,squid_name,task_id,message," +
                            "log_level,create_time,tsquid_id,tsquid_type) " +
                            "values(NEXT VALUE FOR DS_SYS_SF_LOG.id_sequence,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE(),?,?)"
            );
            conn.setAutoCommit(false);
            for (SFLog log : logs) {
                pst.setInt(1, log.getJobId());
                pst.setInt(2, log.getRepositoryId());
                pst.setString(3, log.getRepositoryName());
                pst.setInt(4, log.getProjectId());
                pst.setString(5, log.getProjectName());
                pst.setInt(6, log.getSquidFlowId());
                pst.setString(7, log.getSquidFlowName());
                pst.setInt(8, log.getSquidId());
                pst.setString(9, log.getSquidName());
                pst.setString(10, log.getTaskId());
                pst.setString(11, log.getMessage());
                pst.setInt(12, log.getLogLevel());
                pst.setString(13, log.getTSquidId());
                pst.setInt(14, log.getTSquidType());
                pst.addBatch();
            }
            pst.executeBatch();
            conn.commit();
            conn.close();
            logger.debug("插入日志成功，数量为：" + logs.size());
        } catch (SQLException e) {
            logger.error("插入SFLog失败", e);
        }
    }

    public boolean insert(SFLog log) {
        try {
            Connection conn = dataSource.getConnection();
            PreparedStatement pst = conn.prepareStatement(
                    "UPSERT INTO DS_SYS.SF_LOG" +
                            "(id,job_id,repository_id,repository_name,project_id,project_name," +
                            "squid_flow_id,squid_flow_name,squid_id,squid_name,task_id,message," +
                            "log_level,create_time,tsquid_id,tsquid_type) " +
                            "values(NEXT VALUE FOR DS_SYS_SF_LOG.id_sequence,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE(),?,?)"
            );
            pst.setInt(1, log.getJobId());
            pst.setInt(2, log.getRepositoryId());
            pst.setString(3, log.getRepositoryName());
            pst.setInt(4, log.getProjectId());
            pst.setString(5, log.getProjectName());
            pst.setInt(6, log.getSquidFlowId());
            pst.setString(7, log.getSquidFlowName());
            pst.setInt(8, log.getSquidId());
            pst.setString(9, log.getSquidName());
            pst.setString(10, log.getTaskId());
            pst.setString(11, log.getMessage());
            pst.setInt(12, log.getLogLevel());
            pst.setString(13, log.getTSquidId());
            pst.setInt(14, log.getTSquidType());
            pst.executeUpdate();
            conn.commit();
            conn.close();
        } catch (Exception e) {
            logger.error("插入SFLog失败", e);
            return false;
        }

        return true;
    }

    public synchronized void initTable() throws SQLException {
        Connection conn = dataSource.getConnection();
        String createSequenceSql = "create sequence if not exists ds_sys_sf_log.id_sequence";
        String createTableSql = "create table if not exists ds_sys.sf_log (id integer not null," +
                "job_id integer," +
                "repository_id integer," +
                "repository_name varchar(100)," +
                "project_id integer," +
                "project_name varchar(100)," +
                "squid_flow_id integer," +
                "squid_flow_name  varchar(100)," +
                "squid_id integer," +
                "squid_name varchar(100)," +
                "task_id varchar(36)," +
                "message varchar(4000)," +
                "log_level integer," +
                "create_time date," +
                "tsquid_id varchar(10)," +
                "tsquid_type integer constraint sf_log_pk primary key (id))";
        conn.createStatement().executeUpdate("DROP TABLE IF EXISTS ds_sys.sf_log");
        conn.createStatement().executeUpdate("DROP TABLE IF EXISTS ds_sys.sf_job_history");
        conn.createStatement().executeUpdate("DROP TABLE IF EXISTS ds_sys.sf_job_module_log");
        conn.createStatement().executeUpdate(createTableSql);
        conn.createStatement().executeUpdate(createSequenceSql);
        conn.createStatement().executeUpdate(
                "create table if not exists ds_sys.sf_job_history (" +
                        "task_id varchar(36) not null," +
                        "squid_flow_id integer," +
                        "repository_id integer," +
                        "status integer," +
                        "debug_squids varchar(500)," +
                        "destination_squids varchar(500)," +
                        "caller varchar(10)," +
                        "create_time date," +
                        "update_time date " +
                        " constraint sf_job_history_pk primary key (task_id))"
        );
        conn.createStatement().executeUpdate(
                "create table if not exists ds_sys.sf_job_module_log (" +
                        "task_id varchar(36) not null," +
                        "squid_id integer not null," +
                        "status integer," +
                        "create_time date," +
                        "update_time date " +
                        " constraint sf_job_module_log_pk primary key (task_id,squid_id))"
        );
        conn.commit();
        conn.close();
    }


}
