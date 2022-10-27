package com.eurlanda.datashire.engine.dao;

import com.alibaba.fastjson.JSONObject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 16/1/22.
 */
@Repository
public class ApplicationStatusDao {
    @Resource(name="sysJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    public void saveNewApplicationStatus(int repositoryId, int projectId,
            int squidflowId, String appId, String status, String config) {
        JSONObject json = JSONObject.parseObject(config);
        int userId = json.get("userid") == null ? -1 : Integer.parseInt(json.get("userid").toString());

        jdbcTemplate.update("insert into DS_SYS_APPLICATION_STATUS(repository_id, project_id, squidflow_id, "
                + "application_id, status, config, launch_user_id) values(?, ?, ?, ?, ?, ?, ?)", repositoryId, projectId,
                squidflowId, appId, status, config, userId);
    }

    public void updateApplicationStatus(String appId, String status) {
        jdbcTemplate.update("update DS_SYS_APPLICATION_STATUS SET status=?, "
                + "update_date=CURRENT_TIMESTAMP where application_id=?", status, appId);
    }

    /**
     * 客户端关闭应用时更新应用的状态
     * @param appId
     * @param userId 关闭者的USRID
     */
    public void updateKillApplicationStatus(String appId, int userId) {
        jdbcTemplate.update("update DS_SYS_APPLICATION_STATUS SET status='KILLED', stop_user_id=?, "
                + "update_date=CURRENT_TIMESTAMP where application_id =?", userId, appId);
    }

    /**
     * 根据状态查询applicationStatus
     * @param states 用逗号间隔的状态
     * @return
     */
    public List<Map<String, Object>> getApplicationStatusByState(String states) {
        return jdbcTemplate.queryForList("select * from DS_SYS_APPLICATION_STATUS where status in (" + states + ")");
    }
}
