package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.utility.EnumException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangwen on 2016/4/22.
 */
@Repository
public class TransformationInputsDao {

    @Resource(name="dataSource_sys")
    private DataSource dataSource;
    @Resource(name = "sysJdbcTemplate")
    private JdbcTemplate jdbcTemplate;


    public List<Integer> getTargetInputType(int transtype) throws EnumException {
        ArrayList<Integer> arrayList = new ArrayList<>();
        String code = TransformationTypeEnum.valueOf(transtype).toString();
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            //get squid flow
            List<Map<String, Object>> records=jdbcTemplate.queryForList("SELECT * from DS_TRAN_INPUT_DEFINITION where code='"+code+"'");
            for(Map<String, Object> map:records){
                arrayList.add(Integer.valueOf(""+map.get("INPUT_DATA_TYPE")));
            }
            return arrayList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
