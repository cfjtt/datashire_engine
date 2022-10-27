package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.KafkaExtractSquid;
import com.eurlanda.datashire.entity.KafkaSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.entity.StreamStageSquid;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 14-4-25.
 */
@Repository
public class SSquidFlowDao {

    @Resource(name="dataSource_sys")
    private DataSource dataSource;
    @Autowired
    private SquidFlowDao squidFlowDao;

    public SquidFlow getSSquidFlow(int squidFlowId) {
        Connection conn = null;
        SquidFlow flow = null;
        try {
            conn = dataSource.getConnection();
            //get squid flow
            flow = getSquidFlow(squidFlowId, conn);
            //获得squid所有子类
            flow.setSquidList(getSSquidList(squidFlowId, conn));
            //set squid link
            flow.setSquidLinkList(squidFlowDao.getSquidLinks(conn, squidFlowId));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return flow;
    }

    private SquidFlow getSquidFlow(int squidFlowId, Connection conn) throws SQLException {
        String sql = "select * from DS_SQUID_FLOW where ID=  "+squidFlowId;
        List<SquidFlow> flows = HyperSQLManager.query2List(conn, false, sql, null, SquidFlow.class);
        if(flows == null || flows.size() != 1) {
            throw new EngineException("找不到对应的squidflow,id:" + squidFlowId);
        }
        return flows.get(0);
    }

    //获得所有squid
    private List<Squid> getSSquidList(int squidFlowId, Connection conn) throws SQLException {
        List<Squid> squidList = new ArrayList<>();
        String sql =null;

        // ---------- 数据库抽取 -----------
//        squidList.addAll(squidFlowDao.getDBSquid(conn, squidFlowId));

        // -------------  批处理的squid  ----------------
        squidList.addAll(squidFlowDao.getSquidList(squidFlowId, conn));
//        squidList.addAll(getExtractSquids(conn, squidFlowId));


        // 先落地到 数据库 所以需要查出数据库连接squid
        // 流式数据源 squid

        // kafka
        //sql = "select * from DS_KAFKA_CONNECTION as ds inner join DS_SQUID s on s.id=ds.id where s.SQUID_FLOW_ID = "+squidFlowId;
        sql = "select ID,ZKQUORUM,SQUID_FLOW_ID,SQUID_TYPE_ID,NAME  from DS_SQUID where SQUID_TYPE_ID="+SquidTypeEnum.KAFKA.value()+" and SQUID_FLOW_ID="+squidFlowId;
        List<KafkaSquid> kafkaSquids = HyperSQLManager.query2List(conn, false, sql, null,KafkaSquid.class);
        squidList.addAll(kafkaSquids);

        // kafka extract
        //sql = "select * from DS_KAFKA_EXTRACT as ds inner join DS_SQUID s on s.id=ds.id where s.squid_type_id= "+SquidTypeEnum.KAFKAEXTRACT.value()+" and s.SQUID_FLOW_ID =  "+squidFlowId;
        sql = "select ID,IS_PERSISTED,TABLE_NAME,DESTINATION_SQUID_ID,NUMPARTITIONS,GROUP_ID,SOURCE_TABLE_ID,SQUID_TYPE_ID,SQUID_FLOW_ID,NAME,FILTER from DS_SQUID where SQUID_TYPE_ID="
                +SquidTypeEnum.KAFKAEXTRACT.value()+" and SQUID_FLOW_ID="+squidFlowId;

        List<KafkaExtractSquid> kafkaExtractSquids = HyperSQLManager.query2List(conn, false, sql, null, KafkaExtractSquid.class);
        for(KafkaExtractSquid squid: kafkaExtractSquids){
            squid.setTransformationLinks(squidFlowDao.getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
            squid.setTransformations(squidFlowDao.getTransformations(conn, squid.getId()));
            squid.setColumns(getColumns(conn, squid.getId()));
            squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
        }
        squidList.addAll(kafkaExtractSquids);

        // stream stage squid
        //sql = "select * from DS_DATA_SQUID as ss inner join DS_SQUID s on s.id=ss.id where s.squid_type_id=" + SquidTypeEnum.STREAM_STAGE.value() + " and s.SQUID_FLOW_ID = " + squidFlowId;
        sql = "select ID,NAME,DESCRIPTION,TABLE_NAME,FILTER,SOURCE_IS_SHOW_ALL,IS_SHOW_ALL,SQUID_TYPE_ID,is_persisted,CDC,TRUNCATE_EXISTING_DATA_FLAG,destination_squid_id,process_mode,is_distinct "
                + "from DS_SQUID where squid_type_id = "
                +SquidTypeEnum.STREAM_STAGE.value()+" and squid_flow_id = "+squidFlowId;

        List<StreamStageSquid> streamStageSquids = HyperSQLManager.query2List(conn, false, sql, null, StreamStageSquid.class);
        for(StreamStageSquid squid: streamStageSquids){
            squid.setTransformationLinks(squidFlowDao.getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
            squid.setTransformations(squidFlowDao.getTransformations(conn, squid.getId()));
            squid.setColumns(getColumns(conn, squid.getId()));
            squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            squid.setJoins(squidFlowDao.getSquidJoins(conn, squid.getId()));
        }
        squidList.addAll(streamStageSquids);

        return squidList;
    }

    private List<Column> getColumns(Connection conn, int squidId) throws SQLException {
        List<Column> columns = HyperSQLManager.query2List(conn, false, "select * from DS_COLUMN where SQUID_ID="+squidId, null, Column.class);
        return columns;
    }

    private List<ReferenceColumn> getReferenceColumns(Connection conn, int squidId, String squidName) throws SQLException {

        return HyperSQLManager.query2List(conn, false,
                "select c.*,g.name as  group_name from DS_REFERENCE_COLUMN c left join ds_squid g on c.host_squid_id=g.id where "
                        + squidName + "="+squidId, null, ReferenceColumn.class);

    }
}
