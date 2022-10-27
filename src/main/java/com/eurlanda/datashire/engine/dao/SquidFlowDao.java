package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.common.webService.UpdateProperty;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.JsonUtil;
import com.eurlanda.datashire.engine.util.ListUtil;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.*;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.model.DataCatchSquid;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.server.model.SamplingSquid;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhudebin on 14-4-25.
 */
@Repository
public class SquidFlowDao {

    @Resource(name="dataSource_sys")
    private DataSource dataSource;
    @Resource(name = "sysJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    /**
     * 修改属性
     * @param squidFlowId
     * @param suiqidName
     * @param tableName
     * @param updatePropertyList
     * @return
     */
    public int updateProperty(Integer squidFlowId,String suiqidName,String tableName,List<UpdateProperty> updatePropertyList){
        if(updatePropertyList!=null&&updatePropertyList.size()>0&&suiqidName!=null&&squidFlowId!=null){
            StringBuffer sb = new StringBuffer();
            sb.append( "update DS_SQUID");
            boolean b = false;
            int i =0;
            List<String> sList = new ArrayList<String>();
            for(UpdateProperty up:updatePropertyList){
                String propertyName=up.getProperty();
                String value = up.getValue();
                if(propertyName!=null) {
                    if(b){
                        sb.append(",");
                    }else{
                        sb.append(" set ");
                    }
                    sb.append(propertyName);
                    sb.append("=?");
                    sList.add(value);
                    b=true;
                }
            }
            sList.add(squidFlowId+"");
            sList.add(suiqidName);
            sb.append( " where squid_flow_id=? and name=?");//squidFlowId//suiqidName
            jdbcTemplate.update(sb.toString(),sList.toArray());
            return 0;
        }
        return -1;
    }

    public SquidFlow getSquidFlow(int squidFlowId) {
        Connection conn = null;
        SquidFlow flow = null;
        try {
            conn = dataSource.getConnection();
            //get squid flow
            flow = getSquidFlow(squidFlowId, conn);
            //获得squid所有子类
            flow.setSquidList(getSquidList(squidFlowId, conn));
            //set squid link
            flow.setSquidLinkList(getSquidLinks(conn, squidFlowId));

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

    /**
     * 获取 squidflowInfo
     * @param squidFlowId
     * @return  PROJECTNAME
     *          REPOSITORYNAME
     *          PROJECTID
     *          REPOSITORYID
     */
    public Map<String, Object> getSquidFlowInfo(int squidFlowId) {

        String sql = "select PR.NAME AS PROJECTNAME, RE.NAME AS REPOSITORYNAME, PR.ID as PROJECTID, "
                + "PR.REPOSITORY_ID as REPOSITORYID from (select PROJECT_ID from DS_SQUID_FLOW where ID="
                + squidFlowId + ") as a left join DS_PROJECT PR on a.PROJECT_ID=PR.ID "
                + "LEFT JOIN DS_SYS_REPOSITORY RE ON PR.REPOSITORY_ID=RE.ID";
        return jdbcTemplate.queryForMap(sql);
    }

    public List<DSVariable> getVariableList(int squidFlowId, int projectId) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            String sql = "select * from DS_VARIABLE where (project_id=  "+projectId + " and squid_flow_id=0) or (squid_flow_id=" + squidFlowId + ")";
            return HyperSQLManager.query2List(conn,false,sql,null,DSVariable.class);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try {
                if(conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
        /**
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            //get squid flow
            String sql = "select * from DS_VARIABLE where (project_id=  "+projectId + " and squid_flow_id=0) or (squid_flow_id=" + squidFlowId + ")";
            return HyperSQLManager.query2List(conn, false, sql, null, DSVariable.class);
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
        } */
    }


    public List<DSVariable> getVariableList(int squidFlowId) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            //get squid flow
            String sql = "select * from DS_VARIABLE where (project_id=(select PROJECT_ID from DS_SQUID_FLOW dsf where id="+squidFlowId+") and squid_flow_id=0) or (squid_flow_id="+squidFlowId+")";
            return HyperSQLManager.query2List(conn, false, sql, null, DSVariable.class);
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

    public int updateVariableValue(DSVariable dSVariable){
        Integer returnCode = 0;
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            //get squid flow
            return jdbcTemplate.update(
                    "update DS_VARIABLE set variable_value = '" + dSVariable.getVariable_value()
                            + "' where id=" + dSVariable.getId());
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
            return returnCode;
        }
    }

    /**
     * 获得所有squid
     * @param squidFlowId
     * @param conn
     * @return
     * @throws SQLException
     */
    public List<Squid> getSquidList(int squidFlowId, Connection conn) throws SQLException {
        List<Squid> squidList = new ArrayList<>();
        String sql =null;

        //获取ds_squid表里面的相应的SQUID_FLOW_ID的SQUID_TYPE_ID集合,并去重。
        sql = "SELECT DISTINCT SQUID_TYPE_ID FROM ds_squid WHERE SQUID_FLOW_ID =" + squidFlowId ;
        List<Object> params = new ArrayList<>();
        List<Map<String,Object>> squid_type_id_list = HyperSQLManager.query2List(conn,false, sql,params);

        List id_List = new ArrayList();
        for (Map<String,Object> map : squid_type_id_list) {
            for (Object obj : map.keySet()) {
                String key = (String) obj;
                int value = (Integer) map.get(key);
                id_List.add(value);
            }
        }
        // ---------- 数据库抽取 -----------
        //DBSOURCE(0)
        int dbsquid_squid_type_id = SquidTypeEnum.DBSOURCE.value();
        int clouddbTypeId = SquidTypeEnum.CLOUDDB.value();
        int traindbTypeId = SquidTypeEnum.TRAININGDBSQUID.value();
        if (id_List.contains(dbsquid_squid_type_id) || id_List.contains(clouddbTypeId) || id_List.contains(traindbTypeId)){
            squidList.addAll(getDBSquid(conn, squidFlowId));
        }
        //EXTRACT(2)
        int extract_squid_type_id = SquidTypeEnum.EXTRACT.value();
        if (id_List.contains(extract_squid_type_id)){
            squidList.addAll(getExtractSquids(conn, squidFlowId));
        }

        // ---------- stage --------------
        //STAGE(3)
        int stage_squid_type_id = SquidTypeEnum.STAGE.value();
        if (id_List.contains(stage_squid_type_id)){
            squidList.addAll(getStageSquids(conn, squidFlowId));
        }
        // ---------- report --------------
        // REPORT(6)
        int report_squid_type_id = SquidTypeEnum.REPORT.value();
        if (id_List.contains(report_squid_type_id)){
            squidList.addAll(getReportSquids(conn, squidFlowId));
        }

        // ---------- GISMap -----------------
        // GISMAP(31)
        int gismap_squid_type_id = SquidTypeEnum.GISMAP.value();
        if (id_List.contains(gismap_squid_type_id)){
            squidList.addAll(getMapSquids(conn, squidFlowId));
        }


        //----------CONNECTION----------
        /**
         //web 已经取消该功能
         sql = "select * from DS_WEB_CONNECTION as ds inner join DS_SQUID s on s.id=ds.id where s.SQUID_FLOW_ID = "+squidFlowId;
         List<WebSquid> WEB = HyperSQLManager.query2List(conn, false, sql, null,WebSquid.class);
         squidList.addAll(WEB);

         //weibo
         sql = "select * from DS_WEIBO_CONNECTION as ds inner join DS_SQUID s on s.id=ds.id where s.SQUID_FLOW_ID = "+squidFlowId;
         List<WebSquid> WEIBO = HyperSQLManager.query2List(conn, false, sql, null,WebSquid.class);
         squidList.addAll(WEIBO);
         */


        //查询HDFS的squid是否在表里面；
        //获HDFS的squid的SQUID_TYPE_ID
        int hdfs_squid_type_id = SquidTypeEnum.HDFS.value();
        int sourcecloudfileTypeId = SquidTypeEnum.SOURCECLOUDFILE.value();
        int trainfileTypeId = SquidTypeEnum.TRAINNINGFILESQUID.value();
        // HDFS HDFS(14)
        //判断HDFS是否存在，存在则执行查询语句。否则不执行
        if (id_List.contains(hdfs_squid_type_id)
                || id_List.contains(sourcecloudfileTypeId)
                || id_List.contains(trainfileTypeId)){
            sql = "select ID,HOST,USER_NAME,PASSWORD,FILE_PATH,UNIONALL_FLAG,INCLUDING_SUBFOLDERS_FLAG," +
                    "SQUID_FLOW_ID,NAME,SQUID_TYPE_ID,TABLE_NAME,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL,FILTER,ENCODING,MAX_TRAVEL_DEPTH from DS_SQUID where SQUID_FLOW_ID = "
                    +squidFlowId +" and SQUID_TYPE_ID in ("+sourcecloudfileTypeId+","+hdfs_squid_type_id+","+trainfileTypeId+")";
            List<HdfsSquid> hdfsSquidList = HyperSQLManager.query2List(conn, false, sql, null,HdfsSquid.class);
            for(HdfsSquid squid: hdfsSquidList){
                // 对数猎场新增的cloudFileSquid的中的host替换成真实的地址
                if(ConfigurationUtil.CLOUD_HDFS_PUBLIC_DATASHIRE_SPACE().equals(squid.getHost())
                        || ConfigurationUtil.CLOUD_HDFS_PRIVATE_DATASHIRE_SPACE().equals(squid.getHost())) {
                    squid.setHost(ConfigurationUtil.CLOUD_HDFS_FS_DEFAULTFS());
                }
                if(ConfigurationUtil.TRAIN_HDFS_DATASHIRE_SPACE().equals(squid.getHost())){
                    squid.setHost(ConfigurationUtil.TRAIN_HDFS_REAL_HOSTS());
                }
                squid.setSourceTableList(getSourceTableList(conn, squid.getId()));
            }
            squidList.addAll(hdfsSquidList);
        }

        //FILE FILEFOLDER(12)
        int filefolder_squid_type_id = SquidTypeEnum.FILEFOLDER.value();
        if (id_List.contains(filefolder_squid_type_id)){
            sql="select ID,host,USER_NAME,PASSWORD,FILE_PATH,INCLUDING_SUBFOLDERS_FLAG,UNIONALL_FLAG," +
                    "SQUID_FLOW_ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL,FILTER,ENCODING,MAX_TRAVEL_DEPTH from DS_SQUID where SQUID_FLOW_ID="
                    +squidFlowId+" and SQUID_TYPE_ID="+filefolder_squid_type_id;
            List<FileFolderSquid> fileFolderSquids = HyperSQLManager.query2List(conn, false, sql, null,FileFolderSquid.class);
            for(FileFolderSquid squid: fileFolderSquids){
                squid.setSourceTableList(getSourceTableList(conn, squid.getId()));
            }
            squidList.addAll(fileFolderSquids);
        }



        //FTP FTP(13)
        int ftp_squid_type_id = SquidTypeEnum.FTP.value();
        if (id_List.contains(ftp_squid_type_id)){
            sql = "select ID,host,USER_NAME,PASSWORD,FILE_PATH,INCLUDING_SUBFOLDERS_FLAG,UNIONALL_FLAG,POSTPROCESS,PROTOCOL,ENCRYPTION,ALLOWANONYMOUS_FLAG,MAXCONNECTIONS,TRANSFERMODE_FLAG," +
                    "SQUID_FLOW_ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL,FILTER,ENCODING,MAX_TRAVEL_DEPTH from DS_SQUID where SQUID_TYPE_ID="
                    +ftp_squid_type_id+" and SQUID_FLOW_ID="+squidFlowId;
            List<FtpSquid> ftpSquids = HyperSQLManager.query2List(conn, false, sql, null,FtpSquid.class);
            for(FtpSquid squid: ftpSquids){
                squid.setSourceTableList(getSourceTableList(conn, squid.getId()));
            }
            squidList.addAll(ftpSquids);
        }



        // mongodb MONGODB(38)
        int mongodb_squid_type_id = SquidTypeEnum.MONGODB.value();
        if (id_List.contains(mongodb_squid_type_id)){
            sql = "select ID,DB_TYPE_ID,HOST,PORT,USER_NAME,PASSWORD,DATABASE_NAME," +
                   "SQUID_FLOW_ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL,FILTER,ENCODING,MAX_TRAVEL_DEPTH from DS_SQUID where SQUID_TYPE_ID="
                    +mongodb_squid_type_id+" and SQUID_FLOW_ID="+squidFlowId;
            List<NOSQLConnectionSquid> nosql = HyperSQLManager.query2List(conn, false, sql, null,NOSQLConnectionSquid.class);
            for(NOSQLConnectionSquid squid : nosql) {
                squid.setSourceTableList(getSourceTableList(conn, squid.getId()));
            }
            squidList.addAll(nosql);
        }



        // hbase  HBASE(43)
        int hbase_squid_type_id = SquidTypeEnum.HBASE.value();
        if (id_List.contains(hbase_squid_type_id)){
            sql = "select ID,URL," +
                    "SQUID_FLOW_ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL,FILTER,ENCODING,MAX_TRAVEL_DEPTH from DS_SQUID where SQUID_FLOW_ID = "
                    +squidFlowId +" and SQUID_TYPE_ID="+hbase_squid_type_id;
            //sql = "select * from DS_HBASE_CONNECTION as ds inner join DS_SQUID s on s.id=ds.id where s.SQUID_FLOW_ID = "+squidFlowId;
            //sql = "select ID,URL from DS_SQUID where SQUID_FLOW_ID="+squidFlowId+" and SQUID_TYPE_ID="+hbase_squid_type_id;
            List<HBaseConnectionSquid> hbasesql = HyperSQLManager.query2List(conn, false, sql, null,HBaseConnectionSquid.class);
            for(HBaseConnectionSquid squid : hbasesql) {
                squid.setSourceTableList(getSourceTableList(conn, squid.getId()));
            }
            squidList.addAll(hbasesql);
        }



        // hive connection HIVE(55)
        int hive_squid_type_id = SquidTypeEnum.HIVE.value();
        if (id_List.contains(hive_squid_type_id)){
            sql = "select ID,DB_TYPE_ID,HOST,PORT,USER_NAME,PASSWORD,DATABASE_NAME," +
                    "SQUID_FLOW_ID,NAME,SQUID_TYPE_ID,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL,FILTER,ENCODING,MAX_TRAVEL_DEPTH from DS_SQUID where SQUID_FLOW_ID="
                    +squidFlowId+" and SQUID_TYPE_ID="+hive_squid_type_id;
            List<SystemHiveConnectionSquid> hiveConns = HyperSQLManager.query2List(conn, false, sql, null,SystemHiveConnectionSquid.class);
            for(SystemHiveConnectionSquid squid : hiveConns) {
                squid.setSourceTableList(getSourceTableList(conn, squid.getId()));
            }
            squidList.addAll(hiveConns);
        }



        // cassandra connection CASSANDRA(57)
        int cassandra_squid_type_id = SquidTypeEnum.CASSANDRA.value();
        if (id_List.contains(cassandra_squid_type_id)){
            //sql = "select * from ds_cassandra_sql_connection as ds inner join DS_SQUID s on s.id=ds.id and s.squid_type_id= "
            //   + SquidTypeEnum.CASSANDRA.value() + " where s.SQUID_FLOW_ID = "+squidFlowId;
            sql="select ID,HOST,PORT,keyspace,cluster,verification_mode,username,password,DB_TYPE_ID," +
                    "SQUID_FLOW_ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL,FILTER,ENCODING,MAX_TRAVEL_DEPTH from DS_SQUID where SQUID_FLOW_ID="
                    +squidFlowId+" and  SQUID_TYPE_ID="+cassandra_squid_type_id;
            List<CassandraConnectionSquid> cassandraConns = HyperSQLManager.query2List(conn, false, sql, null,CassandraConnectionSquid.class);
            for(CassandraConnectionSquid squid : cassandraConns) {
                squid.setSourceTableList(getSourceTableList(conn, squid.getId()));
            }
            squidList.addAll(cassandraConns);
        }


        //-----------EXTRACT------------------------

        // WEB EXTRACT WEBEXTRACT(10)
        int webextract_squid_type_id = SquidTypeEnum.WEBEXTRACT.value();
        if (id_List.contains(webextract_squid_type_id)){
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where squid_type_id = "+webextract_squid_type_id+" and squid_flow_id = "+squidFlowId;
            List<WebExtractSquid> webExtractSquids = HyperSQLManager.query2List(conn, false, sql, null,WebExtractSquid.class);
            for(WebExtractSquid squid: webExtractSquids){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(webExtractSquids);
        }

        // WEIBO EXTRACT WEIBOEXTRACT(11)
        int weiboextract_squid_type_id = SquidTypeEnum.WEIBOEXTRACT.value();
        if (id_List.contains(weiboextract_squid_type_id)){
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where squid_type_id = "+weiboextract_squid_type_id+" and squid_flow_id = "+squidFlowId;
            List<WeiBoExtractSquid> weiBoExtractSquids = HyperSQLManager.query2List(conn, false, sql, null,WeiBoExtractSquid.class);
            for(WeiBoExtractSquid squid: weiBoExtractSquids){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(weiBoExtractSquids);
        }

        // WEBLOG EXTRACT WEBLOGEXTRACT(9)
        int weblogextract_squid_type_id = SquidTypeEnum.WEBLOGEXTRACT.value();
        if (id_List.contains(weblogextract_squid_type_id)){
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where squid_type_id = "+weblogextract_squid_type_id+" and squid_flow_id = "+squidFlowId;
            List<WebLogExtractSquid> webLogExtractSquids = HyperSQLManager.query2List(conn, false, sql, null,WebLogExtractSquid.class);
            for(WebLogExtractSquid squid: webLogExtractSquids){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(webLogExtractSquids);
        }

        // XML EXTRACT XML_EXTRACT(8)
        int xml_extract_squid_type_id = SquidTypeEnum.XML_EXTRACT.value();
        if (id_List.contains(xml_extract_squid_type_id)){
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where squid_type_id = "+xml_extract_squid_type_id+" and squid_flow_id = "+squidFlowId;
            List<XmlExtractSquid> xmlExtractSquids = HyperSQLManager.query2List(conn, false, sql, null,XmlExtractSquid.class);
            for(XmlExtractSquid squid: xmlExtractSquids){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(xmlExtractSquids);
        }

        // FILE EXTRACT DOC_EXTRACT(7)
        int doc_extract_squid_type_id = SquidTypeEnum.DOC_EXTRACT.value();
        if (id_List.contains(doc_extract_squid_type_id)){
            sql = "select ID,NAME,IS_PERSISTED,TABLE_NAME,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG,PROCESS_MODE,CDC,"
                    + "EXCEPTION_HANDLING_FLAG,DOC_FORMAT,ROW_FORMAT,DELIMITER,FIELD_LENGTH,HEADER_ROW_NO,FIRST_DATA_ROW_NO,ROW_DELIMITER,"
                    + "ROW_DELIMITER_POSITION,SKIP_ROWS,POST_PROCESS,SOURCE_TABLE_ID,UNION_ALL_FLAG,COMPRESSICON_CODEC," +
                    "DESCRIPTION,SQUID_TYPE_ID,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value "+
                    "from DS_SQUID where SQUID_FLOW_ID="+squidFlowId+" and SQUID_TYPE_ID="+doc_extract_squid_type_id;

            List<DocExtractSquid> docExtractSquids = HyperSQLManager.query2List(conn, false, sql, null,DocExtractSquid.class);
            for(DocExtractSquid squid: docExtractSquids){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(docExtractSquids);
        }


        // MONGO EXTRACT  MONGODBEXTRACT(39)
        int mongodbextract_squid_type_id = SquidTypeEnum.MONGODBEXTRACT.value();
        if (id_List.contains(mongodbextract_squid_type_id)){
            //sql = "select * from DS_DATA_SQUID as ds inner join DS_SQUID s on s.id=ds.id where s.squid_type_id= "+SquidTypeEnum.MONGODBEXTRACT.value()+" and s.SQUID_FLOW_ID =  "+squidFlowId;
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where squid_type_id = "+mongodbextract_squid_type_id+" and squid_flow_id = "+squidFlowId;
            List<MongodbExtractSquid> mongodbExtractSquids = HyperSQLManager.query2List(conn, false, sql, null, MongodbExtractSquid.class);
            for(MongodbExtractSquid squid: mongodbExtractSquids){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(mongodbExtractSquids);
        }

        // hbase extract HBASEEXTRACT(44)
        int hbaseextract_squid_type_id = SquidTypeEnum.HBASEEXTRACT.value();
        if (id_List.contains(hbaseextract_squid_type_id)){
            // sql = "select * from DS_HBASE_EXTRACT as ds inner join DS_SQUID s on s.id=ds.id where s.squid_type_id= "+SquidTypeEnum.HBASEEXTRACT.value()+" and s.SQUID_FLOW_ID =  "+squidFlowId;
            sql ="select ID,IS_PERSISTED,TABLE_NAME,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG,PROCESS_MODE,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,FILTER_TYPE,SCAN,CODE," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,POST_PROCESS,incremental_mode,check_column_id,last_value," +
                    "SQUID_FLOW_ID,SQUID_TYPE_ID,NAME,FILTER,MAX_TRAVEL_DEPTH from DS_SQUID where SQUID_FLOW_ID="+squidFlowId+" and SQUID_TYPE_ID="+hbaseextract_squid_type_id;
            List<HBaseExtractSquid> hes = HyperSQLManager.query2List(conn, false, sql, null, HBaseExtractSquid.class);
            for(HBaseExtractSquid squid: hes){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(hes);
        }


        // hive EXTRACT HIVEEXTRACT(56)
        int hiveextract_squid_type_id = SquidTypeEnum.HIVEEXTRACT.value();
        if (id_List.contains(hiveextract_squid_type_id)){
            //sql = "select * from DS_DATA_SQUID as ds inner join DS_SQUID s on s.id=ds.id where s.squid_type_id= "+SquidTypeEnum.HIVEEXTRACT.value()+" and s.SQUID_FLOW_ID =  "+squidFlowId;
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where squid_type_id = "+hiveextract_squid_type_id+" and squid_flow_id = "+squidFlowId;
            List<SystemHiveExtractSquid> hiveExtrs = HyperSQLManager.query2List(conn, false, sql, null,SystemHiveExtractSquid.class);
            for(SystemHiveExtractSquid squid: hiveExtrs){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(hiveExtrs);
        }


        // cassandra EXTRACT CASSANDRA_EXTRACT(58)
        int cassandra_extract_squid_type_id = SquidTypeEnum.CASSANDRA_EXTRACT.value();
        if (id_List.contains(cassandra_extract_squid_type_id)){
            //sql = "select * from DS_DATA_SQUID as ds inner join DS_SQUID s on s.id=ds.id where s.squid_type_id= "+SquidTypeEnum.CASSANDRA_EXTRACT.value()+" and s.SQUID_FLOW_ID =  "+squidFlowId;
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where squid_type_id = "+cassandra_extract_squid_type_id+" and squid_flow_id = "+squidFlowId;
            List<CassandraExtractSquid> cassandraExtrs = HyperSQLManager.query2List(conn, false, sql, null, CassandraExtractSquid.class);
            for(CassandraExtractSquid squid: cassandraExtrs){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(cassandraExtrs);
        }



        // group tagging GROUPTAGGING(54)
        int grouptagging_squid_type_id = SquidTypeEnum.GROUPTAGGING.value();
        if (id_List.contains(grouptagging_squid_type_id)){
            //sql = "select * from DS_DATA_SQUID as ss inner join DS_SQUID s on s.id=ss.id where s.squid_type_id="+SquidTypeEnum.GROUPTAGGING.value()+" and s.SQUID_FLOW_ID = "+squidFlowId;
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where SQUID_TYPE_ID = "+grouptagging_squid_type_id+" AND SQUID_FLOW_ID = "+squidFlowId;
            List<GroupTaggingSquid> gts = HyperSQLManager.query2List(conn, false, sql,null,GroupTaggingSquid.class);
            for(GroupTaggingSquid squid: gts){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                if (CollectionUtils.isNotEmpty(squid.getTransformationLinks())) {
                    Set<Integer> fromTransformationId = new HashSet<>();
                    for (TransformationLink link : squid.getTransformationLinks()) {
                        fromTransformationId.add(link.getFrom_transformation_id());
                    }
                    squid.setFromTransformations(getTransformationsByIds(conn, fromTransformationId));
                }
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(gts);
        }

        // UserDefinedSquid
        int userDefinedSquidTypeId = SquidTypeEnum.USERDEFINED.value();
        if (id_List.contains(userDefinedSquidTypeId)){
            //sql = "select * from ds_userdefined_squid as ss inner join DS_SQUID s on s.id=ss.id where s.squid_type_id="+SquidTypeEnum.USERDEFINED.value()+" and s.SQUID_FLOW_ID = "+squidFlowId;
            sql = "select ID,selectClassName,SQUID_FLOW_ID,NAME,SQUID_TYPE_ID,TABLE_NAME,FILTER from DS_SQUID where SQUID_TYPE_ID="+userDefinedSquidTypeId+" and SQUID_FLOW_ID="+squidFlowId;
            List<UserDefinedSquid> squids = HyperSQLManager.query2List(conn, false, sql,null,UserDefinedSquid.class);
            for(UserDefinedSquid squid: squids) {
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
                // 查询输入列映射关系
                String mappingColumnSql = "select * from ds_userdefined_datamap_column where squid_id=" + squid.getId();
                List<UserDefinedMappingColumn> mappingColumns = HyperSQLManager.query2List(conn, false, mappingColumnSql,null,UserDefinedMappingColumn.class);
                squid.setUserDefinedMappingColumns(mappingColumns);
                // 查询参数信息
                String parameterColumnSql = "select * from DS_USERDEFINED_PARAMETERS_COLUMN where squid_id=" + squid.getId();
                List<UserDefinedParameterColumn> parameterColumns = HyperSQLManager.query2List(conn, false, parameterColumnSql,null,UserDefinedParameterColumn.class);
                squid.setUserDefinedParameterColumns(parameterColumns);

                // 查询类名,将类名设置为 UserDefinedSquid 的别名
                String classNameSql = "select * from third_jar_definition where alias_name='" + squid.getSelectClassName() + "'";
                String className = (String)HyperSQLManager.query2Object(conn, false, classNameSql,null).get("class_name".toUpperCase());
                squid.setSelectClassName(className);
            }
            squidList.addAll(squids);
        }

        // dataming  LOGREG(21)NAIVEBAYES(22),SVM(23),KMEANS(24),ALS(25),
        // LINEREG(26),RIDGEREG(27),QUANTIFY(28), DISCRETIZE(29),DECISIONTREE(30),ASSOCIATION_RULES(48),
        //   LASSO(63),  RANDOMFORESTCLASSIFIER(64), RANDOMFORESTREGRESSION(65), MULTILAYERPERCEPERONCLASSIFIER(66);
        boolean dataMining_flag =false;
        int[] dataMiningNum = new int[]{21,22,23,24,25,26,27,28,29,30,48,63,64,65,66,67,68,71,72,75}; // 表ds_squid_type中的code
        for(int i = 0;i<dataMiningNum.length;i++){
            if(id_List.contains(dataMiningNum[i])){
                dataMining_flag = true;
                break;
            }
        }
        if(dataMining_flag){
            //sql = "select * from DS_DM_SQUID as ds inner join DS_SQUID s on s.id=ds.id where s.SQUID_FLOW_ID =  "+squidFlowId;
            sql = "select ID,TRAINING_PERCENTAGE,VERSIONING,MIN_BATCH_FRACTION,ITERATION_NUMBER,STEP_SIZE,SMOOTHING_PARAMETER,REGULARIZATION,"
                    + "K,PARALLEL_RUNS,INITIALIZATION_MODE,IMPLICIT_PREFERENCES,CASE_SENSITIVE," +
                    "MIN_VALUE,MAX_VALUE,BUCKET_COUNT,SEED,ALGORITHM,MAX_DEPTH,IMPURITY,MAX_BINS,CATEGORICAL_SQUID,MIN_SUPPORT,MIN_CONFIDENCE,aggregation_depth," +
                    "fit_intercept,solver,standardization,tolerance,tree_number,feature_subset_strategy,min_info_gain,subsampling_rate,initialweights,layers,"
                    + "max_categories,feature_subset_scale,method," +
                    "NAME,SQUID_TYPE_ID,SQUID_FLOW_ID,TABLE_NAME,FILTER,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL ,x_model_squid_id,y_model_squid_id ," +
                    " family,elastic_net_param,min_instances_per_node,threshold ,model_type , minDivisibleClusterSize," +
                    " init_Steps " +
                    " from DS_SQUID where SQUID_TYPE_ID in ("+ JsonUtil.toJSONString(dataMiningNum).replaceAll("\\[|]","")+") and SQUID_FLOW_ID="+squidFlowId;
            List<DataMiningSquid> dms = HyperSQLManager.query2List(conn, false, sql, null,DataMiningSquid.class);
            for(DataMiningSquid squid: dms){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                if (CollectionUtils.isNotEmpty(squid.getTransformationLinks())) {
                    Set<Integer> fromTransformationId = new HashSet<>();
                    for (TransformationLink link : squid.getTransformationLinks()) {
                        fromTransformationId.add(link.getFrom_transformation_id());
                    }
                    squid.setFromTransformations(getTransformationsByIds(conn, fromTransformationId));
                }
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(dms);

        }


        // exception  EXCEPTION(20)
        int exception_squid_type_id = SquidTypeEnum.EXCEPTION.value();
        if (id_List.contains(exception_squid_type_id)){
            //sql = "select * from DS_DATA_SQUID as ss inner join DS_SQUID s on s.id=ss.id where s.squid_type_id="+SquidTypeEnum.EXCEPTION.value()+" and s.SQUID_FLOW_ID = "+squidFlowId;
            sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                    "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                    "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                    "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                    "incremental_mode,check_column_id,last_value from DS_SQUID where squid_type_id = "+exception_squid_type_id+" and squid_flow_id = "+squidFlowId;
            List<ExceptionSquid> exs = HyperSQLManager.query2List(conn, false, sql,null,ExceptionSquid.class);
            for(ExceptionSquid squid: exs){
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                if (CollectionUtils.isNotEmpty(squid.getTransformationLinks())) {
                    Set<Integer> fromTransformationId = new HashSet<>();
                    for (TransformationLink link : squid.getTransformationLinks()) {
                        fromTransformationId.add(link.getFrom_transformation_id());
                    }
                    squid.setFromTransformations(getTransformationsByIds(conn, fromTransformationId));
                }
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
            }
            squidList.addAll(exs);
        }


        // destEsSquid DESTES(40)
        int destes_squid_type_id = SquidTypeEnum.DESTES.value();
        if (id_List.contains(destes_squid_type_id)){
            //sql = "select * from DS_DEST_ES_SQUID as es inner join DS_SQUID s on s.id=es.id where s.squid_type_id="+SquidTypeEnum.DESTES.value()+" and s.SQUID_FLOW_ID = "+squidFlowId;
            sql = "select ID,ip,ESINDEX,ESTYPE,SQUID_FLOW_ID,NAME,SQUID_TYPE_ID,TABLE_NAME,IS_SHOW_ALL,SOURCE_IS_SHOW_ALL,FILTER from DS_SQUID where SQUID_TYPE_ID="
                    +destes_squid_type_id+" and SQUID_FLOW_ID="+squidFlowId;
            List<DestESSquid> destESSquids = HyperSQLManager.query2List(conn, false, sql,null,DestESSquid.class);
            squidList.addAll(destESSquids);
        }


        // destHdfsSquid DEST_HDFS(46)
        int dest_hdfs_squid_type_id = SquidTypeEnum.DEST_HDFS.value();
        int destcloudfileTypeId = SquidTypeEnum.DESTCLOUDFILE.value();
        if (id_List.contains(dest_hdfs_squid_type_id) || id_List.contains(destcloudfileTypeId)){
            //sql = "select * from ds_dest_hdfs_squid as es inner join DS_SQUID s on s.id=es.id where s.SQUID_FLOW_ID = "+squidFlowId;
            sql = "select ID,HOST,HDFS_PATH,FILE_FORMATE,ZIP_TYPE,SAVE_TYPE,ROW_DELIMITER,COLUMN_DELIMITER," +
                    "NAME,SQUID_FLOW_ID,SQUID_TYPE_ID,TABLE_NAME,FILTER from DS_SQUID where SQUID_TYPE_ID IN ("+dest_hdfs_squid_type_id+","+destcloudfileTypeId+") and SQUID_FLOW_ID="+squidFlowId;
            List<DestHDFSSquid> destHDFSSquids = HyperSQLManager.query2List(conn, false, sql,null,DestHDFSSquid.class);
            for(DestHDFSSquid squid : destHDFSSquids) {
                // 对数猎场新增的DestCloudFile的中的host替换成真实的地址
                if(ConfigurationUtil.CLOUD_HDFS_PUBLIC_DATASHIRE_SPACE().equals(squid.getHost())
                        || ConfigurationUtil.CLOUD_HDFS_PRIVATE_DATASHIRE_SPACE().equals(squid.getHost())) {
                    squid.setHost(ConfigurationUtil.CLOUD_HDFS_FS_DEFAULTFS());
                }
            }
            squidList.addAll(destHDFSSquids);
        }

        // destImpalaSquid DEST_IMPALA(47)
        int dest_impala_squid_type_id = SquidTypeEnum.DEST_IMPALA.value();
        if (id_List.contains(dest_impala_squid_type_id)){
            //sql = "select * from ds_dest_impala_squid as es inner join DS_SQUID s on s.id=es.id where s.squid_type_id="+SquidTypeEnum.DEST_IMPALA.value()+" and s.SQUID_FLOW_ID = "+squidFlowId;
            sql = "select ID,HOST,STORE_NAME,IMPALA_TABLE_NAME,AUTHENTICATION_TYPE,NAME,SQUID_TYPE_ID,SQUID_FLOW_ID,TABLE_NAME,FILTER from DS_SQUID where SQUID_TYPE_ID="
                    +dest_impala_squid_type_id+" and SQUID_FLOW_ID="+squidFlowId;
            List<DestImpalaSquid> destImpalaSquids = HyperSQLManager.query2List(conn, false, sql,null,DestImpalaSquid.class);
            squidList.addAll(destImpalaSquids);

        }

        // destHdfsSquid DEST_HIVE(60)
        int destSystemHiveSquidType = SquidTypeEnum.DEST_HIVE.value();
        if (id_List.contains(destSystemHiveSquidType)){
            //sql = "select * from ds_dest_hive_squid as es inner join DS_SQUID s on s.id=es.id where s.SQUID_FLOW_ID = "+squidFlowId;
            sql = "select  id,save_type,table_name,db_name,NAME,SQUID_TYPE_ID,SQUID_FLOW_ID,FILTER from DS_SQUID where SQUID_TYPE_ID="+destSystemHiveSquidType+" and SQUID_FLOW_ID="+squidFlowId;
            List<DestHiveSquid> destHiveSquids = HyperSQLManager.query2List(conn, false, sql,null,DestHiveSquid.class);
            squidList.addAll(destHiveSquids);
        }

        // StatisticsSquid
        int statistics_squid_type_id = SquidTypeEnum.STATISTICS.value();
        if(id_List.contains(statistics_squid_type_id)) {
            //sql = "select * from ds_statistics_squid as ss inner join DS_SQUID s on s.id=ss.id where s.squid_type_id=" + SquidTypeEnum.STATISTICS.value() + " and s.SQUID_FLOW_ID = " + squidFlowId;
            sql = "select id,NAME,statistics_name,SQUID_TYPE_ID,SQUID_FLOW_ID from DS_SQUID where SQUID_TYPE_id="+statistics_squid_type_id+" and SQUID_FLOW_ID="+squidFlowId;
            List<StatisticsSquid> squids = HyperSQLManager.query2List(conn, false, sql, null, StatisticsSquid.class);
            for (StatisticsSquid squid : squids) {
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));

                // 输入列
                String mappingColumnSql = "select * from ds_statistics_datamap_column where squid_id=" + squid.getId();
                List<StatisticsDataMapColumn> mappingColumns = HyperSQLManager.query2List(conn, false, mappingColumnSql, null, StatisticsDataMapColumn.class);
                squid.setStatisticsDataMapColumns(mappingColumns);
                // 控制参数
                String parameterColumnSql = "select * from ds_statistics_parameters_column where squid_id=" + squid.getId();
                List<StatisticsParameterColumn> parameterColumns = HyperSQLManager.query2List(conn, false, parameterColumnSql, null, StatisticsParameterColumn.class);
                squid.setStatisticsParametersColumns(parameterColumns);
            }
            squidList.addAll(squids);
        }

        // DestCassandraSquid DEST_CASSANDRA(60)
        int destCassandraSquidType = SquidTypeEnum.DEST_CASSANDRA.value();
        if (id_List.contains(destCassandraSquidType)){
            //sql = "select * from ds_dest_cassandra_squid as es inner join DS_SQUID s on s.id=es.id where s.SQUID_FLOW_ID = "+squidFlowId;
            sql = "select id,save_type,table_name,dest_squid_id,SQUID_TYPE_ID,SQUID_FLOW_ID,NAME,FILTER from DS_SQUID where SQUID_TYPE_ID="+destCassandraSquidType+" and SQUID_FLOW_ID="+squidFlowId;
            List<DestCassandraSquid> destCassandraSquids = HyperSQLManager.query2List(conn, false, sql,null,DestCassandraSquid.class);
            squidList.addAll(destCassandraSquids);
        }

        int dataViewSquidType = SquidTypeEnum.DATAVIEW.value();
        if (id_List.contains(dataViewSquidType)) {
            sql = "select id,SQUID_TYPE_ID,SQUID_FLOW_ID,NAME from DS_SQUID where SQUID_TYPE_ID=" + dataViewSquidType + " and SQUID_FLOW_ID=" + squidFlowId;
            List<DataCatchSquid> dataViewsSquids = HyperSQLManager.query2List(conn, false, sql, null, DataCatchSquid.class);
            for (DataCatchSquid squid : dataViewsSquids) {
                // squid.setColumns(getModelColumns(conn, squid.getId())); // 查询全是null
                List<Column> entityColumns = getColumns(conn, squid.getId());
                squid.setColumns(entityColumns);
            }
           squidList.addAll(dataViewsSquids);
        }

        int coeffSquidType = SquidTypeEnum.COEFFICIENT.value(); // 70  等待servier发布
        if (id_List.contains(coeffSquidType)) {
            sql = "select id,SQUID_TYPE_ID,SQUID_FLOW_ID,NAME from DS_SQUID where SQUID_TYPE_ID=" + coeffSquidType + " and SQUID_FLOW_ID=" + squidFlowId;
            List<DataCatchSquid> dataCatchSquidSquids = HyperSQLManager.query2List(conn, false, sql, null, DataCatchSquid.class);
            for (DataCatchSquid squid : dataCatchSquidSquids) {
                List<Column> entityColumns = getColumns(conn, squid.getId());
                squid.setColumns(entityColumns);
            }
            squidList.addAll(dataCatchSquidSquids);
        }

        //SampingSquid (76)
        int sampingSquidType = SquidTypeEnum.SAMPLINGSQUID.value();
        if(id_List.contains(sampingSquidType)){
            sql = "select id,SQUID_TYPE_ID,SQUID_FLOW_ID,NAME,SQUID_TYPE_ID,sampling_percent,source_squidId from ds_squid where SQUID_TYPE_ID="+sampingSquidType+" and SQUID_FLOW_ID="+squidFlowId;
            List<SamplingSquid> samplingSquids = HyperSQLManager.query2List(conn,false,sql,null,SamplingSquid.class);
            for(SamplingSquid squid : samplingSquids){
                //setSourceColumns
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
                //setColums
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                if (CollectionUtils.isNotEmpty(squid.getTransformationLinks())) {
                    Set<Integer> fromTransformationId = new HashSet<>();
                    for (TransformationLink link : squid.getTransformationLinks()) {
                        fromTransformationId.add(link.getFrom_transformation_id());
                    }
                    squid.setFromTransformations(getTransformationsByIds(conn, fromTransformationId));
                }
            }
            squidList.addAll(samplingSquids);
        }
        //pivotSquid
        int pivotSquidType = SquidTypeEnum.PIVOTSQUID.value();
        if(id_List.contains(pivotSquidType)){
            sql = "select id,SQUID_TYPE_ID,SQUID_FLOW_ID,NAME,SQUID_TYPE_ID,pivot_column_value,aggregation_type,pivot_column_id,value_column_id,group_by_column_ids from ds_squid where SQUID_TYPE_ID="+pivotSquidType+" and SQUID_FLOW_ID="+squidFlowId;
            List<PivotSquid> pivotSquids = HyperSQLManager.query2List(conn,false,sql,null,PivotSquid.class);
            for(PivotSquid squid : pivotSquids){
                //setSourceColumns
                squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
                //setColums
                squid.setColumns(getColumns(conn, squid.getId()));
                squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                if (CollectionUtils.isNotEmpty(squid.getTransformationLinks())) {
                    Set<Integer> fromTransformationId = new HashSet<>();
                    Set<Integer> toTransformationId = new HashSet<>();
                    for (TransformationLink link : squid.getTransformationLinks()) {
                        fromTransformationId.add(link.getFrom_transformation_id());
                        toTransformationId.add(link.getTo_transformation_id());
                    }
                    squid.setFromTransformations(getTransformationsByIds(conn, fromTransformationId));
                    squid.setToTransformations(getTransformationsByIds(conn,toTransformationId));
                }
            }
            squidList.addAll(pivotSquids);
        }
        return squidList;
    }

    private void setSquid(int squidFlowId, ResultSet rs, Squid squid) throws SQLException {
        squid.setId(rs.getInt("ID"));
        // squid.setKey(rs.getString("KEY"));
        squid.setName(rs.getString("NAME"));
        squid.setDescription(rs.getString("DESCRIPTION"));
        squid.setTable_name(rs.getString("TABLE_NAME"));
        squid.setFilter(rs.getString("FILTER"));
        squid.setSource_is_show_all(rs.getString("SOURCE_IS_SHOW_ALL").equals("Y")|| rs.getString("SOURCE_IS_SHOW_ALL").equals("y") || rs.getString("SOURCE_IS_SHOW_ALL").equals("1")?true:false);
        squid.setIs_show_all(rs.getString("IS_SHOW_ALL").equals("Y")|| rs.getString("IS_SHOW_ALL").equals("y")||rs.getString("IS_SHOW_ALL").equals("1")?true:false);
        squid.setSquid_type(rs.getInt("SQUID_TYPE_ID"));
        squid.setSquidflow_id(squidFlowId);

    }

    //根据squidFlowId获得DBSquid所有属性

    List<? extends Squid> getDBSquid(Connection conn, int squidFlowId) throws SQLException {
        List<DbSquid> dbSquidList = new ArrayList<>();
        PreparedStatement ps = null;
        String sql = "select ID,NAME,DESCRIPTION,TABLE_NAME,FILTER,SOURCE_IS_SHOW_ALL,IS_SHOW_ALL,SQUID_TYPE_ID,DB_TYPE_ID,HOST,PORT,USER_NAME,PASSWORD,DATABASE_NAME "
                + "from DS_SQUID where squid_type_id in( "
                +SquidTypeEnum.DBSOURCE.value()+","+SquidTypeEnum.CLOUDDB.value()+","+SquidTypeEnum.TRAININGDBSQUID.value()+" ) and squid_flow_id = ?";
        /*String sql = "select * from DS_SQL_CONNECTION as ds inner join DS_SQUID s on s.id=ds.id and s.squid_type_id="
                + SquidTypeEnum.DBSOURCE.value() + " where s.SQUID_FLOW_ID = ? ";*/
        try {
            ps = conn.prepareStatement(sql);
            ps.setInt(1, squidFlowId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                DbSquid squid = new DbSquid();
                setSquid(squidFlowId, rs, squid);
                squid.setDb_type(rs.getInt("DB_TYPE_ID"));
                squid.setHost(rs.getString("HOST"));
                squid.setPort(rs.getInt("PORT"));
                squid.setUser_name(rs.getString("USER_NAME"));
                squid.setPassword(rs.getString("PASSWORD"));
                squid.setDb_name(rs.getString("DATABASE_NAME"));
                //squid.setSourceTableList(getSourceTableList(conn, squid.getId()));
                // 对数猎场新增的DestCloudFile的中的host替换成真实的地址
                if((ConfigurationUtil.CLOUD_DB_PRIVATE_DATASHIRE__IP_PORT()+":3306").equals(squid.getHost())
                        || ConfigurationUtil.TRAIN_DB_DATASHIRE_SPACE().equals(squid.getHost())) {
                    //确定连接信息
                    sql = "select dsr.* from ds_sys_repository dsr,ds_project dp,ds_squid_flow dsf where dsr.id = dp.REPOSITORY_ID and dp.id = dsf.project_id  and dsf.id="+squidFlowId;
                    List<Map<String,Object>> mapList = HyperSQLManager.query2List(conn,false,sql,null);
                    String url = "";
                    if(mapList!=null && mapList.size()>0){
                        int repositoryId = Integer.parseInt(mapList.get(0).get("ID")+"");
                        //获取连接信息
                        url=ConfigurationUtil.getDbURLByRepositoryId(repositoryId);
                    }
                    squid.setHost(url);
                }
                /*if(ConfigurationUtil.TRAIN_DB_DATASHIRE_SPACE().equals(squid.getHost())) {
                    //确定连接信息
                    sql = "select dsr.* from ds_sys_repository dsr,ds_project dp,ds_squid_flow dsf where dsr.id = dp.REPOSITORY_ID and dp.id = dsf.project_id  and dsf.id="+squidFlowId;
                    Map<String,Object> map = HyperSQLManager.query2Object(conn,true,sql,null);
                    String url = "";
                    if(map!=null){
                        int repositoryId = Integer.parseInt(map.get("ID")+"");
                        //获取连接信息
                        url=ConfigurationUtil.getDbURLByRepositoryId(repositoryId);
                    }
                    squid.setHost(url);
                }*/
                dbSquidList.add(squid);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
        return dbSquidList;
    }
    private List<SourceTable> getSourceTableList(Connection conn,int squidId){
        String sql ="select * from ds_source_table where source_squid_id=?";
        List<SourceTable> list = new ArrayList<>();
        // 1. 获取 extract squid
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ps.setInt(1, squidId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                SourceTable squid = new SourceTable();
                squid.setTableName(rs.getString("TABLE_NAME"));
                squid.setId(rs.getInt("ID"));
                list.add(squid);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return list ;
    }
    //根据squidFlowId获得ExtractSquid所有属性
    private List<ExtractSquid> getExtractSquids(Connection conn, int squidFlowId) throws SQLException {
        List<ExtractSquid> extractSquidList = new ArrayList<>();
        // 1. 获取 extract squid
        PreparedStatement ps = null;
        String sql = "select ID,NAME,DESCRIPTION,SQUID_TYPE_ID,TABLE_NAME,FILTER,ENCODING,MAX_TRAVEL_DEPTH,SQUID_FLOW_ID," +
                "IS_INCREMENTAL,INCREMENTAL_EXPRESSION,IS_PERSISTED,DESTINATION_SQUID_ID,IS_INDEXED,TOP_N,TRUNCATE_EXISTING_DATA_FLAG," +
                "PROCESS_MODE,LOG_FORMAT,POST_PROCESS,CDC,EXCEPTION_HANDLING_FLAG,SOURCE_TABLE_ID,UNION_ALL_FLAG,XSD_DTD_FILE,XSD_DTD_PATH," +
                "IS_DISTINCT,REF_SQUID_ID,SPLIT_COL,SPLIT_NUM,WINDOW_DURATION,COLSPLIT_NUMUMN,ENABLE_WINDOW,GROUP_COLUMN,SORT_COLUMN,TAGGING_COLUMN," +
                "incremental_mode,check_column_id,last_value,max_extract_numberPerTimes,is_union_table,table_name_setting_type,table_name_setting, table_name_setting_sql from DS_SQUID where squid_type_id = "+SquidTypeEnum.EXTRACT.value()+" and squid_flow_id = "+squidFlowId;
        //String sql = "select * from DS_DATA_SQUID as es inner join DS_SQUID s on s.id=es.id where s.squid_type_id="+SquidTypeEnum.EXTRACT.value()+" and s.SQUID_FLOW_ID =  "+squidFlowId;
        extractSquidList= HyperSQLManager.query2List(conn, false, sql, null,ExtractSquid.class);
        for (ExtractSquid squid: extractSquidList) {
            //setSquid(squidFlowId, rs, squid);
            //squid.setTruncate_existing_data_flag(rs.getInt("TRUNCATE_EXISTING_DATA_FLAG"));
            // squid.setIs_incremental(rs.getString("IS_INCREMENTAL").equals("Y")?true:false);
            //squid.setIncremental_expression(rs.getString("INCREMENTAL_EXPRESSION"));
            // squid.setSource_table_id(rs.getInt("SOURCE_TABLE_ID"));
            //squid.setCdc(rs.getInt("CDC"));

            squid.setTransformationLinks(getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
            squid.setTransformations(getTransformations(conn, squid.getId()));
            squid.setColumns(getColumns(conn, squid.getId()));
            squid.setSourceColumns(getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));

            //extractSquidList.add(squid);
        }
        return extractSquidList;
    }

    public List<SquidJoin> getSquidJoins(Connection conn, int squidId) throws SQLException {
        List<SquidJoin> squidJoinList = new ArrayList<>();
        // 1. 获取 extract squid
        PreparedStatement ps = null;
        String sql = "select * from DS_JOIN where JOINED_SQUID_ID = ? ";
        try {
            ps = conn.prepareStatement(sql);
            ps.setInt(1, squidId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                SquidJoin squid = new SquidJoin();
                // 警告，此处是因为后台保存数据错误，故反着取
                squid.setTarget_squid_id(rs.getInt("JOINED_SQUID_ID"));
                squid.setJoined_squid_id(rs.getInt("TARGET_SQUID_ID"));
                squid.setPrior_join_id(rs.getInt("PRIOR_JOIN_ID"));
                squid.setJoinType(rs.getInt("JOIN_TYPE_ID"));
                squid.setJoin_Condition(rs.getString("JOIN_CONDITION"));
                squidJoinList.add(squid);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
        return squidJoinList;
    }

    //根据squidFlowId获得StageSquid所有属性
    private List<StageSquid> getStageSquids(Connection conn, int squidFlowId) throws SQLException {
        List<StageSquid> stageSquids = new ArrayList<>();
        PreparedStatement ps = null;
        try {
            String sql = "select ID,NAME,DESCRIPTION,TABLE_NAME,FILTER,SOURCE_IS_SHOW_ALL,IS_SHOW_ALL,SQUID_TYPE_ID,is_persisted,CDC,TRUNCATE_EXISTING_DATA_FLAG,destination_squid_id,"
                    + "process_mode,is_distinct from DS_SQUID where squid_type_id = "+SquidTypeEnum.STAGE.value()+" and squid_flow_id = ?";
            //String sql = "select * from DS_DATA_SQUID as ss inner join DS_SQUID s on s.id=ss.id where s.squid_type_id=? and s.SQUID_FLOW_ID = ? ";
            ps = conn.prepareStatement(sql);
            ps.setInt(1, squidFlowId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                StageSquid squid = new StageSquid();
                setSquid(squidFlowId, rs, squid);
                squid.setIs_persisted(rs.getString("is_persisted").equals("Y") || rs.getString("is_persisted").equals("y")|| rs.getString("is_persisted").equals("1")  ? true : false);
                squid.setTruncate_existing_data_flag(rs.getInt("TRUNCATE_EXISTING_DATA_FLAG"));
                squid.setDestination_squid_id(rs.getInt("destination_squid_id"));
                squid.setIs_distinct(rs.getInt("is_distinct"));
                //Todo:Juntao.Zhang 优化
                squid.setProcess_mode(rs.getInt("process_mode"));
                squid.setCdc(rs.getInt("CDC"));
                squid.setTransformationLinks(
                        getTransformationLinks(conn, squid.getId(), "tl.TO_TRANSFORMATION_ID"));
                squid.setTransformations(getTransformations(conn, squid.getId()));
                squid.setColumns(getColumns(conn, squid.getId()));
                if (CollectionUtils.isNotEmpty(squid.getTransformationLinks())) {
                    Set<Integer> fromTransformationId = new HashSet<>();
                    for (TransformationLink link : squid.getTransformationLinks()) {
                        fromTransformationId.add(link.getFrom_transformation_id());
                    }
                    squid.setFromTransformations(getTransformationsByIds(conn, fromTransformationId));
                }
                squid.setSourceColumns(
                        getReferenceColumns(conn, squid.getId(), "REFERENCE_SQUID_ID"));
                squid.setJoins(getSquidJoins(conn, squid.getId()));
                stageSquids.add(squid);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
        return stageSquids;
    }

    private List<Transformation> getTransformationsByIds(Connection conn, Set<Integer> fromTransformationId) throws SQLException {
        PreparedStatement ps = null;
        List<Transformation> transformations = new ArrayList<>();
        if (CollectionUtils.isEmpty(fromTransformationId)) return transformations;
        try {
            Integer[] fromTransformationIds = fromTransformationId.toArray(new Integer[fromTransformationId.size()]);
            String query = "select * from DS_TRANSFORMATION where ID in (?";
            for (int i = 1; i < fromTransformationIds.length; i++) {
                query += ", ?";
            }
            ps = conn.prepareStatement(query + ")");
            for (int i = 0; i < fromTransformationIds.length; i++) {
                ps.setInt(1 + i, fromTransformationIds[i]);
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Transformation transformation = new Transformation();
                transformation.setId(rs.getInt("ID"));
                //  transformation.setKey(rs.getString("KEY"));
                transformation.setSquid_id(rs.getInt("SQUID_ID"));
                transformation.setTranstype(rs.getInt("TRANSFORMATION_TYPE_ID"));
                transformation.setColumn_id(rs.getInt("COLUMN_ID"));
                transformations.add(transformation);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
        return transformations;
    }

    //根据squidflowId获得GISMapSquid所有属性
    private List<GISMapSquid> getMapSquids(Connection conn, int squidFlowId) throws SQLException {
        List<GISMapSquid> mapSquids = new ArrayList<>();
        PreparedStatement ps = null;
        try {
            String sql = "select ID,NAME,DESCRIPTION,TABLE_NAME,FILTER,SOURCE_IS_SHOW_ALL,IS_SHOW_ALL,SQUID_TYPE_ID,IS_REAL_TIME,FOLDER_ID, "
                    + "IS_SUPPORT_HISTORY,MAX_HISTORY_COUNT,IS_SEND_EMAIL,EMAIL_RECEIVERS,EMAIL_TITLE,MAP_NAME,EMAIL_REPORT_FORMAT,IS_PACKED,IS_COMPRESSED,IS_ENCRYPTED "
                    + "from DS_SQUID  where s.SQUID_FLOW_ID = ? and squid_TYPE_ID="+SquidTypeEnum.GISMAP.value();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, squidFlowId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                GISMapSquid squid = new GISMapSquid();
                //ReportSquid squid = new ReportSquid();
                setSquid(squidFlowId, rs, squid);
                squid.setIs_real_time(rs.getBoolean("IS_REAL_TIME"));
                squid.setFolder_id(rs.getInt("FOLDER_ID"));
                squid.setIs_support_history(rs.getBoolean("IS_SUPPORT_HISTORY"));
                squid.setMax_history_count(rs.getInt("MAX_HISTORY_COUNT"));
                squid.setIs_send_email(rs.getBoolean("IS_SEND_EMAIL"));
                squid.setEmail_receivers(rs.getString("EMAIL_RECEIVERS"));
                squid.setEmail_title(rs.getString("EMAIL_TITLE"));
                squid.setMap_name(rs.getString("MAP_NAME"));
                squid.setEmail_report_format(rs.getInt("EMAIL_REPORT_FORMAT"));
                squid.setIs_packed(rs.getBoolean("IS_PACKED"));
                squid.setIs_compressed(rs.getBoolean("IS_COMPRESSED"));
                squid.setIs_encrypted(rs.getBoolean("IS_ENCRYPTED"));

                // squid.setPassword(rs.getString("PASSWORD"));
                mapSquids.add(squid);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
        return mapSquids;
    }

    //根据squidFlowId获得reportSquid所有属性
    private List<ReportSquid> getReportSquids(Connection conn, int squidFlowId) throws SQLException {
        List<ReportSquid> reportSquids = new ArrayList<>();
        PreparedStatement ps = null;
        try {
            //String sql = "select * from DS_REPORT_SQUID as rs inner join DS_SQUID s on s.id=rs.id where s.SQUID_FLOW_ID = ? ";
            String sql = "select ID,NAME,DESCRIPTION,TABLE_NAME,FILTER,SOURCE_IS_SHOW_ALL,IS_SHOW_ALL,SQUID_TYPE_ID,IS_REAL_TIME,REPORT_NAME,"
                    + "REPORT_TEMPLATE,FOLDER_ID, IS_SUPPORT_HISTORY,MAX_HISTORY_COUNT,IS_SEND_EMAIL,EMAIL_RECEIVERS,EMAIL_TITLE,EMAIL_REPORT_FORMAT,"
                    + "IS_PACKED,IS_COMPRESSED,IS_ENCRYPTED from DS_SQUID  where s.SQUID_FLOW_ID = ? and squid_TYPE_ID="+SquidTypeEnum.REPORT.value();
            ps = conn.prepareStatement(sql);
            ps.setInt(1, squidFlowId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                ReportSquid squid = new ReportSquid();
                setSquid(squidFlowId, rs, squid);
                squid.setIs_real_time(rs.getBoolean("IS_REAL_TIME"));
                squid.setReport_name(rs.getString("REPORT_NAME"));
                squid.setReport_template(rs.getString("REPORT_TEMPLATE"));
                squid.setFolder_id(rs.getInt("FOLDER_ID"));
                squid.setIs_support_history(rs.getBoolean("IS_SUPPORT_HISTORY"));
                squid.setMax_history_count(rs.getInt("MAX_HISTORY_COUNT"));
                squid.setIs_send_email(rs.getBoolean("IS_SEND_EMAIL"));
                squid.setEmail_receivers(rs.getString("EMAIL_RECEIVERS"));
                squid.setEmail_title(rs.getString("EMAIL_TITLE"));
                squid.setEmail_report_format(rs.getInt("EMAIL_REPORT_FORMAT"));
                squid.setIs_packed(rs.getBoolean("IS_PACKED"));
                squid.setIs_compressed(rs.getBoolean("IS_COMPRESSED"));
                squid.setIs_encrypted(rs.getBoolean("IS_ENCRYPTED"));

                // squid.setPassword(rs.getString("PASSWORD"));
                reportSquids.add(squid);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
        return reportSquids;
    }

    List<Transformation> getTransformations(Connection conn, int squidId) throws SQLException {
        Map<Integer, List<TransformationInputs>> tiMap = new HashMap<>();
        // 查询所有的transformationinputs
        // 根据transformation id 分组
        PreparedStatement ps = null;
        PreparedStatement ps_inputs = null;
        List<Transformation> transformations = new ArrayList<>();
        try {
            String sql1 = "select I.* from DS_TRAN_INPUTS I LEFT JOIN DS_TRANSFORMATION T ON I.TRANSFORMATION_ID=T.ID WHERE SQUID_ID=" + squidId +" order by I.relative_order asc";
            List<TransformationInputs> transformationInputsList = HyperSQLManager.query2List(conn, false, sql1, null, TransformationInputs.class);
            for(TransformationInputs tis : transformationInputsList) {
                //去除input的input_value的首尾的''
                if(tis.getSource_type() == 1) {
                    String value = tis.getInput_value();
                    if (value != null) {
                        if (value.startsWith("'")) {
                            value = value.substring(1);
                        }
                        if (value.endsWith("'")) {
                            value = value.substring(0, value.length() - 1);
                        }
                    }
                    tis.setInput_value(value);
                }
                if(tiMap.containsKey(tis.getTransformationId())) {
                    tiMap.get(tis.getTransformationId()).add(tis);
                } else {
                    tiMap.put(tis.getTransformationId(), ListUtil.asList(tis));
                }
            }

            // transformation 属性太多了
            String sql = "select * from DS_TRANSFORMATION where SQUID_ID=" + squidId;
            List<Transformation> trans = HyperSQLManager.query2List(conn, false, sql, null,
                    Transformation.class);
            for(Transformation tran : trans) {
                // 设置 transformationinputs
                tran.setInputs(tiMap.get(tran.getId()));
                transformations.add(tran);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (ps_inputs != null) {
                ps_inputs.close();
            }
        }
        return transformations;
    }

    List<TransformationLink> getTransformationLinks(Connection conn, int squidId, String fromOrToTransformationId) throws SQLException {
        PreparedStatement ps = null;
        List<TransformationLink> links = new ArrayList<>();
        try {
            String sql = "select * from DS_TRANSFORMATION_LINK as tl left join DS_TRANSFORMATION as t on " + fromOrToTransformationId + " = t.ID where t.SQUID_ID=? ";
            ps = conn.prepareStatement(sql);
            ps.setInt(1, squidId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                TransformationLink link = new TransformationLink();
                link.setId(rs.getInt("ID"));
                link.setKey(rs.getString("KEY"));
                link.setFrom_transformation_id(rs.getInt("FROM_TRANSFORMATION_ID"));
                link.setTo_transformation_id(rs.getInt("TO_TRANSFORMATION_ID"));
                link.setIn_order(rs.getInt("IN_ORDER"));
                links.add(link);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
        return links;
    }

    public List<DestHDFSColumn> getHDFSColumns(int squidId) {

        List<DestHDFSColumn> columnList  = jdbcTemplate.query("SELECT * FROM ds_dest_hdfs_column WHERE SQUID_ID=?", new RowMapper<DestHDFSColumn>() {
            @Override public DestHDFSColumn mapRow(ResultSet rs, int rowNum) throws SQLException {
                DestHDFSColumn column = new DestHDFSColumn();
                column.setColumn_id(rs.getInt("column_id"));
                column.setColumn_order(rs.getInt("COLUMN_ORDER"));
                column.setField_name(rs.getString("FIELD_NAME"));
                column.setIs_dest_column(rs.getInt("IS_DEST_COLUMN"));
                column.setSquid_id(rs.getInt("squid_id"));
                column.setId(rs.getInt("id"));
                column.setIs_partition_column(rs.getInt("IS_PARTITION_COLUMN"));
                return column;
            }
        }, squidId);
        return columnList;
    }

    public List<DestHiveColumn> getHiveColumns(int squidId) {

        List<DestHiveColumn> columnList  = jdbcTemplate.query("SELECT * FROM ds_dest_hive_column WHERE SQUID_ID=?", new RowMapper<DestHiveColumn>() {
            @Override public DestHiveColumn mapRow(ResultSet rs, int rowNum) throws SQLException {
                DestHiveColumn column = new DestHiveColumn();
                column.setColumn_id(rs.getInt("column_id"));
                column.setColumn_order(rs.getInt("column_order"));
                column.setField_name(rs.getString("field_name"));
                column.setIs_dest_column(rs.getInt("is_dest_column"));
                column.setSquid_id(rs.getInt("squid_id"));
                column.setId(rs.getInt("id"));
                column.setIs_partition_column(rs.getInt("is_partition_column"));
                return column;
            }
        }, squidId);
        return columnList;
    }

    public List<DestCassandraColumn> getCassandraDestColumns(int squidId) {

        List<DestCassandraColumn> columnList  = jdbcTemplate.query(
                "SELECT * FROM ds_dest_cassandra_column WHERE SQUID_ID=? and column_id > 0",
                new RowMapper<DestCassandraColumn>() {
                    @Override public DestCassandraColumn mapRow(ResultSet rs, int rowNum) throws SQLException {
                        DestCassandraColumn column = new DestCassandraColumn();
                        column.setColumn_id(rs.getInt("column_id"));
                        column.setColumn_order(rs.getInt("column_order"));
                        column.setField_name(rs.getString("field_name"));
                        column.setIs_dest_column(rs.getInt("is_dest_column"));
                        column.setSquid_id(rs.getInt("squid_id"));
                        column.setId(rs.getInt("id"));
                        column.setIs_primary_column(rs.getInt("is_primary_column"));
                        return column;
                    }
                }, squidId);
        return columnList;
    }

    public List<DestImpalaColumn> getImpalaColumns(int squidId) {

        List<DestImpalaColumn> columnList  = jdbcTemplate.query("SELECT * FROM ds_dest_impala_column dc inner join ds_column c on dc.column_id=c.id WHERE dc.SQUID_ID=?",
                new RowMapper<DestImpalaColumn>() {
                    @Override
                    public DestImpalaColumn mapRow(ResultSet rs, int rowNum) throws SQLException {
                        DestImpalaColumn destColumn = new DestImpalaColumn();
                        destColumn.setColumn_id(rs.getInt("column_id"));
                        destColumn.setColumn_order(rs.getInt("COLUMN_ORDER"));
                        destColumn.setField_name(rs.getString("FIELD_NAME"));
                        destColumn.setIs_dest_column(rs.getInt("IS_DEST_COLUMN"));
                        destColumn.setSquid_id(rs.getInt("squid_id"));
                        destColumn.setId(rs.getInt("id"));
                        Column c = new Column();

                        c.setData_type(rs.getInt("data_type"));

                        destColumn.setColumn(c);
                        return destColumn;
                    }
                }, squidId);
        return columnList;
    }

    public List<EsColumn> getEsColumns(int squidId) {
        Connection con = null;
        try {
            con = dataSource.getConnection();
            PreparedStatement pst = con.prepareStatement(
                    "SELECT * FROM DS_ES_COLUMN EC WHERE EC.SQUID_ID=?");
            pst.setInt(1, squidId);
            ResultSet rs = pst.executeQuery();
            List<EsColumn> esColumns = new ArrayList<>();
            while (rs.next()) {
                EsColumn esColumn = new EsColumn();
                esColumn.setColumn_id(rs.getInt("COLUMN_ID"));
                esColumn.setField_name(rs.getString("FIELD_NAME"));
                esColumn.setId(rs.getInt("ID"));
                esColumn.setIs_mapping_id(rs.getInt("IS_MAPPING_ID"));
                esColumn.setIs_persist(rs.getInt("IS_PERSIST"));
                esColumn.setSquid_id(rs.getInt("SQUID_ID"));
                esColumns.add(esColumn);
            }
            return esColumns;
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    /**
     * 获取escolumn column_id 与 field_name映射关系
     * @param squidId
     * @return
     */
    public Map<Integer, String> getESColumnMapBySquidId(int squidId) {
        Connection con = null;
        try {
            con = dataSource.getConnection();
            PreparedStatement pst = con.prepareStatement(
                    "SELECT EC.COLUMN_ID,EC.FIELD_NAME FROM DS_ES_COLUMN EC WHERE EC.SQUID_ID=?");
            pst.setInt(1, squidId);
            ResultSet rs = pst.executeQuery();
            Map<Integer, String> map = new HashMap<>();
            while (rs.next()) {
                map.put(rs.getInt(1), rs.getString(2));
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    private List<Column> getColumns(Connection conn, int squidId) throws SQLException {
        List<Column> columns = HyperSQLManager.query2List(conn, false, "select * from DS_COLUMN where SQUID_ID="+squidId, null, Column.class);
        return columns;
    }

    private List<ReferenceColumn> getReferenceColumns(Connection conn, int squidId, String squidName) throws SQLException {

        return HyperSQLManager.query2List(conn, false, "select c.*,g.name as  group_name from DS_REFERENCE_COLUMN c left join ds_squid g on c.host_squid_id=g.id where "
                + squidName + "="+squidId, null, ReferenceColumn.class);

    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void updateExtractLastValue(int squidId, String lastValue) {
        Connection con = null;
        try {
            con = dataSource.getConnection();
            PreparedStatement pst = con.prepareStatement(
                    "update DS_SQUID set last_value=? where id=?");
            pst.setString(1, lastValue);
            pst.setInt(2, squidId);

            pst.executeUpdate();

        } catch (Exception e) {
            throw new RuntimeException(e);
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

    /**
     * 通过ID查询sourceColumn
     * @param id
     * @return
     */
    public SourceColumn getSourceColumnById(int id) {
        Connection con = null;
        try {
            con = dataSource.getConnection();
            PreparedStatement pst = con.prepareStatement(
                    "select id, name from ds_source_column where id=?");
            pst.setInt(1, id);
            ResultSet rs = pst.executeQuery();

            if(rs.next()) {
                SourceColumn sc = new SourceColumn();
                sc.setId(rs.getInt("id"));
                sc.setName(rs.getString("name"));
                return sc;
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<SquidLink> getSquidLinks(Connection conn, int squidFlowId) throws SQLException {
        List<SquidLink> squidLinks = new ArrayList<>();
        PreparedStatement ps = null;
        try {
            String sql = "select ID,`KEY`,FROM_SQUID_ID,TO_SQUID_ID,LINE_TYPE from DS_SQUID_LINK where SQUID_FLOW_ID = ? ";
            ps = conn.prepareStatement(sql);
            ps.setInt((Integer)1, squidFlowId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                SquidLink link = new SquidLink();
                link.setId(rs.getInt("ID"));
                link.setKey(rs.getString("KEY"));
                link.setFrom_squid_id(rs.getInt("FROM_SQUID_ID"));
                link.setTo_squid_id(rs.getInt("TO_SQUID_ID"));
                link.setType(rs.getInt("LINE_TYPE"));
                link.setSquid_flow_id(squidFlowId);
                squidLinks.add(link);
            }
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
        return squidLinks;
    }

    private SquidFlow getSquidFlow(int squidFlowId, Connection conn) throws SQLException {
        String sql = "select * from DS_SQUID_FLOW where ID=  "+squidFlowId;
        SquidFlow flow = HyperSQLManager.query2List(conn, false, sql, null, SquidFlow.class).get(0);
        return flow;
    }
}
