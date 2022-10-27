package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.exception.TargetSquidNotPersistException;
import com.eurlanda.datashire.engine.spark.DatabaseUtils;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.FileFolderSquid;
import com.eurlanda.datashire.entity.FtpSquid;
import com.eurlanda.datashire.entity.HBaseConnectionSquid;
import com.eurlanda.datashire.entity.HdfsSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.enumeration.DataBaseType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 验证 squidflow
 * Created by zhudebin on 16/4/5.
 */
public class SquidFlowValidator {

    private SquidFlow squidFlow;

    public SquidFlowValidator(SquidFlow squidFlow) {
        this.squidFlow = squidFlow;
    }

    public Map<String, String> validate() {
        Map<String, String> messages=validateDataFallTableCreated();
        if(messages.size()!=0){
            return messages;
        }else{
            return validateSquidLinked();
        }

    }

    public Map<String, String> validateDataFallTableCreated() {
        ArrayList<Squid> squidList = (ArrayList<Squid>) squidFlow.getSquidList();
        ArrayList<DbSquid> dbSquids = selectDbSquids(squidList);

        for(Squid squid:squidList){
            if(squid instanceof DataSquid){
                DataSquid dataSquid = (DataSquid)squid;
                if(dataSquid.isIs_persisted()){
                    int dataSourceId=dataSquid.getDestination_squid_id();
                    if(dataSourceId==0){
                        throw new TargetSquidNotPersistException(dataSquid.getName());
                        /**
                        Configuration conf= HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum", ConfigurationUtil.getInnerHbaseHost());
                        conf.set("hbase.zookeeper.property.clientPort", String.valueOf(ConfigurationUtil.getInnerHbasePort()));
                        org.apache.hadoop.hbase.client.Connection connection=null;
                        Admin admin = null;
                        try {
                            connection = ConnectionFactory.createConnection(conf);
                            admin=connection.getAdmin();
                            //存表的时候
                            boolean istableExists=admin.tableExists(TableName.valueOf(dataSquid.getTable_name().toUpperCase()));
                            if(istableExists){
                                return new HashMap<String,String>();
                            }else{
                                return makeMessages(squid,squidFlow,"","没创建落地对象");
                            }

                        } catch (IOException e) {
                            e.printStackTrace();
                        }finally {
                            try {
                                admin.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            try {
                                connection.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        */

                    }else{
                        DbSquid dataSource =null;
                        for (DbSquid dbSquid:dbSquids){
                            if(dataSourceId==dbSquid.getId()){
                                dataSource=dbSquid;
                                break;
                            }
                        }
                        TDataSource tDataSource = new TDataSource();
                        tDataSource.setDbName(dataSource.getDb_name());
                        tDataSource.setUserName(dataSource.getUser_name());
                        tDataSource.setPassword(dataSource.getPassword());
                        tDataSource.setType(DataBaseType.parse(dataSource.getDb_type()));
                        tDataSource.setTableName(dataSquid.getTable_name());
                        tDataSource.setHost(dataSource.getHost());
                        tDataSource.setPort(dataSource.getPort());
                        return handlerConnection(tDataSource,dataSquid);
                    }

                }

            }
        }



        return new HashMap<String,String>();
    }


    public Map<String, String> validateSquidLinked(){
        ArrayList<Squid> squidList = (ArrayList<Squid>) squidFlow.getSquidList();
        List<SquidLink> squidLinks= squidFlow.getSquidLinkList();
        ArrayList<Integer> toList = new ArrayList<>();
        ArrayList<Integer> fromList = new ArrayList<>();
        for (SquidLink squidLink:squidLinks){
            fromList.add(squidLink.getFrom_squid_id());
            toList.add(squidLink.getTo_squid_id());
        }
        for (Squid squid:squidList) {
            if(squid instanceof HBaseConnectionSquid||
               squid instanceof FileFolderSquid||
               squid instanceof DbSquid||
               squid instanceof FtpSquid||
               squid instanceof HdfsSquid
               ){
                if(toList.contains(squid.getId())){
                    return makeMessages(squid,squidFlow,"","此suqid必须建立连接");
                }
            }else{
                if(toList.contains(squid.getId())){

                }else{
                    return makeMessages(squid,squidFlow,"","此suqid必须建立连接");
                }
            }
        }

        return new HashMap<String,String>();
    }

    private  ArrayList<DbSquid> selectDbSquids( ArrayList<Squid> squidList){
        ArrayList<DbSquid> dbSquids = new ArrayList<DbSquid>();
        for(Squid squid:squidList){
            if(squid instanceof DbSquid){
                dbSquids.add((DbSquid)squid);
            }
        }
        return dbSquids;
    }
    private Map<String, String> handlerConnection(TDataSource dataSource,Squid squid){
        Connection conn = null;
        ResultSet rs = null;
        Statement stmt = null;
        String tableName=null;
        DataBaseType dataBaseType = dataSource.getType();
        try {
            conn = DatabaseUtils.getConnection(dataSource);

            stmt = conn.createStatement();
            if (dataBaseType == DataBaseType.MYSQL) {
                rs=stmt.executeQuery("select table_name from information_schema.tables where TABLE_NAME='" + dataSource.getTableName()+"'");
            }else if(dataBaseType==DataBaseType.ORACLE){
                //oracle中 dba_tables中的表是大写的
                rs=stmt.executeQuery(" select table_name from dba_tables where table_name='" + dataSource.getTableName().toUpperCase()+"'");

            }else if(dataBaseType==DataBaseType.SQLSERVER){
                rs=stmt.executeQuery("SELECT * FROM sys.objects WHERE name='"+ dataSource.getTableName()+"'");
            }else{
                throw new RuntimeException("没有匹配到对应的数据库" + dataBaseType);
            }
            while(rs.next()){
                tableName = rs.getString(1) ;
            }
            if(tableName==null){
                return makeMessages(squid,squidFlow,"","没创建落地对象");
            }else{
                return new HashMap<String,String>();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {   // 关闭记录集
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {   // 关闭声明
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {  // 关闭连接对象
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
        return new HashMap<String,String>();
    }

    public  Map<String,String> makeMessages(Squid squid, SquidFlow squidFlow,String messageSqiudFlow,String messageSquid){
        Map<String, String> messages = new HashMap<String, String>();
        messages.put("squidflow","id:"+squidFlow.getId()+",message:\""+messageSqiudFlow+"\"");
        messages.put("squids","[{id:"+squid.getId()+",message:\""+messageSquid+"\"}]");
        return messages;
    }


}
