package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.common.util.HbaseUtil;
import com.eurlanda.datashire.engine.util.IOUtils;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Set;

/**
 * Created by zhudebin on 14-5-27.
 *  落地表要有ID列
 *
 */
public abstract class TTrainSquid extends TSquid {

    private static Log log = LogFactory.getLog(TTrainSquid.class);

    protected TSquid preSquid;
    protected Integer inKey; // 连入到model的列的id
    protected float percentage;  // 训练数据占全体数据的百分比
    protected boolean versioning;   // 是否保留版本
    protected TDataSource tDataSource;  // datasource 模型落地的数据源
    private String tableName;    // 落地的表名
    protected int key; // 连入到分组建模key的列的Id
    protected Set<TSquidType> modelSummaryTSquidTypes ; // 下游连出哪几类Suqid，如系数squid，dataCatchSquid

    /**
     *
     * @return 获取当前模型版本
     */
    protected int init() {
        Connection conn = null;
        String tableName = getTableName();
        try {
            conn = getConnectionFromDS();
            return init(conn,tableName);
        } catch (Exception e) {
            log.error("获取数据库连接异常", e);
            throw new RuntimeException(e);
        }finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     * @return 获取当前模型版本
     */
    protected int init(Connection connection,String tableName ) {
        try {
            int modelVersion = 1;
            if(versioning) { // 新增，获取最大version
                modelVersion = getMaxVersion(connection,tableName) + 1;
            } else { // 覆盖，删除旧数据
                truncateTable(connection,tableName);
                setIdPrimaryKeyAutoIncrementFrom1(connection,tableName);
            }
            if (! isIdPrimaryKey(connection, tableName)) { // 不存在主键
                setIdPrimaryKeyAndAutoIncrement(connection, tableName);
            }
            return modelVersion;
        } catch (Exception e) {
            log.error("获取数据库连接异常", e);
            String errorMsg = e.getMessage();
            if(errorMsg.startsWith("Table") && errorMsg.endsWith("doesn't exist")){
               throw new RuntimeException("不存在落地对象，请创建落地对象");
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取数据库连接
     * @return
     */
    protected Connection getConnectionFromDS() throws Exception {
        DBConnectionInfo info = new DBConnectionInfo();
        info.setDbName(tDataSource.getDbName());
        info.setDbType(tDataSource.getType());
        info.setHost(tDataSource.getHost());
        info.setPort(tDataSource.getPort());
        info.setPassword(tDataSource.getPassword());
        info.setUserName(tDataSource.getUserName());
        Connection conn = null;
        try {
            conn = AdapterDataSourceManager.createConnection(info);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 获得落地表中的最大版本号
     * @param conn
     * @return
     * @throws SQLException
     */
    protected int getMaxVersion(Connection conn,String tableName) throws SQLException {
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            pst = conn.prepareStatement("select max( VERSION ) from " + tableName);
            rs = pst.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return 0;
            }
        }catch (Exception e){
            throw e;
        }finally {
            if(rs != null){
                rs.close();
            }
            if(pst != null){
                pst.close();
            }
        }
    }

    /**
     * 清空落地表的数据
     * @param conn
     * @throws SQLException
     */
    protected void truncateTable(Connection conn,String tableName) throws SQLException {
        PreparedStatement preparedStatement = null;
        try{
            preparedStatement = conn.prepareStatement("delete from " + tableName);
        if (tDataSource.getType() == DataBaseType.HBASE_PHOENIX) {
            preparedStatement.execute();
            conn.commit();
        } else { //mysql
            if (isExistsTable(conn, tableName)) { //表已经在服务端创建
                preparedStatement.execute();
            } else {
                String msg = "不存在落地对象，请先创建落地对象";
                log.error(msg);
                throw new SQLException(msg);
            }
        }
        }catch (Exception e){
            throw e;
        }finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }


    protected String getTableName() {
        if (tableName == null) {
            if (tDataSource.getTableName() == null) {
                if (tDataSource.getType() == DataBaseType.MYSQL) {
                    tableName = genTrainModelTableName(getCurrentFlow().getRepositoryId(), getCurrentFlow().getId(), this.getSquidId());
                }else {
                    throw new RuntimeException("暂不支持数据库"+tDataSource.getType());
                }
            } else {
                tableName = tDataSource.getTableName();
            }
        }
        return tableName;
    }


    /**
     * 设置ID为主键，自增，
     *
     * @param conn
     * @param tableName
     */
    private void setIdPrimaryKeyAndAutoIncrement(Connection conn,String tableName) throws SQLException {
        String sql = "alter table " + tableName +" modify id int  not null auto_increment primary key";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.execute();
        }catch (Exception e){
            throw e;
        }finally {
            if(preparedStatement != null){
                preparedStatement.close();
            }
        }
    }

    /**
     * ID是否是 PrimaryKey
     * @param connection
     * @param tableName
     * @return
     */
    private boolean isIdPrimaryKey(Connection connection,String tableName) throws SQLException {
        ResultSet resultSet = null;
        try {
            resultSet = connection.getMetaData().getPrimaryKeys(connection.getCatalog(), null, tableName);
            while (resultSet.next()) {
                String primaryKeyColumnName = resultSet.getString("COLUMN_NAME");
                if (primaryKeyColumnName.equalsIgnoreCase("id")) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    /**
     * 设置主键从1开始
     * @param connection
     * @param tableName
     */
    private void setIdPrimaryKeyAutoIncrementFrom1(Connection connection ,String tableName) throws SQLException {
        String sql = "alter table " + tableName + " AUTO_INCREMENT 1";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (Exception e) {
            throw e;
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public TSquid getPreSquid() {
        return preSquid;
    }

    public void setPreSquid(TSquid preSquid) {
        this.preSquid = preSquid;
    }

    public float getPercentage() {
        return percentage;
    }

    public void setPercentage(float percentage) {
        this.percentage = percentage;
    }

    public boolean isVersioning() {
        return versioning;
    }

    public void setVersioning(boolean versioning) {
        this.versioning = versioning;
    }

    public TDataSource gettDataSource() {
        return tDataSource;
    }

    public Integer getInKey() {
        return inKey;
    }

    public void setInKey(Integer inKey) {
        this.inKey = inKey;
    }

    public void settDataSource(TDataSource tDataSource) {
        this.tDataSource = tDataSource;
    }

    public List<TSquid> getDependenceSquids() {
        return dependenceSquids;
    }

    public void setDependenceSquids(List<TSquid> dependenceSquids) {
        this.dependenceSquids = dependenceSquids;
    }

    public void setModelSummaryTSquidTypes(Set<TSquidType> modelSummaryTSquidTypes) {
        this.modelSummaryTSquidTypes = modelSummaryTSquidTypes;
    }

    public Set<TSquidType> getModelSummaryTSquidTypes() {
        return this.modelSummaryTSquidTypes;
    }

    /**
     * 保存记录
     * @param model
     * @param precision
     * @param total
     */
    protected void saveModel(java.sql.Connection conn,String tableName,String saveModelSql,String key, int version,
                              Object model,float precision,long total) {
        byte[] modelBytes = null;
        PreparedStatement preparedStatement = null;
        try {
            modelBytes = IOUtils.readObjectToBytes(model);
            preparedStatement = conn.prepareStatement(saveModelSql);
            //   preparedStatement.setLong(1, 0);  // id 是主键，已经设置自增
            preparedStatement.setLong(1, total);
            preparedStatement.setFloat(2, percentage);
            preparedStatement.setBytes(3, modelBytes);
            preparedStatement.setFloat(4, precision);
            preparedStatement.setTimestamp(5, new java.sql.Timestamp(new java.util.Date().getTime()));
            preparedStatement.setInt(6, version);
            preparedStatement.setObject(7, key);
            preparedStatement.execute();
            if (tDataSource.getType() == DataBaseType.HBASE_PHOENIX) {
                conn.commit();
            }
            log.debug("保存模型到MySQL成功... 表名：" + tableName);
        } catch (com.mysql.jdbc.PacketTooBigException e) {
            String msg = "训练模型" + modelBytes.length + "字节，太大不能写入数据表";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        } catch (IOException e) {
            log.error("训练模型序列化异常..", e);
            throw new RuntimeException(e);
        } catch (SQLException e) {
            log.error("训练模型 SQL异常", e);
            throw new RuntimeException("训练模型 SQL异常", e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected String getSaveModelSql(String tableName) {
        String sb = null;
        String defaultPkName = HbaseUtil.DEFAULT_PK_NAME;
        if (tDataSource.getType() == DataBaseType.HBASE_PHOENIX) {
            sb = "upsert into " + tableName +
                    "(TOTAL_DATASET,TRAINING_PERCENTAGE,MODEL,\"PRECISION\",CREATION_DATE,VERSION,\"KEY\",\"" +
                     defaultPkName + "\") values(?,?,?,?,?,?,?,?)";
        } else { //mysql
            sb = "insert into " + tableName + "(" +
                    //     "ID ," +  // id 是主键，已经设置自增
                    "TOTAL_DATASET ," +
                    "TRAINING_PERCENTAGE ," +
                    " MODEL ," +
                    "`PRECISION`," +
                    "CREATION_DATE ," +
                    "VERSION," +
                    "`KEY`" +
                    ") values(?,?,?,?,?,?,?)";
        }
        return sb;
    }

    /**
     * 是否设置了系数squid
     * @return
     */
    protected  Boolean hasCoefficientSquid(){
       return modelSummaryTSquidTypes != null && modelSummaryTSquidTypes.contains(TSquidType.COEFFICIENT_SQUID);
    }

    /**
     * 标准化是否是否设置了datacatchSquid
     * @return
     */
    protected Boolean hasDataCatchSquid(){
        return modelSummaryTSquidTypes != null && modelSummaryTSquidTypes.contains(TSquidType.DATACATCH_SQUID);
    }

    /**
     * 表是否存在
     *
     * @param conn
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static boolean isExistsTable(Connection conn, String tableName) throws SQLException {
        boolean res = false;
        ResultSet resultSet = null;
        try {
            DatabaseMetaData meta = conn.getMetaData();
            resultSet = meta.getTables(conn.getCatalog(), null, tableName, new String[]{"TABLE"});
            while (resultSet.next()) {
                res = true;
                break;
            }
        } catch (SQLException ex) {
            throw ex;
        }finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return res;
    }

    /**
     * 训练模型的表名
     * @param repositoryId
     * @param squidFlowId
     * @param squidId
     * @return
     */
    public static String genTrainModelTableName(int repositoryId, int squidFlowId, int squidId) {
        return "t_" + repositoryId + "_" + squidFlowId + "_" + squidId;
    }

}

