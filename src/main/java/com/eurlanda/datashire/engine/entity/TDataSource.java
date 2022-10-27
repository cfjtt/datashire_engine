package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.entity.clean.UpdateExtractLastValueInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TDataSource implements Serializable {
    private String host;
    private int port;
    private String dbName;
    private String password;
    private String userName;
    private DataBaseType type;
    private String tableName;
    //需要合并的表的名字
    private Set<String> tableNames = new HashSet<>();
    private String filter;
    private List<Object> params;
    // 表别名，用户过滤表达式
    private String alias = " table_alias ";
    // 落地数据库时使用；true 参与CDC,false:不参与
    private boolean isCDC = false;
    //需要更新的最后的lastValue
    private UpdateExtractLastValueInfo updateLastValueInfo;
    //是否需要增量抽取
    private Boolean existIncrementalData = true;
    //每次抽取限制多少条
    private Integer limitNum;
    //检查列的数据类型
    private int checkColumnType;
    public boolean isCDC() {
        return isCDC;
    }
    public void setCDC(boolean isCDC) {
        this.isCDC = isCDC;
    }

    public TDataSource() {
    }

    public TDataSource(String host, int port, String dbName, String password, String userName, String tableName, DataBaseType type) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.password = password;
        this.userName = userName;
        this.tableName = tableName;
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public DataBaseType getType() {
        return type;
    }

    public void setType(DataBaseType type) {
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSafeTableName() {
        if(type == DataBaseType.SQLSERVER) {
            if(getTableName().contains(".")) {
                //int idx = getTableName().indexOf(".");
                String[] tablenames = getTableName().split("\\.");
                String v = "";
                for(int i =0;i<tablenames.length;i++){
                    v+="["+tablenames[i]+"]";
                    if(i<tablenames.length-1){
                        v+=".";
                    }
                }
                //String schema = getTableName().substring(0, idx);
                //String tableName = getTableName().substring(idx + 1);
                return v;
            } else {
                return "[" + getTableName() + "]";
            }
        }
        return getTableName();
    }


    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public UpdateExtractLastValueInfo getUpdateLastValueInfo() {
        return updateLastValueInfo;
    }

    public void setUpdateLastValueInfo(UpdateExtractLastValueInfo updateLastValueInfo) {
        this.updateLastValueInfo = updateLastValueInfo;
    }

    @Override
    public String toString() {
        return "TDataSource [host=" + host + ", port=" + port + ", dbName=" + dbName + ", password=" + password + ", userName=" + userName + ", type=" + type + ", tableName=" + tableName + ", filter=" + filter + ", params=" + params + ", isCDC=" + isCDC + "]";
    }

    public Boolean getExistIncrementalData() {
        return existIncrementalData;
    }

    public void setExistIncrementalData(Boolean existIncrementalData) {
        this.existIncrementalData = existIncrementalData;
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(Set<String> tableNames) {
        this.tableNames = tableNames;
    }

    public Integer getLimitNum() {
        return limitNum;
    }

    public void setLimitNum(Integer limitNum) {
        this.limitNum = limitNum;
    }

    public int getCheckColumnType() {
        return checkColumnType;
    }

    public void setCheckColumnType(int checkColumnType) {
        this.checkColumnType = checkColumnType;
    }
}
