package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.enumeration.NoSQLDataBaseType;

import java.io.Serializable;

public class TNoSQLDataSource implements Serializable {
    private String host;
    private int port;
    private String dbName;
    private String password;
    private String userName;
    private NoSQLDataBaseType type;
    private String tableName;
    private String filter;
    // 落地数据库时使用；true 参与CDC,false:不参与
    private boolean isCDC;

    public boolean isCDC() {
        return isCDC;
    }

    public void setCDC(boolean isCDC) {
        this.isCDC = isCDC;
    }

    public TNoSQLDataSource() {
    }

    public TNoSQLDataSource(String host, int port, String dbName, String password, String userName, String tableName, NoSQLDataBaseType type) {
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

    public NoSQLDataBaseType getType() {
        return type;
    }

    public void setType(NoSQLDataBaseType type) {
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSafeTableName() {
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

    @Override
    public String toString() {
        return "TDataSource [host=" + host + ", port=" + port + ", dbName=" + dbName + ", password=" + password + ", userName=" + userName + ", type=" + type + ", tableName=" + tableName + ", filter=" + filter + ", isCDC=" + isCDC + "]";
    }

}
