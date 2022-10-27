package com.eurlanda.datashire.engine.entity;

import java.io.Serializable;
import java.util.Map;

/**
 *
 * 数据库信息，包括url,数据库类型，sql
 * Created by zhudebin on 13-12-13.
 */
public class DataBaseInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    // 数据库连接url
    private String url;
    // 数据库类型
    private String dataBaseType;
    private String sql;
    // 最小值
    private Long lowerBound;
    // 最大值
    private Long upperBound;
    // 分区数
    private int numPartitions;
    // resultSet映射关系表
    private Map<String, TColumn> rdMap;

    public DataBaseInfo(String url, String dataBaseType, String sql, Long lowerBound, Long upperBound, int numPartitions) {
        this.url = url;
        this.dataBaseType = dataBaseType;
        this.sql = sql;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.numPartitions = numPartitions;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDataBaseType() {
        return dataBaseType;
    }

    public void setDataBaseType(String dataBaseType) {
        this.dataBaseType = dataBaseType;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Long getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(Long lowerBound) {
        this.lowerBound = lowerBound;
    }

    public Long getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(Long upperBound) {
        this.upperBound = upperBound;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public Map<String, TColumn> getRdMap() {
        return rdMap;
    }

    public void setRdMap(Map<String, TColumn> rdMap) {
        this.rdMap = rdMap;
    }
}

