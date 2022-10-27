package com.eurlanda.datashire.engine.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by zhudebin on 2017/4/27.
 */
public class TStructField implements Serializable {
    // 列名
    private String name;
    // 类型
    private TDataType dataType;
    // 是否允许为空
    private boolean nullable;
    // metadata
    private Map<String, Object> metadata;

    // precision: Int, scale: Int
    private Integer precision;
    private Integer scale;


    public TStructField(String name, TDataType dataType, boolean nullable, Integer precision, Integer scale) {
        this.name = name;
        this.dataType = dataType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TDataType getDataType() {
        return dataType;
    }

    public void setDataType(TDataType dataType) {
        this.dataType = dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
