package com.eurlanda.datashire.engine.entity;

import java.io.Serializable;

/**
 * Created by zhudebin on 15-4-15.
 */
public class TOrderItem implements Serializable {

    // 排序字段ID
    private int key;
    private String name;
    // 排序方式,true  asc;false desc
    private boolean ascending;

    public TOrderItem() {
    }

    public TOrderItem(int key, String name, boolean ascending) {
        this.key = key;
        this.name = name;
        this.ascending = ascending;
    }

    public TOrderItem(String name, boolean ascending) {
        this.name = name;
        this.ascending = ascending;
    }

    public TOrderItem(boolean ascending, int key) {
        this.ascending = ascending;
        this.key = key;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public boolean isAscending() {
        return ascending;
    }

    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
