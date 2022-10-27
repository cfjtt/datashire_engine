package com.eurlanda.datashire.engine.enumeration;

import com.eurlanda.datashire.enumeration.SquidTypeEnum;

import java.util.Map;

public enum  AggregateType {
    avg(1),
    min(2),
    sum(3),
    count(4),
    max(7);
    private int _value;
    private static Map<Integer, SquidTypeEnum> map;
    AggregateType(int _value) {
        this._value = _value;
    }
    public static AggregateType valueOf(int value){
        AggregateType returnType = null;
        AggregateType[] types = AggregateType.values();
        for(AggregateType type : types){
            if(type._value==value){
                returnType = type;
                break;
            }
        }
        if(returnType==null){
            throw new RuntimeException("不支持的聚合类型");
        }
        return returnType;
    }
    public int get_value() {
        return _value;
    }

    public void set_value(int _value) {
        this._value = _value;
    }

}
