package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.entity.Column;

import java.io.Serializable;
import java.util.List;

/**
 * 聚合操作
 * Created by zhudebin on 14-1-13.
 */
public class AggregateAction implements Serializable {
    private Type type;
    // 参与聚合的列
    private Integer inKey;
    // 聚合结果输出列
    private Integer outKey;
    // 聚合列数据类型
    private TDataType inDataType;
    // 聚合结果列数据类型
    private TDataType outDataType;
    // first_value,last_value 排序方式
    private List<TOrderItem> orders;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Integer getInKey() {
        return inKey;
    }

    public void setInKey(Integer inKey) {
        this.inKey = inKey;
    }

    public Integer getOutKey() {
        return outKey;
    }

    public void setOutKey(Integer outKey) {
        this.outKey = outKey;
    }

    public TDataType getInDataType() {
        return inDataType;
    }

    public void setInDataType(TDataType inDataType) {
        this.inDataType = inDataType;
    }

    public TDataType getOutDataType() {
        return outDataType;
    }

    public void setOutDataType(TDataType outDataType) {
        this.outDataType = outDataType;
    }

    public List<TOrderItem> getOrders() {
        return orders;
    }

    public void setOrders(List<TOrderItem> orders) {
        this.orders = orders;
    }


    private Column column;
    public Column getColumn() {
        return column;
    }
    public void setColumn(Column column) {
        this.column = column;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregateAction)) return false;

        AggregateAction that = (AggregateAction) o;

        if (inDataType != that.inDataType) return false;
        if (!inKey.equals(that.inKey)) return false;
        if (outDataType != that.outDataType) return false;
        if (!outKey.equals(that.outKey)) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + inKey.hashCode();
        result = 31 * result + outKey.hashCode();
        result = 31 * result + inDataType.hashCode();
        result = 31 * result + outDataType.hashCode();
        return result;
    }
    /**
     * 聚合的类型
     */
    public enum Type {
        // 总数：1；求和：2；最大：3；最小：4;排序最大:5;排序最小 6;字符串求和 7
        SORT(0),COUNT(1),AVG(1),SUM(2),GROUP(0),MAX(3),
        MIN(4),FIRST_VALUE(5),LAST_VALUE(6),STRING_SUM(7);

        // 聚合类型 ,1:求和；2：最大；3：最小；
        // -1：求总数；0：取一个
        private Integer[] aKey;

        private Type(Integer ... aKey) {
            this.aKey = aKey;
        }

        public Integer[] getaKey() {
            return aKey;
        }

        public Integer aggregateKey() {
            return aKey[0];
        }

        public static Type parse(String name) {
        	Type e =  Enum.valueOf(Type.class, name);
        	return e == null ? GROUP : e;
        }
        public static Type valueOf(int value){
            Type returnType = null;
            Type[] types = Type.values();
            for(Type t : types){
                if(t.aggregateKey()==value){
                    returnType = t;
                    break;
                }
            }
            return returnType;
        }
    }
}
