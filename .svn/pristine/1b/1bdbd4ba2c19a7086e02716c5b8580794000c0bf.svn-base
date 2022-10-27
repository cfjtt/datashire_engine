package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.util.TransformationTypeAdaptor;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 转换后的TTransform,适用于spark
 *
 * @author Gene
 */
public class TTransformation implements Serializable {

    private static Log log = LogFactory.getLog(TTransformation.class);

    private int id;
    private int dbId;   // transID
    private ArrayList<Integer> inKeyList = new ArrayList<>();        // 已经记录的输入key
    private ArrayList<Integer> outKeyList = new ArrayList<>();
    private TransformationTypeEnum type;
    private HashMap<String, Object> infoMap = new HashMap<>();
    // 当前transformation 是否执行
    private TFilterExpression filterExpression;
    // transformation inputs 过滤条件
    private List<TFilterExpression> inputsFilter;
    private transient int sequenceIndex;
    private String name;
    
    public int getDbId() {
        return dbId;
    }

    public void setDbId(int dbId) {
        this.dbId = dbId;
    }

    public void process(Map<Integer, DataCell> in) {
//        log.debug("before transformation: [type:" + type.name() + " , in : " + in + "]");
        TransformationTypeAdaptor.mapping(in, this);
//        log.debug("after transformation: [type:" + type.name() + " , in : " + in + "]");
    }

    public Map<String, Object> getInfoMap() {
        return infoMap;
    }

    public void setInfoMap(HashMap<String, Object> infoMap) {
        this.infoMap = infoMap;
    }

    public List<Integer> getInKeyList() {
        return inKeyList;
    }

    public void setInKeyList(ArrayList<Integer> inKeyList) {
        this.inKeyList = inKeyList;
    }

    public List<Integer> getOutKeyList() {
        return outKeyList;
    }

    public void setOutKeyList(ArrayList<Integer> outKeyList) {
        this.outKeyList = outKeyList;
    }

    public TransformationTypeEnum getType() {
        return type;
    }

    public void setType(TransformationTypeEnum type) {
        this.type = type;
    }

    public int getSequenceIndex() {
        return sequenceIndex;
    }

    public void setSequenceIndex(int sequenceIndex) {
        this.sequenceIndex = sequenceIndex;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    /**
     * 为transform 添加一个输入key.
     *
     * @param key
     */
    public void addInKey(Integer key) {
        if (key == null) throw new RuntimeException("输入key不能为空");
        this.inKeyList.add(key);
    }

    /**
     * 为transform添加一个输出key.
     *
     * @param key
     */
    public void addOutKey(Integer key) {
        if (key == null) throw new RuntimeException("输出key不能为空");
        this.outKeyList.add(key);
    }

    @Override
    public String toString() {
        return "TTransformation{" +
                "name=" + name +
                ", id=" + id +
                ", dbId=" + dbId +
                ", inKeyList=" + inKeyList +
                ", outKeyList=" + outKeyList +
                ", type=" + type.name() +
                ", filterExpression=" + filterExpression +
                ", inputsFilter=" + inputsFilter +
                ", sequenceIndex=" + sequenceIndex +
                '}';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dbId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TTransformation other = (TTransformation) obj;
        if (dbId != other.dbId)
            return false;
        return true;
    }

    public TFilterExpression getFilterExpression() {
        return filterExpression;
    }

    public void setFilterExpression(TFilterExpression filterExpression) {
        this.filterExpression = filterExpression;
    }

    public List<TFilterExpression> getInputsFilter() {
        return inputsFilter;
    }

    public void setInputsFilter(List<TFilterExpression> inputsFilter) {
        this.inputsFilter = inputsFilter;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
