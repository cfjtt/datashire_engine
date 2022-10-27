package com.eurlanda.datashire.engine.exception;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TTransformation;

import java.util.Map;

/**
 * transformation runtime exception
 * Created by Juntao.Zhang on 2014/5/23.
 */
public class TransformationException extends RuntimeException {
    private Map<Integer, DataCell> in;
    private TTransformation transformation;

    public TransformationException() {
        super();
    }

    public TransformationException(Map<Integer, DataCell> in, TTransformation transformation, String message) {
        super("[flow in data : " + in.toString() + "],[flow transformation : " + transformation.toString() + "],[cause:" + message + "]");
        this.in = in;
        this.transformation = transformation;
    }

    public TransformationException(Map<Integer, DataCell> in, TTransformation transformation, Exception e) {
        super("{low transformation : " + transformation.toString() + ",input_data : " + in.toString()+"}", e);
        this.in = in;
        this.transformation = transformation;
    }

    public Map<Integer, DataCell> getIn() {
        return in;
    }

    public void setIn(Map<Integer, DataCell> in) {
        this.in = in;
    }

    public TTransformation getTransformation() {
        return transformation;
    }

    public void setTransformation(TTransformation transformation) {
        this.transformation = transformation;
    }
}
