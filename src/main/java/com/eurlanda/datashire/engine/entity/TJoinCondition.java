package com.eurlanda.datashire.engine.entity;

import java.io.Serializable;

public class TJoinCondition implements Serializable {
    private int leftKeyId;
    private int rightKeyId;
    //private int operator; // operator type (目前只考虑 "a join b on a.id=b.id")

    public TJoinCondition() {
        super();
    }

    public TJoinCondition(int leftKeyId, int rightKeyId) {
        super();
        this.leftKeyId = leftKeyId;
        this.rightKeyId = rightKeyId;
    }

    public int getLeftKeyId() {
        return leftKeyId;
    }

    public void setLeftKeyId(int leftKeyId) {
        this.leftKeyId = leftKeyId;
    }

    public int getRightKeyId() {
        return rightKeyId;
    }

    public void setRightKeyId(int rightKeyId) {
        this.rightKeyId = rightKeyId;
    }

}
