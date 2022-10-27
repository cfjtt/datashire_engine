package com.eurlanda.datashire.engine.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * filter 过滤表达式
 */
public class TFilterExpression implements Serializable {

    private String sourceExpression;

	// 表达式 如  $a > 112 and sd$ds == 443
	private String expression;

	// 变量名 -> key.
	private Map<String, Integer> keyMap;

	// 变量 -> 值  如：@abc  111
	private Map<String, Object> varMap;

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public Map<String, Integer> getKeyMap() {
		return keyMap;
	}

	public void setKeyMap(Map<String, Integer> keyMap) {
		this.keyMap = keyMap;
	}

	public Map<String, Object> getVarMap() {
		return varMap;
	}

	public void setVarMap(Map<String, Object> varMap) {
		this.varMap = varMap;
	}

    public String getSourceExpression() {
        return sourceExpression;
    }

    public void setSourceExpression(String sourceExpression) {
        this.sourceExpression = sourceExpression;
    }
}