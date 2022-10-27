package com.eurlanda.datashire.engine.translation.expression;

import java.util.Map;

public interface IExpressionValidator {
	/**
	 * 验证表达式是否合法。
	 * @param data
	 * @return
	 */
	public boolean validate(Map<Integer,Object> data);
	
}
