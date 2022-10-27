package com.eurlanda.datashire.engine.translation.sqlparser;

import java.util.List;

public class BracketNode extends SqlNode{
	List<SqlNode> childs;

	public List<SqlNode> getChilds() {
		return childs;
	}

	public void setChilds(List<SqlNode> childs) {
		this.childs = childs;
	}

	@Override
	public String toString() {
		return "BracketNode [childs=" + childs + "]";
	}
	
	
}
