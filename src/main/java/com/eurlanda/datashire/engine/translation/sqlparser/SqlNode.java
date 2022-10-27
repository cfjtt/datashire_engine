package com.eurlanda.datashire.engine.translation.sqlparser;

public class SqlNode {
	protected String content;

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "SqlNode [content=" + content + "]";
	}
	
}
