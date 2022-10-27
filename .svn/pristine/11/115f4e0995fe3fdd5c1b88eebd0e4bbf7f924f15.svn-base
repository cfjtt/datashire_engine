package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.entity.ExtractSquid;

public class TranslationTest {

	public static void main(String[] args){
		ExtractSquid ss = new ExtractSquid();
		ss.setIncremental_expression("SalesOrderDetail.ModifiedDate > (Select Max(DS_CONNECTION_TYPE.desc) From DS_CONNECTION_TYPE)");
		SqlExctractBuilder sb = new SqlExctractBuilder(null, null);
		sb.setFilter(ss, null);
	}
}
