package com.eurlanda.datashire.engine.enumeration;

/**
 * log format support type
 * Created by Juntao.Zhang on 2014/5/23.
 */
public enum LogFormatType {
    COMMON_LOG_FORMAT(0),
    EXTENDED_LOG_FORMAT(1),
    APACHE_COMBINED_FORMAT(2),
    IIS_LOG_FILE_FORMAT(3);
    private int val;
    
    public static LogFormatType parse(int n){
    	for(LogFormatType e :LogFormatType.values()){
    		if(e.val==n){
    			return e;
    		}
    	}
    	return null;
    }
    LogFormatType(int n){
    	this.val=n;
    }
}
