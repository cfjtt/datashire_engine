package com.eurlanda.datashire.engine.exception;

import com.eurlanda.datashire.engine.enumeration.ERRCode;

/**
 * 引擎异常类型
 * Created by Hans on 2014/4/3.
 */
public enum EngineExceptionType {
  SQUID_FLOW_NULL(10000L, "squid flow is null."),
  DATA_TYPE_ILLEGAL(10001L, "data type illegal."),
  UNION_COLUMN_SIZE_IS_NOT_EQUALS(ERRCode.UNION_COLUMN_SIZE_IS_NOT_EQUALS.getValue(),"union is illegal");
  private long code;
  private String message;

  EngineExceptionType(long code, String message) {
    this.code = code;
    this.message = message;
  }

  public long getCode() {
    return code;
  }

  public void setCode(long code) {
    this.code = code;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
