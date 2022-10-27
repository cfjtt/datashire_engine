package com.eurlanda.datashire.engine.exception;

/**
 * 引擎异常
 * Created by Hans on 2014/4/3.
 */
public class EngineException extends RuntimeException {
  public EngineException() {
    super();
  }

  public EngineException(String message) {
    super(message);
  }

  public EngineException(String message, Throwable cause) {
    super(message, cause);
  }

  public EngineException(Throwable cause) {
    super(cause);
  }

  public EngineException(EngineExceptionType type) {
    this(type.getMessage());
    this.type = type;
  }

  EngineExceptionType type;

  public EngineExceptionType getType() {
    return type;
  }
}
