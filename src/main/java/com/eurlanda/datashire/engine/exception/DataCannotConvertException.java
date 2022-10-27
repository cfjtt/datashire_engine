package com.eurlanda.datashire.engine.exception;

/**
 * Created by zhudebin on 16/2/22.
 */
public class DataCannotConvertException extends RuntimeException {
    public DataCannotConvertException() {
    }

    public DataCannotConvertException(String message) {
        super(message);
    }

    public DataCannotConvertException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataCannotConvertException(Throwable cause) {
        super(cause);
    }

    public DataCannotConvertException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
