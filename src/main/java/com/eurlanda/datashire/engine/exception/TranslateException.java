package com.eurlanda.datashire.engine.exception;

/**
 * 翻译异常
 *
 * Created by zhudebin on 16/2/25.
 */
public class TranslateException extends RuntimeException {
    public TranslateException() {
    }

    public TranslateException(String message) {
        super(message);
    }

    public TranslateException(String message, Throwable cause) {
        super(message, cause);
    }

    public TranslateException(Throwable cause) {
        super(cause);
    }

    public TranslateException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
