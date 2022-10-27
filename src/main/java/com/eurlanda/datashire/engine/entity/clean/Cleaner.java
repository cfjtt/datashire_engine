package com.eurlanda.datashire.engine.entity.clean;

import java.io.Serializable;

/**
 * Created by zhudebin on 14-6-30.
 */
public abstract class Cleaner implements Serializable {

    /**
     * 成功了清理
     */
    public void doSuccess() {
        // 如果有清理的动作,重写该方法
    }

    /**
     * 失败了清理
     */
    public void doFail() {

    }

    /**
     * 马上清理
     */
    public void doNow() {

    }
}
