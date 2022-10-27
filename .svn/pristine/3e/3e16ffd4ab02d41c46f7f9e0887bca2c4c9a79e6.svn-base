package com.eurlanda.datashire.schedule;

import com.eurlanda.datashire.engine.job.YarnApplicationStateMonitor;

/**
 * Created by zhudebin on 2017/3/15.
 */
public class StreamJobMonitorTest {

    public static void main(String[] args) {

        new Thread(new YarnApplicationStateMonitor()).start();

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
