package com.eurlanda.datashire.engine.util.time;

import com.eurlanda.datashire.engine.kafka.LogConsumer;

import java.util.Timer;

/**
 * 定时策略
 * Created by Juntao.Zhang on 2014/6/3.
 */
public class TimerManager {

    //时间间隔
    private static final long PERIOD_TIME = 7 * 100;
    private static final long DELAY = 10 * 100;

    private static synchronized Timer getInstance() {
        if (timer == null)
            timer = new Timer("timer manager", true);
        return timer;
    }

    static Timer timer;


    public static void startLogSchedule() {
        timer = getInstance();
        timer.schedule(new LogConsumer.LogTimerTask(), DELAY, PERIOD_TIME);
        LogConsumer.consumSFLog();
    }

}