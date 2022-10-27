package com.eurlanda.datashire.engine.kafka;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.engine.dao.SFLogDao;
import com.eurlanda.datashire.engine.entity.SFLog;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.getKafkaLogTopic;
import static com.eurlanda.datashire.engine.util.ConfigurationUtil.getKafkaZookeeperAddress;

/**
 * Created by zhudebin on 14-5-30.
 */
public class LogConsumer {
    private static Log log = LogFactory.getLog(LogConsumer.class);

    private static SFLogDao logDao = new SFLogDao(ConstantUtil.getSysDataSource());
    private static ConsumerConnector consumer;
    private static String topic = getKafkaLogTopic();

    private static ConsumerConnector getConsumer() {
        if (consumer == null) {
            synchronized (LogConsumer.class) {
                if (consumer == null) {
                    consumer = kafka.consumer.Consumer
                            .createJavaConsumerConnector(createConsumerConfig());
                }
            }
        }
        return consumer;
    }

    private static KafkaStream<byte[], byte[]> getKafkaLogStream() {
        Map<String, Integer> topickMap = new HashMap<>();
        topickMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = getConsumer().createMessageStreams(topickMap);
        return streamMap.get(topic).get(0);
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", getKafkaZookeeperAddress());
        props.put("group.id", "engine_" + new Random().nextInt(1000));
        // 演示用 0，测试用1，开发用2
        props.put("consumer.id", "0");
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    private static LinkedBlockingQueue<SFLog> queue = new LinkedBlockingQueue<>();
    private static final int PAGE_SIZE = 1000;
    private static final AtomicBoolean lock = new AtomicBoolean(false);

    public static void consumSFLog() {
        ConsumerIterator<byte[], byte[]> iter = getKafkaLogStream().iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next().message();
            SFLog sfLog = JSON.parseObject(bytes, SFLog.class);
            queue.add(sfLog);
            //1000条
            if (queue.size() > PAGE_SIZE) {
                insertLog();
            }
        }
    }

    private static void insertLog() {
        if (lock.compareAndSet(false, true)) {
            int count = 1000;
            List<SFLog> tmp = new ArrayList<>();
            while (count-- > 0) {
                SFLog sfLog = queue.poll();
                if(sfLog != null) {
                    tmp.add(sfLog);
                } else {
                    break;
                }
            }
            if(ConfigurationUtil.isLogToDB()) {
                logDao.insert(tmp);
            }
            lock.set(false);
        }
    }

    // 大于5秒
    public static class LogTimerTask extends TimerTask {
        @Override
        public void run() {
            if (queue.size() > 0) {
                insertLog();
            }
        }
    }


}
