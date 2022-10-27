package com.eurlanda.datashire.engine.kafka;

import com.eurlanda.datashire.engine.entity.SFLog;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhudebin on 14-5-30.
 */
public class KafkaTest {

    public static void main(String[] args) {
//        testProducer();
//        testConsum();
//        testKafka();
        listTopic();
    }

    public static void testProducer() {
        /*for(long i=0; i<10000; i++) {
            SFLog sfLog = new SFLog();
            sfLog.setId(System.currentTimeMillis());

            LogProducer.sendLog(sfLog);
        }*/

        long idx = 0;
        while(true) {
            idx ++;
            if(idx % 10 == 0) {
                try {
                    Thread.sleep(3000l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            SFLog sfLog = new SFLog();
            sfLog.setId(System.currentTimeMillis());

            LogProducer.sendLog(sfLog);
        }
    }

    public static void testConsum() {
        LogConsumer.consumSFLog();
    }

    private static ConsumerConnector consumer;
    private static String topic = "ds_log";

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
        props.put("zookeeper.connect", "e180,e181,e182:2181");
        props.put("group.id", "0");
        // 演示用 0，测试用1，开发用2
        props.put("consumer.id", "0");
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public static void testKafka() {
        ConsumerIterator<byte[], byte[]> iter = getKafkaLogStream().iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next().message();
            System.out.println(new String(bytes));
        }
    }

    public static void listTopic() {
//       kafka.admin.TopicCommand.listTopics(new ZkClient("e101:2181"), new TopicCommand.TopicCommandOptions(new String[]{}));
//         List<String> topics = ZkUtils.getAllTopics(new ZkClient("e101:2181")).toList();
    }

}
