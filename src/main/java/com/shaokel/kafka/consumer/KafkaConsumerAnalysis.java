package com.shaokel.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

//代码清单8-1 消费者客户端示例
@Slf4j
public class KafkaConsumerAnalysis {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-shaokel";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //获取该主题下的所有信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        System.out.println(topic + "下的元数据信息为：" + partitionInfos.toString());
        //订阅topic下的主题信息
        consumer.subscribe(Arrays.asList(topic));
        //通过正则表达式进行匹配
//        consumer.subscribe(Pattern.compile("topic-.*"));
        //执行主题和分区
//        consumer.assign(Arrays.asList(new TopicPartition("topic-demo", 0)));

//        List<TopicPartition> partitions = new ArrayList<>();
//        if (partitionInfos != null) {
//            for (PartitionInfo tpInfo : partitionInfos) {
//                partitions.add(new TopicPartition(tpInfo.topic(), tpInfo.partition()));
//            }
//        }
//        consumer.assign(partitions);

        //取消订阅
//        consumer.unsubscribe();
//        consumer.subscribe(new ArrayList<String>());
//        consumer.assign(new ArrayList<TopicPartition>());

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                    //do something to process record.
                }

                //records(topic)指定主题进行消费
//                for (String topic : Arrays.asList(topic)) {
//                    for (ConsumerRecord<String, String> record : records.records(topic)) {
//                        System.out.println(record.topic() + " : " + record.value());
//                    }
//                }
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            consumer.close();
        }
    }
}
