package com.shaokel.kafka.demos.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

//消费位移的演示
@Slf4j
public class ConsumerOffset {

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
        //关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        TopicPartition tp = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(tp));
        long lastConsumedOffset = -1;//当前消费到的位移
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                break;
            }
            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
            lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            consumer.commitSync();//同步提交消费位移
        }
        //消费位移
        System.out.println("comsumed offset is " + lastConsumedOffset);

        //提交的位移
        OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
        System.out.println("commited offset is " + offsetAndMetadata.offset());

        //准备消费的位移---一般情况下和提交的位移一致
        long posititon = consumer.position(tp);
        System.out.println("the offset of the next record is " + posititon);
    }

    //同步提交，会造成消息重复消费
    public void syncommit() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                //do some logical processing.
            }
            consumer.commitSync();
        }
    }

    //批量处理+批量提交，会造成消息重复消费
    public void batchSyncCommit() {
        final int minBatchSize = 200;
        List<ConsumerRecord> buffer = new ArrayList<>();
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //do some logical processing with buffer.
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    //精准提交，性能低
    public void accurateSyncCommit() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
            //do some logical processing.
            long offset = record.offset();
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
        }
    }

    //(常用方式)按分区粒度同步提交消费位移
    public void partitionSizeSyncCommit() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        //do some logical processing.
                    }
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumedOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    //异步提交，存在重复消费
    public void asyncCommit() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                //do some logical processing.
            }
            consumer.commitAsync((offsets, exception) -> {
                if (exception == null) {
                    System.out.println(offsets);
                } else {
                    log.error("fail to commit offsets {}", offsets, exception);
                }
            });
        }
    }

    //消费者出现异常，可以在最后退出时进行同步提交
    public void asyncAndSync() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (isRunning.get()) {
                //poll records and do some logical processing.
                consumer.commitAsync();
            }
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    //一个相对完整的消费程序的逻辑
    //当关闭这个消费逻辑的时候，可以调用 consumer.wakeup()，也可以调用 isRunning.set(false)。
    public void fullConsument() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (isRunning.get()) {
                //consumer.poll(***)
                //process the record.
                //commit offset.
            }
        } catch (WakeupException e) {
            // ingore the error
        } catch (Exception e) {
            // do some logic process.
        } finally {
            // maybe commit offset.
            consumer.close();
        }

    }
}
