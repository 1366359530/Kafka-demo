package com.shaokel.kafka.consumer;

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

    //seek方法的使用示例--指定偏移量消费
    //在执行 seek() 方法之前需要先执行一次 poll() 方法，等到分配到分区之后才可以重置消费位置
    public void seek() {
        Properties props = initConfig();
        //代码清单12-1
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        //poll()中的时间参数太短，会导致获取不到分区，太长会产生不必要的等待
        //poll(Duration.ofMillis(0))时，会获取不到消息
        consumer.poll(Duration.ofMillis(10000));
        Set<TopicPartition> assignment = consumer.assignment();
        for (TopicPartition tp : assignment) {
            //未分配到的分区执行 seek() 方法会报错
            consumer.seek(tp, 10);
        }
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            //consume the record.
        }
    }

    //使用seek()方法从分区末尾消费
    //尾部消息指的是将要写入最新消息的位置，不是当前消息的位置
    public void endOffsets() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, offsets.get(tp));
        }
    }

    //offsetsForTimes
    //获取指定时间消息的偏移量
    public void offsetsForTimes() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        for (TopicPartition tp : assignment) {
            timestampToSearch.put(tp, System.currentTimeMillis() - 24 * 3600 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets =
                consumer.offsetsForTimes(timestampToSearch);
        for (TopicPartition tp : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        }
    }

    //消费位移保存在DB中
    public void consumentOffsetDb() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        for (TopicPartition tp : assignment) {
            long offset = getOffsetFromDB(tp);//从DB中读取消费位移
            consumer.seek(tp, offset);
        }
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    //process the record.
                }
                long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                //将消费位移存储在DB中
                storeOffsetToDB(partition, lastConsumedOffset + 1);
            }
        }
    }

    long getOffsetFromDB(TopicPartition tp) {
        return 1L;
    }

    long storeOffsetToDB(TopicPartition tp, long offset) {
        return 1L;
    }

}
