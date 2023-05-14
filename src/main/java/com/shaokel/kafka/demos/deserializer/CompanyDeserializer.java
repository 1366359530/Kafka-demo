package com.shaokel.kafka.demos.deserializer;

import com.shaokel.kafka.demos.serializer.Company;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

public class CompanyDeserializer implements Deserializer<Company> {
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received by DemoDeserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        name = new String(nameBytes, StandardCharsets.UTF_8);
        address = new String(addressBytes, StandardCharsets.UTF_8);

        return new Company(name, address);
    }

    public void close() {
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
    }
}
