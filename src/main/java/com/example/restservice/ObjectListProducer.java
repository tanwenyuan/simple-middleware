package com.example.restservice;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class ObjectListProducer {
    public static void main(String[] args) {
        long events = 2;
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.102:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.TYPE_MAPPINGS, "user:com.example.restservice.User");


        props.put("message.timeout.ms", "3000");

        Producer<String, List<User>> producer = new KafkaProducer<>(props);

        String topic = "kafeidou";

        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = System.currentTimeMillis();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;

            User user =new User();
            user.setId(nEvents);
            user.setName(msg);

            ProducerRecord<String, List<User>> data = new ProducerRecord<>(topic, ip, Arrays.asList(user));
            producer.send(data, (metadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println("The offset of the record we just sent is: " + metadata.offset());
                }
            });
        }
        System.out.println("send message done");
        producer.close();
        System.exit(-1);
    }
}
