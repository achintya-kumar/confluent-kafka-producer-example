package com.achintya.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091");
        props.put("client.id", InetAddress.getLocalHost().getHostName());
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close()));

        while (true) {
            String key = "key-" + String.valueOf(System.currentTimeMillis());
            String value = new Date().toString();
            producer.send(
                    new ProducerRecord<>(
                            "kumar-topic",
                            key,
                            value));
            System.out.println(String.format("\tMessage sent successfully - %s - %s", key, value));
            Thread.sleep(1000);
        }
    }
}
